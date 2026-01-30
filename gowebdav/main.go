package main

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gotd/td/telegram"
	"github.com/gotd/td/telegram/message"
	"github.com/gotd/td/telegram/uploader"
	"github.com/gotd/td/tg"
	"github.com/joho/godotenv"
	_ "github.com/mattn/go-sqlite3"
)

// ==========================================
// 1. CONFIGURATIONS & GLOBALS
// ==========================================

var (
	API_ID     int
	API_HASH   string
	CHANNEL_ID int64
	HOST       string
	PORT       int
	DB_FILE    string
	BOT_TOKENS []string

	WEBDAV_USER = "admin"
	WEBDAV_PASS = "admin"

	db *sql.DB

	// Bot Pool Logic
	activeBots []*telegram.Client
	botMu      sync.RWMutex

	// Uploading Files Tracker
	uploadingFiles sync.Map

	// Cache
	pathCache   = make(map[string]*Item)
	pathCacheMu sync.RWMutex
)

const CHUNK_SIZE = 512 * 1024

type Item struct {
	ID        int64
	Name      string
	FileID    string
	MessageID int
	Size      int64
	IsDir     bool
	ParentID  int64
}

// ==========================================
// 2. DATABASE LOGIC
// ==========================================

func initDB() {
	var err error
	db, err = sql.Open("sqlite3", fmt.Sprintf("file:%s?cache=shared&mode=rwc", DB_FILE))
	if err != nil {
		log.Fatal("Failed to open DB:", err)
	}

	db.SetMaxOpenConns(1)

	schema := `
	CREATE TABLE IF NOT EXISTS items (
		id INTEGER PRIMARY KEY AUTOINCREMENT, 
		name TEXT, 
		file_id TEXT, 
		message_id INTEGER, 
		size INTEGER, 
		is_dir BOOLEAN DEFAULT 0,
		parent_id INTEGER DEFAULT 0, 
		UNIQUE(name, parent_id)
	);
	CREATE INDEX IF NOT EXISTS idx_parent ON items(parent_id);
	`
	if _, err := db.Exec(schema); err != nil {
		log.Fatal("Failed to create schema:", err)
	}

	var genID int
	err = db.QueryRow("SELECT id FROM items WHERE name='General' AND parent_id=0").Scan(&genID)
	if err == sql.ErrNoRows {
		db.Exec("INSERT INTO items (name, is_dir, parent_id) VALUES ('General', 1, 0)")
	}

	db.Exec("PRAGMA journal_mode=WAL;")
	db.Exec("PRAGMA synchronous = NORMAL;")

	fmt.Printf("✅ Database Initialized at: %s\n", DB_FILE)
}

func getItemByPathCached(pathStr string) *Item {
	cleanPath := strings.Trim(pathStr, "/")
	if cleanPath == "" {
		cleanPath = ""
	}

	pathCacheMu.RLock()
	if item, found := pathCache[cleanPath]; found {
		pathCacheMu.RUnlock()
		return item
	}
	pathCacheMu.RUnlock()

	parts := strings.Split(cleanPath, "/")
	if cleanPath == "" {
		parts = []string{}
	}

	parentID := int64(0)
	var currentItem *Item
	currentItem = &Item{ID: 0, IsDir: true, ParentID: -1}

	if len(parts) > 0 && parts[0] != "" {
		for _, part := range parts {
			decodedPart, _ := url.QueryUnescape(part)
			var id int64
			var name, fid string
			var mid int
			var size int64
			var isDirInt int
			var pid int64

			err := db.QueryRow("SELECT id, name, file_id, message_id, size, is_dir, parent_id FROM items WHERE name=? AND parent_id=?", decodedPart, parentID).
				Scan(&id, &name, &fid, &mid, &size, &isDirInt, &pid)

			if err != nil {
				return nil
			}

			currentItem = &Item{
				ID:        id,
				Name:      name,
				FileID:    fid,
				MessageID: mid,
				Size:      size,
				IsDir:     isDirInt == 1,
				ParentID:  pid,
			}
			parentID = id
		}
	}

	pathCacheMu.Lock()
	if len(pathCache) > 128 {
		for k := range pathCache {
			delete(pathCache, k)
			break
		}
	}
	pathCache[cleanPath] = currentItem
	pathCacheMu.Unlock()

	return currentItem
}

func dbAddOrUpdate(name, fileID string, msgID int, size int64, isDir bool, parentID int64) {
	isDirInt := 0
	if isDir {
		isDirInt = 1
	}

	var existingID int64
	err := db.QueryRow("SELECT id FROM items WHERE name=? AND parent_id=?", name, parentID).Scan(&existingID)

	if err == nil {
		db.Exec("UPDATE items SET file_id=?, message_id=?, size=?, is_dir=? WHERE id=?", fileID, msgID, size, isDirInt, existingID)
	} else {
		db.Exec("INSERT INTO items (name, file_id, message_id, size, is_dir, parent_id) VALUES (?, ?, ?, ?, ?, ?)", name, fileID, msgID, size, isDirInt, parentID)
	}

	pathCacheMu.Lock()
	pathCache = make(map[string]*Item)
	pathCacheMu.Unlock()
}

func dbDeleteRecursive(itemID int64) {
	rows, err := db.Query("SELECT id FROM items WHERE parent_id=?", itemID)
	if err == nil {
		defer rows.Close()
		for rows.Next() {
			var childID int64
			rows.Scan(&childID)
			dbDeleteRecursive(childID)
		}
	}
	db.Exec("DELETE FROM items WHERE id=?", itemID)
	pathCacheMu.Lock()
	pathCache = make(map[string]*Item)
	pathCacheMu.Unlock()
}

// ==========================================
// 3. BOT POOL LOGIC
// ==========================================

func smartAPICall(action func(ctx context.Context, client *telegram.Client) error) error {
	botMu.RLock()
	count := len(activeBots)
	botMu.RUnlock()

	if count == 0 {
		return fmt.Errorf("no bots available yet")
	}

	retries := count * 2
	for i := 0; i < retries; i++ {
		botMu.RLock()
		client := activeBots[rand.Intn(len(activeBots))]
		botMu.RUnlock()

		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Hour)
		err := action(ctx, client)
		cancel()

		if err == nil {
			return nil
		}
		time.Sleep(1 * time.Second)
	}
	return fmt.Errorf("all bots failed")
}

// ==========================================
// 4. WEBDAV HANDLERS
// ==========================================

func webdavHandler(w http.ResponseWriter, r *http.Request) {
	user, pass, ok := r.BasicAuth()
	if !ok || user != WEBDAV_USER || pass != WEBDAV_PASS {
		w.Header().Set("WWW-Authenticate", `Basic realm="Restricted"`)
		http.Error(w, "Unauthorized", 401)
		return
	}

	path := r.URL.Path
	if strings.HasPrefix(path, "http") {
		u, _ := url.Parse(path)
		path = u.Path
	}
	cleanPath := strings.Trim(path, "/")
	item := getItemByPathCached(cleanPath)

	switch r.Method {
	case "OPTIONS":
		w.Header().Set("DAV", "1, 2")
		w.Header().Set("Allow", "OPTIONS, PROPFIND, GET, PUT, DELETE, MKCOL, MOVE, COPY")
		w.WriteHeader(200)

	case "PROPFIND":
		if item == nil {
			http.Error(w, "Not Found", 404)
			return
		}
		depth := r.Header.Get("Depth")
		if depth == "" {
			depth = "1"
		}

		w.Header().Set("Content-Type", "application/xml; charset=utf-8")
		w.WriteHeader(207)

		buildNode := func(name string, isDir bool, size int64, href string) string {
			resType := ""
			contentType := "application/octet-stream"
			if isDir {
				resType = "<D:collection/>"
				contentType = "httpd/unix-directory"
			}
			safeName := htmlEscape(name)
			return fmt.Sprintf(`
			<D:response>
				<D:href>%s</D:href>
				<D:propstat>
					<D:prop>
						<D:displayname>%s</D:displayname>
						<D:getcontentlength>%d</D:getcontentlength>
						<D:resourcetype>%s</D:resourcetype>
						<D:getcontenttype>%s</D:getcontenttype>
					</D:prop>
					<D:status>HTTP/1.1 200 OK</D:status>
				</D:propstat>
			</D:response>`, href, safeName, size, resType, contentType)
		}

		output := `<?xml version="1.0" encoding="utf-8" ?><D:multistatus xmlns:D="DAV:">`
		baseHref := r.URL.Path
		if !strings.HasSuffix(baseHref, "/") && item.IsDir {
			baseHref += "/"
		}
		output += buildNode(item.Name, item.IsDir, item.Size, baseHref)

		if item.IsDir && depth != "0" {
			rows, _ := db.Query("SELECT name, size, is_dir FROM items WHERE parent_id=?", item.ID)
			if rows != nil {
				for rows.Next() {
					var cName string
					var cSize int64
					var cIsDirInt int
					rows.Scan(&cName, &cSize, &cIsDirInt)
					
					childHref := strings.TrimRight(baseHref, "/") + "/" + url.PathEscape(cName)
					if cIsDirInt == 1 {
						childHref += "/"
					}

					output += buildNode(cName, cIsDirInt == 1, cSize, childHref)
				}
				rows.Close()
			}
		}
		output += "</D:multistatus>"
		fmt.Fprint(w, output)

	case "GET":
		if item == nil || item.IsDir {
			http.Error(w, "Not Found", 404)
			return
		}

		rangeHeader := r.Header.Get("Range")
		startByte := int64(0)
		endByte := item.Size - 1

		if rangeHeader != "" {
			ranges := strings.Replace(rangeHeader, "bytes=", "", 1)
			parts := strings.Split(ranges, "-")
			startByte, _ = strconv.ParseInt(parts[0], 10, 64)
			if len(parts) > 1 && parts[1] != "" {
				endByte, _ = strconv.ParseInt(parts[1], 10, 64)
			}
			w.WriteHeader(206)
			w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", startByte, endByte, item.Size))
		} else {
			w.WriteHeader(200)
		}

		w.Header().Set("Content-Length", strconv.FormatInt(endByte-startByte+1, 10))
		w.Header().Set("Accept-Ranges", "bytes")
		w.Header().Set("Content-Type", "application/octet-stream")

		err := smartAPICall(func(ctx context.Context, client *telegram.Client) error {
			api := client.API()

			msgs, err := api.ChannelsGetMessages(ctx, &tg.ChannelsGetMessagesRequest{
				Channel: &tg.InputChannel{
					ChannelID: CHANNEL_ID,
					AccessHash: 0,
				},
				ID: []tg.InputMessageClass{&tg.InputMessageID{ID: item.MessageID}},
			})

			if err != nil {
				return err
			}

			var location tg.InputFileLocationClass
			if channelMsgs, ok := msgs.(*tg.MessagesChannelMessages); ok {
				if len(channelMsgs.Messages) > 0 {
					msg, ok := channelMsgs.Messages[0].(*tg.Message)
					if ok {
						switch media := msg.Media.(type) {
						case *tg.MessageMediaDocument:
							doc, _ := media.Document.AsNotEmpty()
							location = &tg.InputDocumentFileLocation{
								ID:            doc.ID,
								AccessHash:    doc.AccessHash,
								FileReference: doc.FileReference,
								ThumbSize:     "",
							}
						}
					}
				}
			}

			if location == nil {
				return fmt.Errorf("file location not found")
			}

			offset := startByte
			limit := endByte + 1

			for offset < limit {
				chunkSize := int64(CHUNK_SIZE)
				if offset+chunkSize > limit {
					chunkSize = limit - offset
				}

				result, err := api.UploadGetFile(ctx, &tg.UploadGetFileRequest{
					Location: location,
					Offset:   offset,
					Limit:    int(chunkSize),
				})
				if err != nil {
					return err
				}

				var bytes []byte
				switch r := result.(type) {
				case *tg.UploadFile:
					bytes = r.Bytes
				case *tg.UploadFileCDNRedirect:
					return fmt.Errorf("cdn not supported")
				}

				if len(bytes) == 0 {
					break
				}

				if _, err := w.Write(bytes); err != nil {
					return err
				}

				if f, ok := w.(http.Flusher); ok {
					f.Flush()
				}

				offset += int64(len(bytes))
			}
			return nil
		})

		if err != nil {
			log.Println("Stream error:", err)
		}

	case "PUT":
		parentPath := filepath.Dir(cleanPath)
		fileName := filepath.Base(cleanPath)
		parentItem := getItemByPathCached(parentPath)

		if parentItem == nil {
			http.Error(w, "Parent Not Found", 409)
			return
		}

		if _, loaded := uploadingFiles.LoadOrStore(fileName, true); loaded {
			http.Error(w, "Already Uploading", 409)
			return
		}
		defer uploadingFiles.Delete(fileName)

		tempDir := os.TempDir()
		tempPath := filepath.Join(tempDir, fileName)
		outFile, err := os.Create(tempPath)
		if err != nil {
			http.Error(w, "Server Error", 500)
			return
		}

		_, err = io.Copy(outFile, r.Body)
		outFile.Close()
		if err != nil {
			os.Remove(tempPath)
			http.Error(w, "Upload Failed", 500)
			return
		}

		err = smartAPICall(func(ctx context.Context, client *telegram.Client) error {
			u := uploader.NewUploader(client.API())

			upload, err := u.FromPath(ctx, tempPath)
			if err != nil {
				return err
			}

			sender := message.NewSender(client.API())
			target := sender.Resolve(fmt.Sprintf("-100%d", CHANNEL_ID))

			updates, err := target.File(ctx, upload)
			if err != nil {
				return err
			}

			var msg *tg.Message
			switch u := updates.(type) {
			case *tg.Updates:
				for _, update := range u.Updates {
					switch m := update.(type) {
					case *tg.UpdateNewMessage:
						if mm, ok := m.Message.(*tg.Message); ok {
							msg = mm
						}
					case *tg.UpdateNewChannelMessage:
						if mm, ok := m.Message.(*tg.Message); ok {
							msg = mm
						}
					}
				}
			case *tg.UpdateShortSentMessage:
				// Minimal info case
			}

			if msg == nil {
				return fmt.Errorf("message not found in updates")
			}

			var fid string
			var size int64

			if doc, ok := msg.Media.(*tg.MessageMediaDocument); ok {
				d, _ := doc.Document.AsNotEmpty()
				fid = fmt.Sprintf("%d", d.ID)
				size = d.Size
			}

			dbAddOrUpdate(fileName, fid, msg.ID, size, false, parentItem.ID)
			return nil
		})

		os.Remove(tempPath)

		if err != nil {
			log.Println("Upload Error:", err)
			http.Error(w, "TG Upload Failed", 502)
			return
		}

		w.WriteHeader(201)

	case "MKCOL":
		parentPath := filepath.Dir(cleanPath)
		parentItem := getItemByPathCached(parentPath)
		if parentItem != nil {
			dbAddOrUpdate(filepath.Base(cleanPath), "", 0, 0, true, parentItem.ID)
			w.WriteHeader(201)
		} else {
			http.Error(w, "Conflict", 409)
		}

	case "DELETE":
		if item != nil {
			dbDeleteRecursive(item.ID)
			w.WriteHeader(204)
		} else {
			http.Error(w, "Not Found", 404)
		}

	case "MOVE":
		dest := r.Header.Get("Destination")
		destURL, _ := url.Parse(dest)
		destPath := strings.Trim(destURL.Path, "/")
		destParent := getItemByPathCached(filepath.Dir(destPath))

		if item != nil && destParent != nil {
			db.Exec("UPDATE items SET name=?, parent_id=? WHERE id=?", filepath.Base(destPath), destParent.ID, item.ID)
			pathCacheMu.Lock()
			pathCache = make(map[string]*Item)
			pathCacheMu.Unlock()
			w.WriteHeader(201)
		} else {
			http.Error(w, "Conflict", 409)
		}

	case "COPY":
		dest := r.Header.Get("Destination")
		destURL, _ := url.Parse(dest)
		destPath := strings.Trim(destURL.Path, "/")
		destParent := getItemByPathCached(filepath.Dir(destPath))

		if item != nil && destParent != nil {
			// Copy is basically creating a new DB entry pointing to the same file info
			isDirInt := 0
			if item.IsDir { isDirInt = 1 }
			
			db.Exec("INSERT INTO items (name, file_id, message_id, size, is_dir, parent_id) VALUES (?, ?, ?, ?, ?, ?)", 
				filepath.Base(destPath), item.FileID, item.MessageID, item.Size, isDirInt, destParent.ID)
			
			pathCacheMu.Lock()
			pathCache = make(map[string]*Item)
			pathCacheMu.Unlock()
			w.WriteHeader(201)
		} else {
			http.Error(w, "Conflict", 409)
		}
	}
}

func htmlEscape(s string) string {
	return strings.ReplaceAll(strings.ReplaceAll(s, "&", "&amp;"), "<", "&lt;")
}

// ==========================================
// 5. MAIN
// ==========================================

func main() {
	if err := godotenv.Load(); err != nil {
		log.Println("Warning: .env file not found")
	}

	API_ID, _ = strconv.Atoi(os.Getenv("API_ID"))
	API_HASH = os.Getenv("API_HASH")
	CHANNEL_ID, _ = strconv.ParseInt(os.Getenv("CHANNEL_ID"), 10, 64)
	HOST = os.Getenv("HOST")
	if HOST == "" { HOST = "0.0.0.0" }
	p, _ := strconv.Atoi(os.Getenv("PORT"))
	if p == 0 { PORT = 9001 } else { PORT = p }

	cwd, _ := os.Getwd()
	DB_FILE = filepath.Join(cwd, "files.db")

	i := 1
	for {
		t := os.Getenv(fmt.Sprintf("BOT_TOKEN_%d", i))
		if t == "" { break }
		BOT_TOKENS = append(BOT_TOKENS, t)
		i++
	}

	initDB()

	var wg sync.WaitGroup
	for idx, token := range BOT_TOKENS {
		wg.Add(1)
		go func(i int, t string) {
			time.Sleep(time.Duration(i) * 15 * time.Second)

			for { 
				client := telegram.NewClient(API_ID, API_HASH, telegram.Options{})

				err := client.Run(context.Background(), func(ctx context.Context) error {
					status, err := client.Auth().Bot(ctx, t)
					if err != nil {
						return err
					}

					user, _ := status.User.AsNotEmpty()
					fmt.Printf("✅ Bot %d Connected: %s\n", i+1, user.Username)

					botMu.Lock()
					activeBots = append(activeBots, client)
					botMu.Unlock()

					<-ctx.Done()
					return ctx.Err()
				})

				fmt.Printf("⚠️ Bot %d failed (Retrying in 10s): %v\n", i+1, err)
				time.Sleep(10 * time.Second)
			}
			wg.Done()
		}(idx, token)
	}

	go func() {
		time.Sleep(5 * time.Second)
		fmt.Printf("🚀 WebDAV Server running on %s:%d\n", HOST, PORT)
		http.HandleFunc("/", webdavHandler)
		log.Fatal(http.ListenAndServe(fmt.Sprintf("%s:%d", HOST, PORT), nil))
	}()

	wg.Wait()
}
