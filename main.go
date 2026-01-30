package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"net/http"
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
	"github.com/gotd/td/tgerr"
	"github.com/joho/godotenv"
	"go.uber.org/zap"
	"golang.org/x/net/webdav"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

// --- Config ---
type Config struct {
	APIID     int
	APIHash   string
	BotTokens []string
	ChannelID int64
	Host      string
	Port      string
}

// --- Bot Worker Structure ---
type BotWorker struct {
	ID            int
	Client        *telegram.Client
	API           *tg.Client
	Uploader      *uploader.Uploader
	Sender        *message.Sender
	CooldownUntil time.Time
	Mutex         sync.Mutex
}

// --- DB Model ---
type FileNode struct {
	ID        uint      `gorm:"primaryKey"`
	Name      string    `gorm:"index"`
	IsDir     bool
	Size      int64
	TgFileID  string
	CreatedAt time.Time
	UpdatedAt time.Time
}

var (
	db       *gorm.DB
	cfg      Config
	botPool  []*BotWorker
	poolLock sync.Mutex
)

// --- Custom FileInfo for WebDAV ---
type TGFileInfo struct {
	name    string
	size    int64
	mode    os.FileMode
	modTime time.Time
	isDir   bool
}

func (fi *TGFileInfo) Name() string       { return fi.name }
func (fi *TGFileInfo) Size() int64        { return fi.size }
func (fi *TGFileInfo) Mode() os.FileMode  { return fi.mode }
func (fi *TGFileInfo) ModTime() time.Time { return fi.modTime }
func (fi *TGFileInfo) IsDir() bool        { return fi.isDir }
func (fi *TGFileInfo) Sys() interface{}   { return nil }

func main() {
	loadEnv()
	initDB()
	initBotPool()

	fs := &TGFileSystem{}
	ls := webdav.NewMemLS()
	handler := &webdav.Handler{
		FileSystem: fs,
		LockSystem: ls,
		Logger: func(r *http.Request, err error) {
			if err != nil {
				log.Printf("WEBDAV ERROR: %s", err)
			}
		},
	}

	addr := fmt.Sprintf("%s:%s", cfg.Host, cfg.Port)
	fmt.Printf("🚀 Server running on %s with %d Bots\n", addr, len(botPool))
	http.ListenAndServe(addr, handler)
}

// --- Bot Pool ---
func initBotPool() {
	for i, token := range cfg.BotTokens {
		client := telegram.NewClient(cfg.APIID, cfg.APIHash, telegram.Options{
			Logger: zap.NewNop(),
		})
		worker := &BotWorker{ID: i + 1, Client: client}
		go func(w *BotWorker, t string) {
			w.Client.Run(context.Background(), func(ctx context.Context) error {
				if _, err := w.Client.Auth().Bot(ctx, t); err != nil {
					return err
				}
				w.API = w.Client.API()
				w.Uploader = uploader.NewUploader(w.API)
				w.Sender = message.NewSender(w.API)
				fmt.Printf("✅ Bot %d Connected!\n", w.ID)
				<-ctx.Done()
				return nil
			})
		}(worker, token)
		botPool = append(botPool, worker)
		time.Sleep(1 * time.Second)
	}
}

func getAvailableBot() (*BotWorker, error) {
	poolLock.Lock()
	defer poolLock.Unlock()
	for _, bot := range botPool {
		if time.Now().After(bot.CooldownUntil) && bot.Uploader != nil {
			return bot, nil
		}
	}
	return nil, fmt.Errorf("busy")
}

// --- WebDAV Implementation ---
type TGFileSystem struct{}

func (fs *TGFileSystem) Mkdir(ctx context.Context, name string, perm os.FileMode) error {
	name = strings.TrimPrefix(name, "/")
	db.Create(&FileNode{Name: name, IsDir: true, CreatedAt: time.Now()})
	return nil
}

func (fs *TGFileSystem) RemoveAll(ctx context.Context, name string) error { return nil }
func (fs *TGFileSystem) Rename(ctx context.Context, o, n string) error   { return nil }

// FIX: Stat Implementation to handle Root Directory
func (fs *TGFileSystem) Stat(ctx context.Context, name string) (os.FileInfo, error) {
	name = strings.TrimPrefix(name, "/")
	if name == "" {
		return &TGFileInfo{name: "", isDir: true, mode: os.ModeDir | 0755, modTime: time.Now()}, nil
	}
	
	var node FileNode
	if err := db.Where("name = ?", name).First(&node).Error; err != nil {
		return nil, os.ErrNotExist
	}
	
	mode := os.FileMode(0644)
	if node.IsDir {
		mode = os.ModeDir | 0755
	}
	return &TGFileInfo{
		name:    node.Name,
		size:    node.Size,
		isDir:   node.IsDir,
		mode:    mode,
		modTime: node.UpdatedAt,
	}, nil
}

func (fs *TGFileSystem) OpenFile(ctx context.Context, name string, flag int, perm os.FileMode) (webdav.File, error) {
	name = strings.TrimPrefix(name, "/")
	
	if flag&os.O_WRONLY != 0 || flag&os.O_CREATE != 0 {
		return &TGUploadFile{name: name}, nil
	}
	
	// Handle Directory Listing (ReadDir)
	if name == "" {
		return &TGUploadFile{name: "", isDir: true}, nil
	}

	return nil, os.ErrNotExist // Download not implemented yet
}

// --- Upload & File Handler ---
type TGUploadFile struct {
	name   string
	buffer *io.PipeReader
	writer *io.PipeWriter
	isDir  bool
}

// FIX: Readdir Implementation to list files
func (f *TGUploadFile) Readdir(count int) ([]os.FileInfo, error) {
	if !f.isDir {
		return nil, os.ErrInvalid
	}
	var nodes []FileNode
	db.Find(&nodes) // Load all files from DB
	
	var infos []os.FileInfo
	for _, node := range nodes {
		mode := os.FileMode(0644)
		if node.IsDir { mode = os.ModeDir | 0755 }
		infos = append(infos, &TGFileInfo{
			name: node.Name, size: node.Size, isDir: node.IsDir, mode: mode, modTime: node.UpdatedAt,
		})
	}
	return infos, nil
}

func (f *TGUploadFile) Stat() (os.FileInfo, error) {
	return &TGFileInfo{name: f.name, isDir: f.isDir, mode: os.ModeDir | 0755}, nil
}

func (f *TGUploadFile) Write(p []byte) (n int, err error) {
	if f.writer == nil {
		pr, pw := io.Pipe()
		f.buffer = pr
		f.writer = pw
		go func() {
			ctx := context.Background()
			fileName := filepath.Base(f.name)
			maxRetries := 5
			success := false

			for i := 0; i < maxRetries; i++ {
				bot, err := getAvailableBot()
				if err != nil {
					time.Sleep(5 * time.Second)
					continue
				}
				fmt.Printf("📤 Uploading %s via Bot %d...\n", fileName, bot.ID)
				file, err := bot.Uploader.FromReader(ctx, fileName, f.buffer)
				if err != nil {
					if wait, ok := tgerr.AsFloodWait(err); ok {
						bot.Mutex.Lock()
						bot.CooldownUntil = time.Now().Add(wait)
						bot.Mutex.Unlock()
						continue
					}
					log.Printf("Upload Fail: %v", err)
					break
				}
				_, err = bot.Sender.To(&tg.InputPeerChannel{ChannelID: cfg.ChannelID, AccessHash: 0}).Media(ctx, message.File(file))
				if err == nil {
					fmt.Printf("✅ Success: %s\n", fileName)
					// Save to DB
					db.Create(&FileNode{Name: f.name, IsDir: false, Size: 0, UpdatedAt: time.Now()})
					success = true
					break
				}
			}
			if !success { f.buffer.CloseWithError(fmt.Errorf("failed")) }
		}()
	}
	return f.writer.Write(p)
}

func (f *TGUploadFile) Close() error {
	if f.writer != nil { return f.writer.Close() }
	return nil
}
func (f *TGUploadFile) Read(p []byte) (n int, err error) { return 0, io.EOF }
func (f *TGUploadFile) Seek(offset int64, whence int) (int64, error) { return 0, nil }

func loadEnv() {
	godotenv.Load()
	cfg.APIID, _ = strconv.Atoi(os.Getenv("API_ID"))
	cfg.APIHash = os.Getenv("API_HASH")
	i := 1
	for {
		t := os.Getenv(fmt.Sprintf("BOT_TOKEN_%d", i))
		if t == "" { break }
		cfg.BotTokens = append(cfg.BotTokens, t)
		i++
	}
	cfg.ChannelID, _ = strconv.ParseInt(os.Getenv("CHANNEL_ID"), 10, 64)
	cfg.Host = os.Getenv("HOST")
	if cfg.Host == "" { cfg.Host = "0.0.0.0" }
	cfg.Port = os.Getenv("PORT")
	if cfg.Port == "" { cfg.Port = "9001" }
}

func initDB() {
	db, _ = gorm.Open(sqlite.Open("tg_filesystem.db"), &gorm.Config{})
	db.AutoMigrate(&FileNode{})
}
