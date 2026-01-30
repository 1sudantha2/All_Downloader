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
	ID       uint   `gorm:"primaryKey"`
	Name     string `gorm:"index"`
	IsDir    bool
	TgFileID string
}

var (
	db       *gorm.DB
	cfg      Config
	botPool  []*BotWorker
	poolLock sync.Mutex
)

func main() {
	// 1. Setup
	loadEnv()
	initDB()

	// 2. Initialize All Bots
	initBotPool()

	// 3. Start WebDAV
	fs := &TGFileSystem{}
	ls := webdav.NewMemLS() // FIX 1: Corrected LockSystem initialization
	handler := &webdav.Handler{
		FileSystem: fs,
		LockSystem: ls,
		Logger: func(r *http.Request, err error) {
			if err != nil {
				log.Printf("WEBDAV: %s", err)
			}
		},
	}

	addr := fmt.Sprintf("%s:%s", cfg.Host, cfg.Port)
	fmt.Printf("🚀 Server running on %s with %d Bots\n", addr, len(botPool))
	http.ListenAndServe(addr, handler)
}

// --- Bot Pool Management ---

func initBotPool() {
	for i, token := range cfg.BotTokens {
		client := telegram.NewClient(cfg.APIID, cfg.APIHash, telegram.Options{
			Logger: zap.NewNop(),
		})

		worker := &BotWorker{
			ID:     i + 1,
			Client: client,
		}

		go func(w *BotWorker, t string) {
			err := w.Client.Run(context.Background(), func(ctx context.Context) error {
				// FIX 2: Handle 2 return values (ignore the first one)
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
			if err != nil {
				log.Printf("❌ Bot %d failed: %v", w.ID, err)
			}
		}(worker, token)

		botPool = append(botPool, worker)
		time.Sleep(1 * time.Second)
	}
}

func getAvailableBot() (*BotWorker, error) {
	poolLock.Lock()
	defer poolLock.Unlock()

	for _, bot := range botPool {
		if time.Now().After(bot.CooldownUntil) {
			if bot.Uploader != nil {
				return bot, nil
			}
		}
	}
	return nil, fmt.Errorf("all bots are busy or cooling down")
}

// --- WebDAV Upload Logic ---

type TGFileSystem struct{}

func (fs *TGFileSystem) OpenFile(ctx context.Context, name string, flag int, perm os.FileMode) (webdav.File, error) {
	if flag&os.O_WRONLY != 0 || flag&os.O_CREATE != 0 {
		return &TGUploadFile{name: name}, nil
	}
	return nil, os.ErrNotExist
}

func (fs *TGFileSystem) Mkdir(ctx context.Context, name string, perm os.FileMode) error { return nil }
func (fs *TGFileSystem) RemoveAll(ctx context.Context, name string) error { return nil }
func (fs *TGFileSystem) Rename(ctx context.Context, o, n string) error   { return nil }
func (fs *TGFileSystem) Stat(ctx context.Context, name string) (os.FileInfo, error) { return nil, os.ErrNotExist }

type TGUploadFile struct {
	name   string
	buffer *io.PipeReader
	writer *io.PipeWriter
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
					log.Println("Waiting for available bot...")
					time.Sleep(5 * time.Second)
					continue
				}

				fmt.Printf("📤 Uploading %s using Bot %d...\n", fileName, bot.ID)

				file, err := bot.Uploader.FromReader(ctx, fileName, f.buffer)
				if err != nil {
					// FIX 3: AsFloodWait returns duration directly
					if waitTime, ok := tgerr.AsFloodWait(err); ok {
						fmt.Printf("⚠️ Bot %d Flood Wait! Sleeping for %v\n", bot.ID, waitTime)
						
						bot.Mutex.Lock()
						bot.CooldownUntil = time.Now().Add(waitTime)
						bot.Mutex.Unlock()
						
						continue
					}
					
					log.Printf("Upload Error: %v", err)
					break
				}

				_, err = bot.Sender.To(&tg.InputPeerChannel{
					ChannelID:  cfg.ChannelID,
					AccessHash: 0, 
				}).Media(ctx, message.File(file))

				if err != nil {
					// FIX 3 (Repeat): AsFloodWait returns duration directly
					if waitTime, ok := tgerr.AsFloodWait(err); ok {
						bot.Mutex.Lock()
						bot.CooldownUntil = time.Now().Add(waitTime)
						bot.Mutex.Unlock()
						continue
					}
					log.Printf("Send Error: %v", err)
				} else {
					fmt.Printf("✅ Upload Success: %s\n", fileName)
					success = true
					break
				}
			}
			
			if !success {
				f.buffer.CloseWithError(fmt.Errorf("upload failed after retries"))
			}
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
func (f *TGUploadFile) Readdir(count int) ([]os.FileInfo, error) { return nil, nil }
func (f *TGUploadFile) Stat() (os.FileInfo, error) { return nil, nil }

// --- Helpers ---
func loadEnv() {
	godotenv.Load()
	apiIDStr := os.Getenv("API_ID")
	if apiIDStr == "" {
		// Fallback purely for testing if env is missing, but better to log fatal
		log.Println("API_ID is missing in .env")
	}
	cfg.APIID, _ = strconv.Atoi(apiIDStr)
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
	var err error
	db, err = gorm.Open(sqlite.Open("tg_filesystem.db"), &gorm.Config{})
	if err != nil { log.Fatal(err) }
	db.AutoMigrate(&FileNode{})
}
