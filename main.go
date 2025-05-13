package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/migadu/sora/cache"
	"github.com/migadu/sora/consts"
	"github.com/migadu/sora/db"
	"github.com/migadu/sora/server/cleaner"
	"github.com/migadu/sora/server/imap"
	"github.com/migadu/sora/server/lmtp"
	"github.com/migadu/sora/server/managesieve"
	"github.com/migadu/sora/server/pop3"
	"github.com/migadu/sora/server/uploader"
	"github.com/migadu/sora/storage"
)

func main() {
	seed := flag.Bool("seed", false, "Insert seed data into the database")
	insecureAuth := flag.Bool("insecure-auth", false, "Allow authentication without TLS")
	debug := flag.Bool("debug", false, "Print all commands and responses")

	// Define command-line flags for database and S3 credentials
	dbHost := flag.String("dbhost", "localhost", "Database host")
	dbPort := flag.String("dbport", "5432", "Database port")
	dbUser := flag.String("dbuser", "postgres", "Database user")
	dbPassword := flag.String("dbpassword", "", "Database password (can be empty for local development)")
	dbName := flag.String("dbname", "imap_db", "Database name")

	s3Endpoint := flag.String("s3endpoint", "", "S3 endpoint")
	s3AccessKey := flag.String("s3accesskey", "", "S3 access key")
	s3SecretKey := flag.String("s3secretkey", "", "S3 secret key")
	s3Bucket := flag.String("s3bucket", "", "S3 bucket name")

	startImap := flag.Bool("imap", true, "Start the IMAP server")
	imapAddr := flag.String("imapaddr", ":143", "IMAP server address")
	startLmtp := flag.Bool("lmtp", true, "Start the LMTP server")
	lmtpAddr := flag.String("lmtpaddr", ":24", "LMTP server address")
	startPop3 := flag.Bool("pop3", true, "Start the POP3 server")
	pop3Addr := flag.String("pop3addr", ":110", "POP3 server address")
	uploaderTempPath := flag.String("uploaderpath", "/tmp/sora/uploads", "Directory for pending uploads")
	cachePath := flag.String("cachedir", "/tmp/sora/cache", "Directory for cached files")
	maxCacheSize := flag.Int64("maxcachesize", 100*1024*1024, "Maximum cache size in bytes (default: 100MB)")

	startManageSieve := flag.Bool("managesieve", true, "Start the ManageSieve server")
	managesieveAddr := flag.String("managesieveaddr", ":4190", "ManageSieve server address")

	// Parse the command-line flags
	flag.Parse()

	if !*startImap && !*startLmtp && !*startPop3 {
		log.Fatal("No servers enabled. Please enable at least one server (IMAP, LMTP, or POP3).")
	}

	// Ensure required arguments are provided
	if *s3AccessKey == "" || *s3SecretKey == "" || *s3Bucket == "" {
		log.Fatal("Missing required credentials. Ensure S3 access key, secret key, and bucket are provided.")
	}

	// Initialize S3 storage
	log.Printf("Connecting to S3 endpoint %s, bucket %s", *s3Endpoint, *s3Bucket)
	s3storage, err := storage.NewS3Storage(*s3Endpoint, *s3AccessKey, *s3SecretKey, *s3Bucket, true)
	if err != nil {
		log.Fatalf("Failed to initialize S3 storage at endpoint %s: %v", *s3Endpoint, err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Handle SIGINT and SIGTERM for graceful shutdown
	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, os.Interrupt, syscall.SIGTERM)
	go func() {
		sig := <-signalChan
		log.Printf("Received signal: %s, shutting down...", sig)
		cancel()
	}()

	// Initialize the database connection
	log.Printf("Connecting to database at %s:%s as user %s, using database %s", *dbHost, *dbPort, *dbUser, *dbName)
	database, err := db.NewDatabase(ctx, *dbHost, *dbPort, *dbUser, *dbPassword, *dbName, *debug)
	if err != nil {
		log.Fatalf("Failed to connect to the database: %v", err)
	}
	defer database.Close() // Ensure the database is closed on exit

	// If the seed flag is set, insert test data
	// TODO: Remove this in production
	if *seed {
		log.Println("Seeding the database with test data...")
		err = database.InsertUser(ctx, "user@domain.com", "password")
		if err != nil {
			log.Fatalf("Failed to insert test user: %v", err)
		}
	}

	hostname, _ := os.Hostname()

	errChan := make(chan error, 1)

	// Initialize cache
	cache, err := cache.New(*cachePath, *maxCacheSize, database)
	if err != nil {
		log.Fatalf("Failed to initialize cache: %v", err)
	}
	if err := cache.SyncFromDisk(); err != nil {
		log.Fatalf("Failed to sync cache from disk: %v", err)
	}
	cache.StartPurgeLoop(ctx)

	// Initialize the S3 cleanup worker
	cleanupWorker := cleaner.New(database, s3storage, consts.CLEANUP_INTERVAL, consts.CLEANUP_GRACE_PERIOD)
	cleanupWorker.Start(ctx)

	// Start the upload worker
	uploadWorker, err := uploader.New(ctx, *uploaderTempPath, database, s3storage, cache, errChan)
	if err != nil {
		log.Fatalf("Failed to create upload worker: %v", err)
	}
	uploadWorker.Start(ctx)

	// Start LMTP server
	if *startLmtp {
		go startLMTPServer(ctx, hostname, *lmtpAddr, s3storage, database, uploadWorker, *debug, errChan)
	}

	// Start IMAP server
	if *startImap {
		go startIMAPServer(ctx, hostname, *imapAddr, s3storage, database, uploadWorker, cache, *insecureAuth, *debug, errChan)
	}

	// Start POP3 server
	if *startPop3 {
		go startPOP3Server(ctx, hostname, *pop3Addr, s3storage, database, uploadWorker, cache, *insecureAuth, *debug, errChan)
	}

	// Start ManageSieve server
	if *startManageSieve {
		go startManageSieveServer(ctx, hostname, *managesieveAddr, database, *insecureAuth, *debug, errChan)
	}

	// Wait for any errors from the servers
	select {
	case <-ctx.Done():
		log.Println("Context canceled. Shutting down.")
	case err := <-errChan:
		log.Fatalf("Server error: %v", err)
	}
}

func startIMAPServer(ctx context.Context, hostname, addr string, s3storage *storage.S3Storage, database *db.Database, uploadWorker *uploader.UploadWorker, cache *cache.Cache, insecureAuth bool, debug bool, errChan chan error) {
	s, err := imap.New(hostname, addr, s3storage, database, uploadWorker, cache, insecureAuth, debug)
	if err != nil {
		errChan <- err
		return
	}

	go func() {
		<-ctx.Done()
		log.Println("Shutting down IMAP server...")
		s.Close()
	}()

	if err := s.Serve(addr); err != nil && ctx.Err() == nil {
		errChan <- err
	}
}

func startLMTPServer(ctx context.Context, hostname, addr string, s3storage *storage.S3Storage, database *db.Database, uploadWorker *uploader.UploadWorker, debug bool, errChan chan error) {
	s := lmtp.New(hostname, addr, s3storage, database, uploadWorker, debug, errChan)

	go func() {
		<-ctx.Done()
		log.Println("Shutting down LMTP server...")
		s.Close()
	}()

	// block forever or until error is pushed into errChan by LMTP itself
	select {
	case <-ctx.Done():
	case err := <-errChan:
		log.Printf("LMTP server exited: %v", err)
	}
}

func startPOP3Server(ctx context.Context, hostname string, addr string, s3storage *storage.S3Storage, database *db.Database, uploadWorker *uploader.UploadWorker, cache *cache.Cache, insecureAuth bool, debug bool, errChan chan error) {
	s, err := pop3.New(hostname, addr, s3storage, database, uploadWorker, cache, insecureAuth, debug)
	if err != nil {
		errChan <- err
		return
	}

	go func() {
		<-ctx.Done()
		log.Println("Shutting down POP3 server...")
		s.Close()
	}()

	s.Start(errChan)
}

func startManageSieveServer(ctx context.Context, hostname string, addr string, database *db.Database, insecureAuth bool, debug bool, errChan chan error) {
	s, err := managesieve.New(hostname, addr, database, insecureAuth, debug)
	if err != nil {
		errChan <- err
		return
	}

	go func() {
		<-ctx.Done()
		log.Println("Shutting down ManageSieve server...")
		s.Close()
	}()

	s.Start(errChan)
}
