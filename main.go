package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/BurntSushi/toml"
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
	// Initialize with application defaults
	cfg := newDefaultConfig()

	// --- Define Command-Line Flags ---
	// These flags will override values from the config file if set.
	// Their default values are set from the initial `cfg` for consistent -help messages.

	configPath := flag.String("config", "config.toml", "Path to TOML configuration file")

	// General flags
	fInsecureAuth := flag.Bool("insecure-auth", cfg.InsecureAuth, "Allow authentication without TLS (overrides config)")
	fDebug := flag.Bool("debug", cfg.Debug, "Print all commands and responses (overrides config)")

	// Database flags
	fDbHost := flag.String("dbhost", cfg.Database.Host, "Database host (overrides config)")
	fDbPort := flag.String("dbport", cfg.Database.Port, "Database port (overrides config)")
	fDbUser := flag.String("dbuser", cfg.Database.User, "Database user (overrides config)")
	fDbPassword := flag.String("dbpassword", cfg.Database.Password, "Database password (overrides config)")
	fDbName := flag.String("dbname", cfg.Database.Name, "Database name (overrides config)")

	// S3 flags
	fS3Endpoint := flag.String("s3endpoint", cfg.S3.Endpoint, "S3 endpoint (overrides config)")
	fS3AccessKey := flag.String("s3accesskey", cfg.S3.AccessKey, "S3 access key (overrides config)")
	fS3SecretKey := flag.String("s3secretkey", cfg.S3.SecretKey, "S3 secret key (overrides config)")
	fS3Bucket := flag.String("s3bucket", cfg.S3.Bucket, "S3 bucket name (overrides config)")

	// Server enable/address flags
	fStartImap := flag.Bool("imap", cfg.Servers.StartImap, "Start the IMAP server (overrides config)")
	fImapAddr := flag.String("imapaddr", cfg.Servers.ImapAddr, "IMAP server address (overrides config)")
	fStartLmtp := flag.Bool("lmtp", cfg.Servers.StartLmtp, "Start the LMTP server (overrides config)")
	fLmtpAddr := flag.String("lmtpaddr", cfg.Servers.LmtpAddr, "LMTP server address (overrides config)")
	fStartPop3 := flag.Bool("pop3", cfg.Servers.StartPop3, "Start the POP3 server (overrides config)")
	fPop3Addr := flag.String("pop3addr", cfg.Servers.Pop3Addr, "POP3 server address (overrides config)")
	fStartManageSieve := flag.Bool("managesieve", cfg.Servers.StartManageSieve, "Start the ManageSieve server (overrides config)")
	fManagesieveAddr := flag.String("managesieveaddr", cfg.Servers.ManageSieveAddr, "ManageSieve server address (overrides config)")

	// Paths flags
	fUploaderTempPath := flag.String("uploaderpath", cfg.Paths.UploaderTemp, "Directory for pending uploads (overrides config)")
	fCachePath := flag.String("cachedir", cfg.Paths.CacheDir, "Directory for cached files (overrides config)")
	fMaxCacheSizeMB := flag.Int64("cachesize", cfg.Paths.MaxCacheSizeMB, "Disk cache size in Megabytes (overrides config)")

	// LMTP specific
	fExternalRelay := flag.String("externalrelay", cfg.LMTP.ExternalRelay, "External relay for LMTP (overrides config)")

	// TLS general
	fTlsInsecureSkipVerify := flag.Bool("tlsinsecureskipverify", cfg.TLS.InsecureSkipVerify, "Skip TLS cert verification (overrides config)")

	// TLS specific flags (enable, cert, key for each service)
	fImapTLS := flag.Bool("imaptls", cfg.TLS.IMAP.Enable, "Enable TLS for IMAP (overrides config)")
	fImapTLSCert := flag.String("imaptlscert", cfg.TLS.IMAP.CertFile, "TLS cert for IMAP (overrides config)")
	fImapTLSKey := flag.String("imaptlskey", cfg.TLS.IMAP.KeyFile, "TLS key for IMAP (overrides config)")

	fPop3TLS := flag.Bool("pop3tls", cfg.TLS.POP3.Enable, "Enable TLS for POP3 (overrides config)")
	fPop3TLSCert := flag.String("pop3tlscert", cfg.TLS.POP3.CertFile, "TLS cert for POP3 (overrides config)")
	fPop3TLSKey := flag.String("pop3tlskey", cfg.TLS.POP3.KeyFile, "TLS key for POP3 (overrides config)")

	fLmtpTLS := flag.Bool("lmtptls", cfg.TLS.LMTP.Enable, "Enable TLS for LMTP (overrides config)")
	fLmtpTLSCert := flag.String("lmtptlscert", cfg.TLS.LMTP.CertFile, "TLS cert for LMTP (overrides config)")
	fLmtpTLSKey := flag.String("lmtptlskey", cfg.TLS.LMTP.KeyFile, "TLS key for LMTP (overrides config)")

	fManageSieveTLS := flag.Bool("managesievetls", cfg.TLS.ManageSieve.Enable, "Enable TLS for ManageSieve (overrides config)")
	fManageSieveTLSCert := flag.String("managesievetlscert", cfg.TLS.ManageSieve.CertFile, "TLS cert for ManageSieve (overrides config)")
	fManageSieveTLSKey := flag.String("managesievetlskey", cfg.TLS.ManageSieve.KeyFile, "TLS key for ManageSieve (overrides config)")

	flag.Parse()

	// --- Load Configuration from TOML File ---
	// Values from the TOML file will override the application defaults.
	if _, err := toml.DecodeFile(*configPath, &cfg); err != nil {
		if os.IsNotExist(err) {
			// If -config flag was explicitly set by user and file not found, it's an error.
			// Otherwise, if default config.toml is not found, it's just a warning.
			if isFlagSet("config") {
				log.Fatalf("Error: Specified configuration file '%s' not found: %v", *configPath, err)
			} else {
				log.Printf("Warning: Default configuration file '%s' not found. Using application defaults and command-line flags.", *configPath)
			}
		} else { // Other errors (e.g., parsing error)
			log.Fatalf("Error parsing configuration file '%s': %v", *configPath, err)
		}
	} else {
		log.Printf("Loaded configuration from %s", *configPath)
	}

	// --- Apply Command-Line Flag Overrides ---
	// If a flag was explicitly set on the command line, its value overrides both
	// application defaults and values from the TOML file.
	if isFlagSet("insecure-auth") {
		cfg.InsecureAuth = *fInsecureAuth
	}
	if isFlagSet("debug") {
		cfg.Debug = *fDebug
	}

	if isFlagSet("dbhost") {
		cfg.Database.Host = *fDbHost
	}
	if isFlagSet("dbport") {
		cfg.Database.Port = *fDbPort
	}
	if isFlagSet("dbuser") {
		cfg.Database.User = *fDbUser
	}
	if isFlagSet("dbpassword") {
		cfg.Database.Password = *fDbPassword
	}
	if isFlagSet("dbname") {
		cfg.Database.Name = *fDbName
	}

	if isFlagSet("s3endpoint") {
		cfg.S3.Endpoint = *fS3Endpoint
	}
	if isFlagSet("s3accesskey") {
		cfg.S3.AccessKey = *fS3AccessKey
	}
	if isFlagSet("s3secretkey") {
		cfg.S3.SecretKey = *fS3SecretKey
	}
	if isFlagSet("s3bucket") {
		cfg.S3.Bucket = *fS3Bucket
	}

	if isFlagSet("imap") {
		cfg.Servers.StartImap = *fStartImap
	}
	if isFlagSet("imapaddr") {
		cfg.Servers.ImapAddr = *fImapAddr
	}
	if isFlagSet("lmtp") {
		cfg.Servers.StartLmtp = *fStartLmtp
	}
	if isFlagSet("lmtpaddr") {
		cfg.Servers.LmtpAddr = *fLmtpAddr
	}
	if isFlagSet("pop3") {
		cfg.Servers.StartPop3 = *fStartPop3
	}
	if isFlagSet("pop3addr") {
		cfg.Servers.Pop3Addr = *fPop3Addr
	}
	if isFlagSet("managesieve") {
		cfg.Servers.StartManageSieve = *fStartManageSieve
	}
	if isFlagSet("managesieveaddr") {
		cfg.Servers.ManageSieveAddr = *fManagesieveAddr
	}

	if isFlagSet("uploaderpath") {
		cfg.Paths.UploaderTemp = *fUploaderTempPath
	}
	if isFlagSet("cachedir") {
		cfg.Paths.CacheDir = *fCachePath
	}
	if isFlagSet("cachesize") {
		cfg.Paths.MaxCacheSizeMB = *fMaxCacheSizeMB
	}

	if isFlagSet("externalrelay") {
		cfg.LMTP.ExternalRelay = *fExternalRelay
	}

	if isFlagSet("tlsinsecureskipverify") {
		cfg.TLS.InsecureSkipVerify = *fTlsInsecureSkipVerify
	}

	if isFlagSet("imaptls") {
		cfg.TLS.IMAP.Enable = *fImapTLS
	}
	if isFlagSet("imaptlscert") {
		cfg.TLS.IMAP.CertFile = *fImapTLSCert
	}
	if isFlagSet("imaptlskey") {
		cfg.TLS.IMAP.KeyFile = *fImapTLSKey
	}

	if isFlagSet("pop3tls") {
		cfg.TLS.POP3.Enable = *fPop3TLS
	}
	if isFlagSet("pop3tlscert") {
		cfg.TLS.POP3.CertFile = *fPop3TLSCert
	}
	if isFlagSet("pop3tlskey") {
		cfg.TLS.POP3.KeyFile = *fPop3TLSKey
	}

	if isFlagSet("lmtptls") {
		cfg.TLS.LMTP.Enable = *fLmtpTLS
	}
	if isFlagSet("lmtptlscert") {
		cfg.TLS.LMTP.CertFile = *fLmtpTLSCert
	}
	if isFlagSet("lmtptlskey") {
		cfg.TLS.LMTP.KeyFile = *fLmtpTLSKey
	}

	if isFlagSet("managesievetls") {
		cfg.TLS.ManageSieve.Enable = *fManageSieveTLS
	}
	if isFlagSet("managesievetlscert") {
		cfg.TLS.ManageSieve.CertFile = *fManageSieveTLSCert
	}
	if isFlagSet("managesievetlskey") {
		cfg.TLS.ManageSieve.KeyFile = *fManageSieveTLSKey
	}

	// --- Application Logic using cfg ---

	if !cfg.Servers.StartImap && !cfg.Servers.StartLmtp && !cfg.Servers.StartPop3 && !cfg.Servers.StartManageSieve {
		log.Fatal("No servers enabled. Please enable at least one server (IMAP, LMTP, or POP3).")
	}

	// Ensure required arguments are provided
	if cfg.S3.AccessKey == "" || cfg.S3.SecretKey == "" || cfg.S3.Bucket == "" {
		log.Fatal("Missing required credentials. Ensure S3 access key, secret key, and bucket are provided.")
	}

	// Initialize S3 storage
	s3EndpointToUse := cfg.S3.Endpoint
	if s3EndpointToUse == "" {
		// Let the storage library use its default if endpoint is empty,
		// or set a specific default like "s3.amazonaws.com"
		log.Println("S3 endpoint not specified, using AWS default.")
	}
	log.Printf("Connecting to S3 endpoint '%s', bucket '%s'", s3EndpointToUse, cfg.S3.Bucket)
	s3storage, err := storage.New(s3EndpointToUse, cfg.S3.AccessKey, cfg.S3.SecretKey, cfg.S3.Bucket, true)
	if err != nil {
		log.Fatalf("Failed to initialize S3 storage at endpoint '%s': %v", s3EndpointToUse, err)
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
	log.Printf("Connecting to database at %s:%s as user %s, using database %s", cfg.Database.Host, cfg.Database.Port, cfg.Database.User, cfg.Database.Name)
	database, err := db.NewDatabase(ctx, cfg.Database.Host, cfg.Database.Port, cfg.Database.User, cfg.Database.Password, cfg.Database.Name, cfg.Debug)
	if err != nil {
		log.Fatalf("Failed to connect to the database: %v", err)
	}
	defer database.Close() // Ensure the database is closed on exit

	hostname, _ := os.Hostname()

	errChan := make(chan error, 1)

	// Initialize cache
	actualCacheSizeBytes := cfg.Paths.MaxCacheSizeMB * 1024 * 1024
	cacheInstance, err := cache.New(cfg.Paths.CacheDir, actualCacheSizeBytes, database)
	if err != nil {
		log.Fatalf("Failed to initialize cache: %v", err)
	}
	defer cacheInstance.Close() // Ensure the cache is closed on exit

	if err := cacheInstance.SyncFromDisk(); err != nil {
		log.Fatalf("Failed to sync cache from disk: %v", err)
	}
	cacheInstance.StartPurgeLoop(ctx)

	// Initialize the S3 cleanup worker
	cleanupWorker := cleaner.New(database, s3storage, cacheInstance, consts.CLEANUP_INTERVAL, consts.CLEANUP_GRACE_PERIOD)
	cleanupWorker.Start(ctx)

	// Start the upload worker
	uploadWorker, err := uploader.New(ctx, cfg.Paths.UploaderTemp, hostname, database, s3storage, cacheInstance, errChan)
	if err != nil {
		log.Fatalf("Failed to create upload worker: %v", err)
	}
	uploadWorker.Start(ctx)

	// Start LMTP server
	if cfg.Servers.StartLmtp {
		go startLMTPServer(ctx, hostname, cfg.Servers.LmtpAddr, s3storage, database, uploadWorker, cfg.Debug, cfg.LMTP.ExternalRelay, errChan, cfg.TLS.LMTP.CertFile, cfg.TLS.LMTP.KeyFile, cfg.TLS.InsecureSkipVerify)
	}

	// Start IMAP server
	if cfg.Servers.StartImap {
		go startIMAPServer(ctx, hostname, cfg.Servers.ImapAddr, s3storage, database, uploadWorker, cacheInstance, cfg.InsecureAuth, cfg.Debug, errChan, cfg.TLS.IMAP.CertFile, cfg.TLS.IMAP.KeyFile, cfg.TLS.InsecureSkipVerify)
	}

	// Start POP3 server
	if cfg.Servers.StartPop3 {
		go startPOP3Server(ctx, hostname, cfg.Servers.Pop3Addr, s3storage, database, uploadWorker, cacheInstance, cfg.InsecureAuth, cfg.Debug, errChan, cfg.TLS.POP3.CertFile, cfg.TLS.POP3.KeyFile, cfg.TLS.InsecureSkipVerify)
	}

	// Start ManageSieve server
	if cfg.Servers.StartManageSieve {
		go startManageSieveServer(ctx, hostname, cfg.Servers.ManageSieveAddr, database, cfg.InsecureAuth, cfg.Debug, errChan, cfg.TLS.ManageSieve.CertFile, cfg.TLS.ManageSieve.KeyFile, cfg.TLS.InsecureSkipVerify)
	}

	// Wait for any errors from the servers
	select {
	case <-ctx.Done():
		log.Println("Shutting down SORA servers...")
	case err := <-errChan:
		log.Fatalf("Server error: %v", err)
	}
}

// Note: The startXServer functions now take certFile and keyFile directly.
// The logic for whether TLS is enabled for a server (e.g., cfg.TLS.IMAP.Enable)
// should ideally be handled *inside* the imap.New (or equivalent) function,
// or you pass the enable flag as well. For simplicity here, we assume New handles it if cert/key are provided.

func startIMAPServer(ctx context.Context, hostname, addr string, s3storage *storage.S3Storage, database *db.Database, uploadWorker *uploader.UploadWorker, cacheInstance *cache.Cache, insecureAuth bool, debug bool, errChan chan error, tlsCertFile, tlsKeyFile string, insecureSkipVerify bool) {
	s, err := imap.New(ctx, hostname, addr, s3storage, database, uploadWorker, cacheInstance, insecureAuth, debug, tlsCertFile, tlsKeyFile, insecureSkipVerify) // Assumes New checks if tlsCertFile/KeyFile are empty to enable TLS
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

func startLMTPServer(ctx context.Context, hostname, addr string, s3storage *storage.S3Storage, database *db.Database, uploadWorker *uploader.UploadWorker, debug bool, externalRelay string, errChan chan error, tlsCertFile, tlsKeyFile string, insecureSkipVerify bool) {
	// lmtp.New now returns the server instance without starting ListenAndServe
	lmtpServer, err := lmtp.New(ctx, hostname, addr, s3storage, database, uploadWorker, debug, externalRelay, tlsCertFile, tlsKeyFile, insecureSkipVerify)
	if err != nil {
		errChan <- fmt.Errorf("failed to create LMTP server: %w", err)
		return
	}

	go func() {
		<-ctx.Done()
		log.Println("Shutting down LMTP server...")
		if err := lmtpServer.Close(); err != nil {
			log.Printf("Error closing LMTP server: %v", err)
		}
	}()

	lmtpServer.Start(errChan)
}

func startPOP3Server(ctx context.Context, hostname string, addr string, s3storage *storage.S3Storage, database *db.Database, uploadWorker *uploader.UploadWorker, cacheInstance *cache.Cache, insecureAuth bool, debug bool, errChan chan error, tlsCertFile, tlsKeyFile string, insecureSkipVerify bool) {
	s, err := pop3.New(ctx, hostname, addr, s3storage, database, uploadWorker, cacheInstance, insecureAuth, debug, tlsCertFile, tlsKeyFile, insecureSkipVerify) // Pass ctx
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

func startManageSieveServer(ctx context.Context, hostname string, addr string, database *db.Database, insecureAuth bool, debug bool, errChan chan error, tlsCertFile, tlsKeyFile string, insecureSkipVerify bool) {
	s, err := managesieve.New(ctx, hostname, addr, database, insecureAuth, debug, tlsCertFile, tlsKeyFile, insecureSkipVerify) // Pass ctx
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

// Helper function to check if a flag was explicitly set on the command line
func isFlagSet(name string) bool {
	isSet := false
	flag.Visit(func(f *flag.Flag) {
		if f.Name == name {
			isSet = true
		}
	})
	return isSet
}
