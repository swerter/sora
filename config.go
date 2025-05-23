package main

import (
	"github.com/migadu/sora/consts"
)

// DatabaseConfig holds database configuration.
type DatabaseConfig struct {
	Host     string `toml:"host"`
	Port     string `toml:"port"`
	User     string `toml:"user"`
	Password string `toml:"password"`
	Name     string `toml:"name"`
}

// S3Config holds S3 configuration.
type S3Config struct {
	Endpoint  string `toml:"endpoint"`
	AccessKey string `toml:"access_key"`
	SecretKey string `toml:"secret_key"`
	Bucket    string `toml:"bucket"`
}

// ServersConfig holds server configuration.
type ServersConfig struct {
	StartImap        bool   `toml:"start_imap"`
	ImapAddr         string `toml:"imap_addr"`
	StartLmtp        bool   `toml:"start_lmtp"`
	LmtpAddr         string `toml:"lmtp_addr"`
	StartPop3        bool   `toml:"start_pop3"`
	Pop3Addr         string `toml:"pop3_addr"`
	StartManageSieve bool   `toml:"start_managesieve"`
	ManageSieveAddr  string `toml:"managesieve_addr"`
}

// PathsConfig holds paths configuration.
type PathsConfig struct {
	UploaderTemp   string `toml:"uploader_temp_path"`
	CacheDir       string `toml:"cache_dir"`
	MaxCacheSizeMB int64  `toml:"max_cache_size_mb"`
}

// LMTPConfig holds LMTP configuration.
type LMTPConfig struct {
	ExternalRelay string `toml:"external_relay"`
}

// TLSSubConfig holds TLS sub-configuration for each protocol.
type TLSSubConfig struct {
	Enable   bool   `toml:"enable"`
	CertFile string `toml:"cert_file"`
	KeyFile  string `toml:"key_file"`
}

// TLSConfig holds TLS configuration.
type TLSConfig struct {
	InsecureSkipVerify bool         `toml:"insecure_skip_verify"`
	IMAP               TLSSubConfig `toml:"imap"`
	POP3               TLSSubConfig `toml:"pop3"`
	LMTP               TLSSubConfig `toml:"lmtp"`
	ManageSieve        TLSSubConfig `toml:"managesieve"`
}

// Config holds all configuration for the application.
type Config struct {
	LogOutput    string         `toml:"log_output"`
	InsecureAuth bool           `toml:"insecure_auth"`
	Debug        bool           `toml:"debug"`
	Database     DatabaseConfig `toml:"database"`
	S3           S3Config       `toml:"s3"`
	Servers      ServersConfig  `toml:"servers"`
	Paths        PathsConfig    `toml:"paths"`
	LMTP         LMTPConfig     `toml:"lmtp"`
	TLS          TLSConfig      `toml:"tls"`
}

// newDefaultConfig creates a Config struct with default values.
func newDefaultConfig() Config {
	return Config{
		LogOutput:    "syslog",
		InsecureAuth: false,
		Debug:        false,
		Database: struct {
			Host     string `toml:"host"`
			Port     string `toml:"port"`
			User     string `toml:"user"`
			Password string `toml:"password"`
			Name     string `toml:"name"`
		}{
			Host:     "localhost",
			Port:     "5432",
			User:     "postgres",
			Password: "",
			Name:     "sora_mail_db",
		},
		S3: struct {
			Endpoint  string `toml:"endpoint"`
			AccessKey string `toml:"access_key"`
			SecretKey string `toml:"secret_key"`
			Bucket    string `toml:"bucket"`
		}{
			Endpoint:  "",
			AccessKey: "",
			SecretKey: "",
			Bucket:    "",
		},
		Servers: struct {
			StartImap        bool   `toml:"start_imap"`
			ImapAddr         string `toml:"imap_addr"`
			StartLmtp        bool   `toml:"start_lmtp"`
			LmtpAddr         string `toml:"lmtp_addr"`
			StartPop3        bool   `toml:"start_pop3"`
			Pop3Addr         string `toml:"pop3_addr"`
			StartManageSieve bool   `toml:"start_managesieve"`
			ManageSieveAddr  string `toml:"managesieve_addr"`
		}{
			StartImap:        true,
			ImapAddr:         ":143",
			StartLmtp:        true,
			LmtpAddr:         ":24",
			StartPop3:        true,
			Pop3Addr:         ":110",
			StartManageSieve: true,
			ManageSieveAddr:  ":4190",
		},
		Paths: struct {
			UploaderTemp   string `toml:"uploader_temp_path"`
			CacheDir       string `toml:"cache_dir"`
			MaxCacheSizeMB int64  `toml:"max_cache_size_mb"`
		}{
			UploaderTemp:   "/tmp/sora/uploads",
			CacheDir:       "/tmp/sora/cache",
			MaxCacheSizeMB: consts.MAX_TOTAL_CACHE_SIZE / (1024 * 1024),
		},
		LMTP: struct {
			ExternalRelay string `toml:"external_relay"`
		}{
			ExternalRelay: "",
		},
		TLS: TLSConfig{
			InsecureSkipVerify: false,
			IMAP: TLSSubConfig{
				Enable:   false,
				CertFile: "",
				KeyFile:  "",
			},
			POP3: TLSSubConfig{
				Enable:   false,
				CertFile: "",
				KeyFile:  "",
			},
			LMTP: TLSSubConfig{
				Enable:   false,
				CertFile: "",
				KeyFile:  "",
			},
			ManageSieve: TLSSubConfig{
				Enable:   false,
				CertFile: "",
				KeyFile:  "",
			},
		},
	}
}
