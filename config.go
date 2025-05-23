package main

import (
	"github.com/migadu/sora/consts"
)

// Config holds all configuration for the application.
type Config struct {
	InsecureAuth bool `toml:"insecure_auth"`
	Debug        bool `toml:"debug"`

	Database struct {
		Host     string `toml:"host"`
		Port     string `toml:"port"`
		User     string `toml:"user"`
		Password string `toml:"password"`
		Name     string `toml:"name"`
	} `toml:"database"`

	S3 struct {
		Endpoint  string `toml:"endpoint"`
		AccessKey string `toml:"access_key"`
		SecretKey string `toml:"secret_key"`
		Bucket    string `toml:"bucket"`
	} `toml:"s3"`

	Servers struct {
		StartImap        bool   `toml:"start_imap"`
		ImapAddr         string `toml:"imap_addr"`
		StartLmtp        bool   `toml:"start_lmtp"`
		LmtpAddr         string `toml:"lmtp_addr"`
		StartPop3        bool   `toml:"start_pop3"`
		Pop3Addr         string `toml:"pop3_addr"`
		StartManageSieve bool   `toml:"start_managesieve"`
		ManageSieveAddr  string `toml:"managesieve_addr"`
	} `toml:"servers"`

	Paths struct {
		UploaderTemp   string `toml:"uploader_temp_path"`
		CacheDir       string `toml:"cache_dir"`
		MaxCacheSizeMB int64  `toml:"max_cache_size_mb"`
	} `toml:"paths"`

	LMTP struct {
		ExternalRelay string `toml:"external_relay"`
	} `toml:"lmtp"`

	TLS struct {
		InsecureSkipVerify bool `toml:"insecure_skip_verify"`
		IMAP               struct {
			Enable   bool   `toml:"enable"`
			CertFile string `toml:"cert_file"`
			KeyFile  string `toml:"key_file"`
		} `toml:"imap"`
		POP3 struct {
			Enable   bool   `toml:"enable"`
			CertFile string `toml:"cert_file"`
			KeyFile  string `toml:"key_file"`
		} `toml:"pop3"`
		LMTP struct {
			Enable   bool   `toml:"enable"`
			CertFile string `toml:"cert_file"`
			KeyFile  string `toml:"key_file"`
		} `toml:"lmtp"`
		ManageSieve struct {
			Enable   bool   `toml:"enable"`
			CertFile string `toml:"cert_file"`
			KeyFile  string `toml:"key_file"`
		} `toml:"managesieve"`
	} `toml:"tls"`
}

// newDefaultConfig creates a Config struct with default values.
func newDefaultConfig() Config {
	return Config{
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
		TLS: struct {
			InsecureSkipVerify bool `toml:"insecure_skip_verify"`
			IMAP               struct {
				Enable   bool   `toml:"enable"`
				CertFile string `toml:"cert_file"`
				KeyFile  string `toml:"key_file"`
			} `toml:"imap"`
			POP3 struct {
				Enable   bool   `toml:"enable"`
				CertFile string `toml:"cert_file"`
				KeyFile  string `toml:"key_file"`
			} `toml:"pop3"`
			LMTP struct {
				Enable   bool   `toml:"enable"`
				CertFile string `toml:"cert_file"`
				KeyFile  string `toml:"key_file"`
			} `toml:"lmtp"`
			ManageSieve struct {
				Enable   bool   `toml:"enable"`
				CertFile string `toml:"cert_file"`
				KeyFile  string `toml:"key_file"`
			} `toml:"managesieve"`
		}{
			InsecureSkipVerify: false,
			IMAP: struct {
				Enable   bool   `toml:"enable"`
				CertFile string `toml:"cert_file"`
				KeyFile  string `toml:"key_file"`
			}{Enable: false},
			POP3: struct {
				Enable   bool   `toml:"enable"`
				CertFile string `toml:"cert_file"`
				KeyFile  string `toml:"key_file"`
			}{Enable: false},
			LMTP: struct {
				Enable   bool   `toml:"enable"`
				CertFile string `toml:"cert_file"`
				KeyFile  string `toml:"key_file"`
			}{Enable: false},
			ManageSieve: struct {
				Enable   bool   `toml:"enable"`
				CertFile string `toml:"cert_file"`
				KeyFile  string `toml:"key_file"`
			}{Enable: false},
		},
	}
}
