module github.com/migadu/sora

go 1.24.3

require (
	github.com/emersion/go-imap/v2 v2.0.0-beta.5.0.20250515140551-a1e4f0b6eb30
	github.com/emersion/go-message v0.18.2
	github.com/emersion/go-smtp v0.21.3
	github.com/foxcpp/go-sieve v0.0.0-20240130002450-72d6b002882a
	github.com/google/uuid v1.6.0
	github.com/jackc/pgx/v5 v5.7.4
	github.com/k3a/html2text v1.2.1
	github.com/mattn/go-sqlite3 v1.14.27
	github.com/minio/minio-go/v7 v7.0.89
	github.com/stretchr/testify v1.9.0
	golang.org/x/crypto v0.36.0
)

// Use the fork at github.com/migadu/go-sieve instead of the original repository
replace github.com/foxcpp/go-sieve => github.com/migadu/go-sieve v0.0.0-20240130002450-72d6b002882a

require (
	github.com/BurntSushi/toml v1.5.0 // indirect
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/dustin/go-humanize v1.0.1 // indirect
	github.com/emersion/go-sasl v0.0.0-20241020182733-b788ff22d5a6 // indirect
	github.com/go-ini/ini v1.67.0 // indirect
	github.com/goccy/go-json v0.10.5 // indirect
	github.com/jackc/pgpassfile v1.0.0 // indirect
	github.com/jackc/pgservicefile v0.0.0-20240606120523-5a60cdf6a761 // indirect
	github.com/jackc/puddle/v2 v2.2.2 // indirect
	github.com/klauspost/compress v1.18.0 // indirect
	github.com/klauspost/cpuid/v2 v2.2.10 // indirect
	github.com/kr/text v0.2.0 // indirect
	github.com/minio/crc64nvme v1.0.1 // indirect
	github.com/minio/md5-simd v1.1.2 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	github.com/rogpeppe/go-internal v1.14.1 // indirect
	github.com/rs/xid v1.6.0 // indirect
	github.com/stretchr/objx v0.5.2 // indirect
	golang.org/x/net v0.38.0 // indirect
	golang.org/x/sync v0.12.0 // indirect
	golang.org/x/sys v0.31.0 // indirect
	golang.org/x/text v0.23.0 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
	lukechampine.com/blake3 v1.4.1 // indirect
	rsc.io/binaryregexp v0.2.0 // indirect
)
