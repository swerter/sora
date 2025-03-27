# SORA — Hackable IMAP Server
**Status: Experimental – Not Production Ready**

**Sora** is a simple, minimalistic IMAP server built for composability.  
It serves as a lightweight building block for larger infrastructure systems, with an emphasis on correctness, extensibility, and simplicity.

---

## Features

- Built around the [go-imap](https://github.com/emersion/go-imap) library
- Standards-compliant **IMAP4rev1** server
- **S3-compatible** object storage for message bodies
- **PostgreSQL** for metadata and indexing
- Minimal dependencies and clean, understandable codebase
- Can be embedded as a Go module or deployed as a standalone daemon
- Fast startup and efficient resource usage
- **LMTP** support for message delivery
- **POP3** support
- **ManageSIEVE** and **SIEVE** support _coming soon_

---

## Use Cases

Sora is ideal for:

- Custom cloud-native email infrastructure
- Research and experimentation
- Integrating with modern storage and indexing backends
- Self-hosted environments with external authentication and delivery pipelines

---

## Status: Experimental

Sora is functional, but **not yet production-ready**.  
Cross-client compatibility is still being tested. Some clients may misbehave or fail to operate correctly.

Use in test environments only. Patches and pull requests are welcome.

---

## Requirements

- Go 1.20+
- PostgreSQL
- S3-compatible object storage (e.g. MinIO, AWS S3)

---

## Getting Started

```bash
git clone https://github.com/yourname/sora.git
cd sora
go run main.go -insecure-auth -debug -seed \
  -imap -lmtp -pop3 \
  -s3accesskey YOUR_S3_ACCESS_KEY -s3bucket YOUR_S3_BUCKET -s3secretkey YOUR_S3_SECRET_KEY -s3endpoint YOUR_S3_ENDPOINT \
  -dbhost DB_HOST \
  -debug     
