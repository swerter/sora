package consts

import "time"

const BATCH_SIZE = 10
const MAX_CONCURRENCY = 20
const MAX_UPLOAD_ATTEMPTS = 5
const PENDING_UPLOAD_RETRY_INTERVAL = 30 * time.Second
