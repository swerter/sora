package consts

import "time"

const CLEANUP_GRACE_PERIOD = time.Hour * 24 * 14 // 14 days in seconds
const CLEANUP_INTERVAL = time.Minute * 60        // 1 hour in seconds
