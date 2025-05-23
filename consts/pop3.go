package consts

import "time"

const POP3_MAX_ERRORS_ALLOWED = 3
const POP3_ERROR_DELAY = 3 * time.Second
const POP3_IDLE_TIMEOUT = 5 * time.Minute // Maximum duration of inactivity before the connection is closed
