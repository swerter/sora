package managesieve

import (
	"context"

	"github.com/migadu/sora/db"
)

// DBer is an interface for database operations specific to ManageSieve
type DBer interface {
	GetUserIDByAddress(ctx context.Context, address string) (int64, error)
	Authenticate(ctx context.Context, userID int64, password string) error
	GetUserScripts(ctx context.Context, userID int64) ([]*db.SieveScript, error)
	GetActiveScript(ctx context.Context, userID int64) (*db.SieveScript, error)
	GetScriptByName(ctx context.Context, name string, userID int64) (*db.SieveScript, error)
	CreateScript(ctx context.Context, userID int64, name, script string) (*db.SieveScript, error)
	UpdateScript(ctx context.Context, scriptID, userID int64, name, script string) (*db.SieveScript, error)
	SetScriptActive(ctx context.Context, scriptID, userID int64, active bool) error
	DeleteScript(ctx context.Context, scriptID, userID int64) error
	Close()
}
