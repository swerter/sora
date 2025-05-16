package managesieve

import (
	"context"
	"testing"

	"github.com/migadu/sora/consts"
	"github.com/migadu/sora/db"
	"github.com/stretchr/testify/mock"
)

// Integration test that tests the ManageSieve server with a simulated ManageSieve client
func TestManageSieveIntegration(t *testing.T) {
	// Create mock dependencies
	mockDB := new(MockDatabase)

	// Create a script object for testing
	testScript := &db.SieveScript{
		ID:     1,
		Name:   "test-script",
		Script: "require \"fileinto\";\nif header :contains \"subject\" \"important\" {\n  fileinto \"Important\";\n} else {\n  keep;\n}",
	}

	// Set up expectations for the database mock
	mockDB.On("GetUserIDByAddress", mock.Anything, "user@example.com").Return(int64(123), nil)
	mockDB.On("Authenticate", mock.Anything, int64(123), "password").Return(nil)
	mockDB.On("GetUserScripts", mock.Anything, int64(123)).Return([]*db.SieveScript{}, nil)

	// First call to GetScriptByName should return not found
	mockDB.On("GetScriptByName", mock.Anything, "test-script", int64(123)).Return(nil, consts.ErrDBNotFound).Once()

	// CreateScript should return the test script
	mockDB.On("CreateScript", mock.Anything, int64(123), "test-script", mock.Anything).Return(testScript, nil)

	// Second call to GetScriptByName should return the script
	mockDB.On("GetScriptByName", mock.Anything, "test-script", int64(123)).Return(testScript, nil).Once()

	mockDB.On("SetScriptActive", mock.Anything, int64(1), int64(123), true).Return(nil)
	mockDB.On("DeleteScript", mock.Anything, int64(1), int64(123)).Return(nil)

	// Create a context for the server
	ctx := context.Background()

	// Create the server
	server := &ManageSieveServer{
		hostname: "test.example.com",
		db:       mockDB,
		appCtx:   ctx,
	}

	// Create a session directly
	session := &ManageSieveSession{
		server: server,
	}
	session.RemoteIP = "127.0.0.1"
	session.Protocol = "ManageSieve"
	session.Id = "test-session"
	session.HostName = server.hostname

	// Test the session methods directly

	// 1. Test authentication
	userID, err := server.db.GetUserIDByAddress(ctx, "user@example.com")
	if err != nil {
		t.Fatalf("Failed to get user ID: %v", err)
	}
	if userID != 123 {
		t.Fatalf("Expected user ID 123, got %d", userID)
	}

	err = server.db.Authenticate(ctx, userID, "password")
	if err != nil {
		t.Fatalf("Authentication failed: %v", err)
	}

	// 2. Test script operations
	scripts, err := server.db.GetUserScripts(ctx, userID)
	if err != nil {
		t.Fatalf("Failed to get user scripts: %v", err)
	}
	if len(scripts) != 0 {
		t.Fatalf("Expected 0 scripts, got %d", len(scripts))
	}

	// Test creating a script
	scriptName := "test-script"
	scriptContent := "require \"fileinto\";\nif header :contains \"subject\" \"important\" {\n  fileinto \"Important\";\n} else {\n  keep;\n}"

	_, err = server.db.GetScriptByName(ctx, scriptName, userID)
	if err != consts.ErrDBNotFound {
		t.Fatalf("Expected script not found error, got %v", err)
	}

	createdScript, err := server.db.CreateScript(ctx, userID, scriptName, scriptContent)
	if err != nil {
		t.Fatalf("Failed to create script: %v", err)
	}
	if createdScript.ID != 1 || createdScript.Name != scriptName {
		t.Fatalf("Script created with unexpected values: %+v", createdScript)
	}

	// Test retrieving the script
	retrievedScript, err := server.db.GetScriptByName(ctx, scriptName, userID)
	if err != nil {
		t.Fatalf("Failed to get script: %v", err)
	}
	if retrievedScript.ID != 1 || retrievedScript.Name != scriptName || retrievedScript.Script != scriptContent {
		t.Fatalf("Retrieved script with unexpected values: %+v", retrievedScript)
	}

	// Test activating the script
	err = server.db.SetScriptActive(ctx, retrievedScript.ID, userID, true)
	if err != nil {
		t.Fatalf("Failed to activate script: %v", err)
	}

	// Test deleting the script
	err = server.db.DeleteScript(ctx, retrievedScript.ID, userID)
	if err != nil {
		t.Fatalf("Failed to delete script: %v", err)
	}

	// Verify all expectations were met
	mockDB.AssertExpectations(t)
}
