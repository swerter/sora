package sieveengine

import "testing"

// TestValidateScript pins what every script write path checks before storing.
func TestValidateScript(t *testing.T) {
	if err := ValidateScript(`require ["fileinto", "mime"]; if header :mime :anychild :type "Content-Type" "image" { fileinto "Images"; }`, nil); err != nil {
		t.Fatalf("default set rejects a script delivery compiles: %v", err)
	}
	if err := ValidateScript(`require "enclose"; keep;`, nil); err == nil {
		t.Fatal("accepted an extension the engine does not have; delivery would skip the script whole")
	}
	if err := ValidateScript(`require "editheader"; addheader "X-A" "b";`, nil); err == nil {
		t.Fatal("accepted editheader, which is opt-in")
	}
	if err := ValidateScript(`require "editheader"; addheader "X-A" "b";`, []string{"editheader"}); err != nil {
		t.Fatalf("rejected editheader although configured: %v", err)
	}
	if err := ValidateScript(`if header :contains "subject" "x" { fileinto "X"; }`, nil); err == nil {
		t.Fatal("accepted fileinto without its require")
	}
}
