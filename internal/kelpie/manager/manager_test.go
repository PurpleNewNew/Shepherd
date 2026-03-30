package manager

import (
	"testing"

	"codeberg.org/agnoie/shepherd/pkg/global"
)

func TestNewManagerInitializesManagers(t *testing.T) {
	store := global.NewStoreWithTransports(nil)
	if err := store.SetPreAuthToken("test-preauth-token"); err != nil {
		t.Fatalf("set preauth token: %v", err)
	}
	mgr := NewManager(store)
	if mgr.ConsoleManager == nil || mgr.ChildrenManager == nil {
		t.Fatalf("expected sub-managers to be initialized")
	}
}
