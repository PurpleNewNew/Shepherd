package global

import (
	"net"
	"testing"

	"codeberg.org/agnoie/shepherd/pkg/utils"
)

func TestInitialAndUpdateGComponent(t *testing.T) {
	store := NewStoreWithTransports(nil)
	if err := store.SetPreAuthToken("test-preauth-token"); err != nil {
		t.Fatalf("set preauth token: %v", err)
	}

	c1, c2 := net.Pipe()
	defer c1.Close()
	defer c2.Close()

	store.InitializeComponent(c1, "secret", "uuid", "raw", "raw")

	sess := store.ActiveSession()
	if sess == nil {
		t.Fatalf("expected active session after initialization")
	}
	if sess.Secret() != "secret" || sess.UUID() != "uuid" {
		t.Fatalf("unexpected session values: secret=%s uuid=%s", sess.Secret(), sess.UUID())
	}
	if sess.Conn() == nil {
		t.Fatalf("expected session connection")
	}

	store.UpdateActiveConn(c2)
	if conn := sess.Conn(); conn != nil {
		if sc, ok := conn.(*utils.SafeConn); !ok || sc.Conn != c2 {
			t.Fatalf("expected session connection to update to c2")
		}
	} else {
		t.Fatalf("expected connection after update")
	}
}
