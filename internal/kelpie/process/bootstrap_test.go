package process

import (
	"context"
	"testing"

	"codeberg.org/agnoie/shepherd/internal/kelpie/initial"
	"codeberg.org/agnoie/shepherd/internal/kelpie/topology"
	"codeberg.org/agnoie/shepherd/pkg/global"
)

func TestAdminBuilderRespectsProvidedSession(t *testing.T) {
	store := global.NewStoreWithTransports(nil)
	topo := topology.NewTopology()
	sess, client := newTestSession("BUILDER-ENTRY")
	defer func() {
		_ = sess.Conn().Close()
		_ = client.Close()
	}()

	admin := newAdminBuilder(context.Background(), &initial.Options{}, topo, store).
		withSession(sess).
		build()
	defer admin.Stop()

	if current := admin.currentSession(); current != sess {
		t.Fatalf("expected builder to use provided session")
	}
}
