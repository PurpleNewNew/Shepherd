package manager

import (
	"context"
	"net"
	"strconv"
	"sync"
	"testing"
	"time"
)

func startSupplementalManager(t *testing.T) (*supplementalManager, context.CancelFunc) {
	t.Helper()
	manager := newSupplementalManager()
	ctx, cancel := context.WithCancel(context.Background())
	go manager.run(ctx)
	return manager, cancel
}

func submitSupplementalTask(t *testing.T, manager *supplementalManager, task *SupplementalTask) *SupplementalResult {
	t.Helper()
	select {
	case manager.TaskChan <- task:
	case <-time.After(time.Second):
		t.Fatalf("timed out submitting task %+v", task)
	}
	select {
	case res := <-manager.ResultChan:
		return res
	case <-time.After(time.Second):
		t.Fatalf("timed out waiting for result of task %+v", task)
	}
	return nil
}

func TestSupplementalManagerConcurrentAttach(t *testing.T) {
	manager, cancel := startSupplementalManager(t)
	defer cancel()

	const linkID = "link-concurrent"
	link := &SupplementalLink{
		LinkUUID: linkID,
		PeerUUID: "peer-node",
		State:    SuppStatePending,
	}

	if res := submitSupplementalTask(t, manager, &SupplementalTask{Mode: SuppAddOrUpdate, Link: link}); !res.OK {
		t.Fatalf("failed to add link: %+v", res)
	}

	const workers = 16
	readyEvents := make(chan *SupplementalLink, workers)
	var readyWG sync.WaitGroup
	readyWG.Add(1)
	go func() {
		defer readyWG.Done()
		for i := 0; i < workers; i++ {
			select {
			case ready := <-manager.ConnReadyChan:
				readyEvents <- ready
			case <-time.After(2 * time.Second):
				t.Errorf("timeout waiting for ready notification #%d", i)
				return
			}
		}
	}()

	var wg sync.WaitGroup
	wg.Add(workers)
	for i := 0; i < workers; i++ {
		go func(idx int) {
			defer wg.Done()
			connParent, connChild := net.Pipe()
			defer connChild.Close()
			res := submitSupplementalTask(t, manager, &SupplementalTask{
				Mode:     SuppAttachConn,
				LinkUUID: linkID,
				Conn:     connParent,
			})
			if !res.OK {
				t.Errorf("attach #%d failed", idx)
			}
		}(i)
	}
	wg.Wait()
	readyWG.Wait()
	close(readyEvents)

	received := 0
	for ready := range readyEvents {
		if ready == nil || ready.LinkUUID != linkID {
			t.Fatalf("unexpected ready notification: %#v", ready)
		}
		if ready.Conn != nil {
			_ = ready.Conn.Close()
		}
		received++
	}
	if received != workers {
		t.Fatalf("expected %d ready notifications, got %d", workers, received)
	}
}

func TestSupplementalManagerConcurrentMarkReadyAndRemove(t *testing.T) {
	manager, cancel := startSupplementalManager(t)
	defer cancel()

	const linkCount = 8
	for i := 0; i < linkCount; i++ {
		link := &SupplementalLink{
			LinkUUID:    linkID(i),
			PeerUUID:    linkID(i),
			State:       SuppStatePending,
			LastUpdated: time.Now(),
		}
		if res := submitSupplementalTask(t, manager, &SupplementalTask{Mode: SuppAddOrUpdate, Link: link}); !res.OK {
			t.Fatalf("failed to add link %d", i)
		}
	}

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		for i := 0; i < linkCount; i++ {
			res := submitSupplementalTask(t, manager, &SupplementalTask{Mode: SuppMarkReady, LinkUUID: linkID(i)})
			if !res.OK {
				// 链接可能已被另一协程移除，这里只需验证不会阻塞或 panic。
				continue
			}
		}
	}()

	go func() {
		defer wg.Done()
		for i := 0; i < linkCount; i++ {
			res := submitSupplementalTask(t, manager, &SupplementalTask{Mode: SuppDetach, LinkUUID: linkID(i)})
			if !res.OK {
				t.Errorf("detach failed for %s", linkID(i))
			}
		}
	}()

	wg.Wait()
}

func linkID(i int) string {
	return "supp-link-" + strconv.Itoa(i)
}
