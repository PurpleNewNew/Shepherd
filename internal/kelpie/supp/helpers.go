package supp

import "time"

// TestOnlyResetSupplementalController 重置内存中的补链控制器，便于测试隔离。
func TestOnlyResetSupplementalController() {
	suppCtrl = newSuppLinkController()
}

// TestOnlyRegisterSupplementalLink 注册补链记录供测试使用。
func TestOnlyRegisterSupplementalLink(linkUUID, listener, dialer, defaultIP string) {
	suppCtrl.register(linkUUID, listener, dialer, defaultIP)
}

// TestOnlyMarkSupplementalReady 将指定补链标记为就绪状态。
func TestOnlyMarkSupplementalReady(linkUUID string) {
	suppCtrl.markReady(linkUUID)
}

// TestOnlyRecordSupplementalHeartbeat 为补链端点记录一次心跳。
func TestOnlyRecordSupplementalHeartbeat(linkUUID, endpoint string, alive bool) {
	suppCtrl.recordHeartbeat(linkUUID, endpoint, time.Now(), alive)
}

// TestOnlySupplementalStatus 返回补链当前状态（若存在）。
func TestOnlySupplementalStatus(linkUUID string) (SuppLinkStatus, bool) {
	record := suppCtrl.get(linkUUID)
	if record == nil {
		return SuppLinkRetired, false
	}
	return record.Status, true
}
