package dtn

// Persistor 为 Kelpie 提供可选的 DTN bundle 持久化能力。
//
// 语义约定：
// - bundle 应在入队时持久化，并一直保留到最终成功 ACK 到达。
// - 这使得 Kelpie 重启前后仍能实现“至少一次”投递（inflight bundle 会被恢复）。
//
// 注意：Persistor 被有意定义在 dtn 包内，
// 以避免队列实现与某个具体数据库或后端耦合。
type Persistor interface {
	UpsertDTNBundle(bundle *Bundle) error
	DeleteDTNBundle(id string) error
	DeleteDTNBundles(ids []string) error
	LoadDTNBundles() ([]*Bundle, error)
}
