#ifndef STOCKMAN_KELPIE_CONTROLLER_HPP
#define STOCKMAN_KELPIE_CONTROLLER_HPP

#include <QObject>
#include <QString>
#include <QByteArray>
#include <QMap>
#include <QHash>
#include <QUuid>
#include <QPointer>
#include <QTimer>
#include <QFile>
#include <functional>
#include <vector>
#include <optional>
#include <map>

#include <atomic>
#include <mutex>
#include <memory>
#include <thread>
#include <deque>
#include <condition_variable>

#include <grpcpp/client_context.h>
#include <grpcpp/support/sync_stream.h>

#include <Stockman/Grpc/Client.hpp>
#include <Stockman/AppContext.hpp>
#include "proto/kelpieui/v1/kelpieui.pb.h"

namespace StockmanNamespace
{
    struct AppContext;
    class KelpieController;

    // 独立的 ProxyStreamHandle QObject，避免嵌套 Q_OBJECT 带来的 moc 限制
    class ProxyStreamHandle : public QObject
    {
        Q_OBJECT
    public:
        using Ptr = std::shared_ptr<ProxyStreamHandle>;

        ~ProxyStreamHandle() override;

        [[nodiscard]] QString SessionId() const { return sessionId_; }
        [[nodiscard]] QString TargetUuid() const { return targetUuid_; }
        bool SendData(const QByteArray& data);
        bool SendResize(uint16_t rows, uint16_t cols);
        void Close(const QString& reason = {});

    signals:
        void DataReceived(const QByteArray& data);
        void Closed(const QString& error);

    private:
        friend class KelpieController;
        ProxyStreamHandle(const QString& sessionId,
                          const QString& targetUuid,
                          std::unique_ptr<kelpieui::v1::KelpieUIService::Stub> stub,
                          std::unique_ptr<grpc::ClientReaderWriter<kelpieui::v1::StreamRequest, kelpieui::v1::StreamResponse>> stream,
                          std::shared_ptr<grpc::ClientContext> context);

        void startReader();
        void finish(const QString& error);

        QString sessionId_;
        QString targetUuid_;
        std::unique_ptr<kelpieui::v1::KelpieUIService::Stub> stub_;
        std::unique_ptr<grpc::ClientReaderWriter<kelpieui::v1::StreamRequest, kelpieui::v1::StreamResponse>> stream_;
        std::shared_ptr<grpc::ClientContext> context_;
        std::atomic<bool> closed_{false};
        std::mutex writeMutex_;
        std::thread readerThread_;

        // Blocking read support for synchronous consumers
        std::mutex queueMutex_;
        std::condition_variable queueCv_;
        std::deque<QByteArray> queue_;
        size_t queuedBytes_{0};
        QString lastError_;

    public:
        // Blocks until data is available or the stream is closed.
        // Returns true on success; false only if a terminal error occurred (errorMessage set).
        bool WaitForData(QByteArray& out, QString& errorMessage);
    };

    class KelpieController : public QObject
    {
        Q_OBJECT

    public:
        struct FileUploadSpec {
            QString localPath;
            QString remotePath;
            bool overwrite{false};
        };

        explicit KelpieController(std::weak_ptr<StockmanNamespace::AppContext> ctx = {}, QObject* parent = nullptr);
        ~KelpieController() override;

        void SetContext(std::weak_ptr<StockmanNamespace::AppContext> ctx) { context_ = std::move(ctx); }

        bool Connect(const Grpc::ConnectionOptions& options, QString& errorMessage);
        void Disconnect();

        [[nodiscard]] bool Connected() const;
        [[nodiscard]] uint64_t ConnectionEpoch() const;
        void RequestSnapshotRefresh();

        [[nodiscard]] kelpieui::v1::Snapshot LatestSnapshot() const;

        bool ListNodes(const QString& targetUuid,
                       const QString& networkId,
                       std::vector<kelpieui::v1::NodeInfo>* nodes,
                       QString& errorMessage);
        bool GetTopology(const QString& targetUuid,
                         const QString& networkId,
                         kelpieui::v1::GetTopologyResponse* topology,
                         QString& errorMessage);
	        bool ListSessions(const QStringList& targetUuids,
	                          const std::vector<kelpieui::v1::SessionStatus>& statuses,
	                          bool includeInactive,
	                          std::vector<kelpieui::v1::SessionInfo>* sessions,
	                          QString& errorMessage);
	        bool CloseStream(uint32_t streamId, const QString& reason, QString& errorMessage);
	        bool MarkSession(const QString& targetUuid, kelpieui::v1::SessionMarkAction action, const QString& reason, QString& errorMessage);
	        bool RepairSession(const QString& targetUuid, bool force, const QString& reason, QString& errorMessage);
	        bool ReconnectSession(const QString& targetUuid, const QString& reason, QString& errorMessage);
        bool TerminateSession(const QString& targetUuid, const QString& reason, QString& errorMessage);
        bool UpdateNodeMemo(const QString& targetUuid, const QString& memo, QString& errorMessage);

        bool CreatePivotListener(const QString& targetUuid,
                                 const kelpieui::v1::PivotListenerSpec& spec,
                                 kelpieui::v1::PivotListener* created,
                                 QString& errorMessage);
        bool ListPivotListeners(std::vector<kelpieui::v1::PivotListener>* listeners,
                                QString& errorMessage);
        bool UpdatePivotListener(const QString& listenerId,
                                 const kelpieui::v1::PivotListenerSpec& spec,
                                 const QString& desiredStatus,
                                 kelpieui::v1::PivotListener* updated,
                                 QString& errorMessage);
        bool DeletePivotListener(const QString& listenerId, QString& errorMessage);
        bool ListControllerListeners(std::vector<kelpieui::v1::ControllerListener>* listeners,
                                     QString& errorMessage);
        bool CreateControllerListener(const kelpieui::v1::ControllerListenerSpec& spec,
                                      kelpieui::v1::ControllerListener* created,
                                      QString& errorMessage);
        bool UpdateControllerListener(const QString& listenerId,
                                      const kelpieui::v1::ControllerListenerSpec* spec,
                                      std::optional<kelpieui::v1::ControllerListenerStatus> desiredStatus,
                                      kelpieui::v1::ControllerListener* updated,
                                      QString& errorMessage);
        bool DeleteControllerListener(const QString& listenerId,
                                      kelpieui::v1::ControllerListener* removed,
                                      QString& errorMessage);

        bool SendChatMessage(const QString& message, QString& errorMessage);
        bool ListChatMessages(int limit,
                              const QString& beforeId,
                              std::vector<kelpieui::v1::ChatMessage>* messages,
                              QString& errorMessage);

        bool ListProxies(std::vector<kelpieui::v1::ProxyInfo>* proxies, QString& errorMessage);
        bool StartForwardProxy(const QString& targetUuid,
                               const QString& localBind,
                               const QString& remoteAddr,
                               kelpieui::v1::StartForwardProxyResponse* response,
                               QString& errorMessage);
        bool StopForwardProxy(const QString& targetUuid,
                              const QString& proxyId,
                              int* stopped,
                              QString& errorMessage);
        bool StartBackwardProxy(const QString& targetUuid,
                                const QString& remotePort,
                                const QString& localPort,
                                kelpieui::v1::StartBackwardProxyResponse* response,
                                QString& errorMessage);
        bool StopBackwardProxy(const QString& targetUuid,
                               const QString& proxyId,
                               int* stopped,
                               QString& errorMessage);
        bool ListSupplementalEvents(int limit,
                                    std::vector<kelpieui::v1::SupplementalEvent>* events,
                                    QString& errorMessage);
        bool GetSupplementalStatus(kelpieui::v1::SupplementalStatus* status, QString& errorMessage);
        bool GetSupplementalMetrics(kelpieui::v1::SupplementalMetrics* metrics, QString& errorMessage);
        bool ListSupplementalQuality(int limit,
                                     const std::vector<QString>& nodeUuids,
                                     std::vector<kelpieui::v1::SupplementalQuality>* qualities,
                                     QString& errorMessage);
        bool ListRepairs(std::vector<kelpieui::v1::RepairStatus>* repairs, QString& errorMessage);
        bool ListAuditLogs(const QString& username,
                           const QString& method,
                           const QDateTime& from,
                           const QDateTime& toTime,
                           int limit,
                           std::vector<kelpieui::v1::AuditLogEntry>* entries,
                            QString& errorMessage);
        bool ListLoot(const QString& targetUuid,
                      kelpieui::v1::LootCategory category,
                      int limit,
                      const QString& beforeId,
                      const QStringList& tags,
                      std::vector<kelpieui::v1::LootItem>* items,
                      QString& errorMessage);
        bool SubmitLoot(const QString& targetUuid,
                        kelpieui::v1::LootCategory category,
                        const QString& name,
                        const QString& path,
                        const QMap<QString, QString>& metadata,
                        const QStringList& tags,
                        const QString& hash,
                        uint64_t size,
                        bool uploadContent,
                        kelpieui::v1::LootItem* item,
                        QString& errorMessage);
        bool CollectLootFile(const QString& targetUuid,
                             const QString& remotePath,
                             const QStringList& tags,
                             kelpieui::v1::LootItem* item,
                             QString& errorMessage);
        bool ListRemoteFiles(const QString& targetUuid,
                             const QString& path,
                             kelpieui::v1::ListRemoteFilesResponse* listing,
                             QString& errorMessage);
        bool SyncLootToFile(const QString& lootId,
                            const QString& localPath,
                            kelpieui::v1::LootItem* item,
                            qint64* bytesWritten,
                            const std::function<void(qint64, qint64)>& onProgress,
                            QString& errorMessage);
        bool ListDtnBundles(const QString& targetUuid,
                            int limit,
                            std::vector<kelpieui::v1::DtnBundle>* bundles,
                            QString& errorMessage);
        bool GetDtnQueueStats(const QString& targetUuid,
                              kelpieui::v1::DtnQueueStats* stats,
                              QString& errorMessage);
        bool EnqueueDtnPayload(const QString& targetUuid,
                               const QString& payload,
                               kelpieui::v1::DtnPriority priority,
                               int64_t ttlSeconds,
                               QString* bundleId,
                               QString& errorMessage);

        bool GetMetrics(kelpieui::v1::GetMetricsResponse* metrics, QString& errorMessage);
        bool StreamStats(std::vector<kelpieui::v1::StreamStatInfo>* stats, QString& errorMessage);
        bool StreamDiagnostics(std::vector<kelpieui::v1::StreamDiag>* diag, QString& errorMessage);
        bool StreamPing(const QString& targetUuid, int count, int payloadSize, QString& errorMessage);
        bool NodeStatus(const QString& targetUuid, kelpieui::v1::NodeStatusResponse* resp, QString& errorMessage);
        bool ShutdownNode(const QString& targetUuid, QString& errorMessage);
        bool ListNetworks(std::vector<kelpieui::v1::NetworkInfo>* networks,
                          QString* activeNetworkId,
                          QString& errorMessage);
        bool UseNetwork(const QString& networkId, QString* activeNetworkId, QString& errorMessage);
        bool ResetNetwork(QString* activeNetworkId, QString& errorMessage);
        bool SetNodeNetwork(const QString& targetUuid, const QString& networkId, QString& errorMessage);
        bool PruneOffline(int* removed, QString& errorMessage);

        bool ListSleepProfiles(std::vector<kelpieui::v1::SleepProfile>* profiles, QString& errorMessage);
        bool UpdateSleep(const QString& targetUuid,
                         std::optional<int> sleepSeconds,
                         std::optional<int> workSeconds,
                         std::optional<double> jitterPercent,
                         QString& errorMessage);

        bool StartDial(const QString& targetUuid,
                       const QString& address,
                       const QString& reason,
                       kelpieui::v1::StartDialResponse* response,
                       QString& errorMessage);
        bool CancelDial(const QString& dialId, kelpieui::v1::CancelDialResponse* response, QString& errorMessage);
        bool ListDials(std::vector<kelpieui::v1::DialStatus>* dials, QString& errorMessage);

        std::shared_ptr<ProxyStreamHandle> StartSshSession(const QString& targetUuid,
                                                           const QString& serverAddr,
                                                           const QString& username,
                                                           const QString& password,
                                                           QString& errorMessage);
        bool StartSshTunnel(const QString& targetUuid,
                            const QString& serverAddr,
                            const QString& agentPort,
                            kelpieui::v1::SshTunnelAuthMethod authMethod,
                            const QString& username,
                            const QString& password,
                            const QByteArray& privateKey,
                            QString& errorMessage);

        std::shared_ptr<ProxyStreamHandle> OpenProxyStream(const QString& targetUuid,
                                                           const QString& kind,
                                                           const QMap<QString, QString>& options,
                                                           QString& errorMessage);
        std::shared_ptr<ProxyStreamHandle> OpenProxyStream(const kelpieui::v1::ProxyStreamHandle& handle,
                                                           QString& errorMessage);
        std::shared_ptr<ProxyStreamHandle> StartShellStream(const QString& targetUuid,
                                                            kelpieui::v1::ShellMode mode,
                                                            QString& errorMessage);
        [[nodiscard]] bool UploadFileDataplane(const QString& targetUuid,
                                               const FileUploadSpec& spec,
                                               QString& errorMessage,
                                               const std::function<void(qint64, qint64)>& onProgress = {},
                                               std::stop_token stopToken = {});
        std::shared_ptr<ProxyStreamHandle> StartSocksProxy(const QString& targetUuid,
                                                           kelpieui::v1::SocksProxyAuth auth,
                                                           const QString& username,
                                                           const QString& password,
                                                           QString& errorMessage);

        bool GetSessionDiagnostics(const QString& targetUuid,
                                   bool includeProcesses,
                                   bool includeMetrics,
                                   kelpieui::v1::SessionDiagnosticsResponse* response,
                                   QString& errorMessage);

    signals:
        void SnapshotUpdated();
        void LogMessageReceived(const QString& message);
        void CommandOutputReceived(const QString& message);

    private:
        struct RpcConfig {
            std::shared_ptr<grpc::Channel> channel;
            Grpc::ConnectionOptions options;
            uint64_t epoch{0};
        };

        std::weak_ptr<StockmanNamespace::AppContext> context_;
        std::unique_ptr<Grpc::Client> client_;
        mutable std::mutex rpcMutex_;
        std::shared_ptr<grpc::Channel> rpcChannel_;
        Grpc::ConnectionOptions rpcOptions_;
        std::atomic<uint64_t> connectionEpoch_{0};
        std::atomic<bool> snapshotRefreshing_{false};
        mutable std::mutex snapshotMutex_;
        kelpieui::v1::Snapshot latestSnapshot_;

        std::atomic<bool> watchActive_{false};

        mutable std::mutex streamMutex_;
        QHash<QString, std::weak_ptr<ProxyStreamHandle>> activeStreams_;

        std::optional<RpcConfig> rpcConfig(QString& errorMessage) const;
        template <typename Service, typename Request, typename Response, typename Method>
        bool invokeUnary(QString& errorMessage,
                         const Request& req,
                         Response* resp,
                         Method method) const;

        std::shared_ptr<ProxyStreamHandle> createProxyStream(const QString& targetUuid,
                                                             const QString& sessionId,
                                                             const QMap<QString, QString>& options,
                                                             QString& errorMessage);
        static bool parseDataplaneEndpoint(const std::string& endpoint, QString& host, quint16& port, bool& useTls);
        struct TransferEndpoint {
            QString host;
            quint16 port{0};
            bool useTls{false};
            QString token;
        };

        bool doUpload(const TransferEndpoint& endpoint,
                      const Grpc::ConnectionOptions& grpcOptions,
                      const QString& remotePath,
                      qint64 offset,
                      QFile& file,
                      QString& errorMessage,
                      const std::function<void(qint64, qint64)>& onProgress,
                      std::stop_token stopToken);
        static QString generateSessionId();
        void removeStreamHandle(const QString& sessionId);

        void handleUiEvent(const kelpieui::v1::UiEvent& event);
        void refreshSnapshotAsync();

        QTimer* snapshotDebounce_{nullptr};
    };
}

#endif // STOCKMAN_KELPIE_CONTROLLER_HPP
