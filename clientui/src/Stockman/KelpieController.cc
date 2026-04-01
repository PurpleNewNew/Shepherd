#include <Stockman/KelpieController.hpp>

#include <Stockman/AppContext.hpp>
#include <Stockman/KelpieState.hpp>
#include <UserInterface/StockmanUI.hpp>

#include <QMetaObject>
#include <QTimer>
#include <map>
#include <QFile>
#include <QNetworkAccessManager>
#include <QNetworkRequest>
#include <QNetworkReply>
#include <QElapsedTimer>
#include <QTcpSocket>
#include <QSslSocket>
#include <QSslConfiguration>
#include <QSslCertificate>
#include <QtGlobal>
#include <QDataStream>
#include <QEventLoop>
#include <QUrl>
#include <QProcessEnvironment>
#include <QFileInfo>
#include <QSaveFile>
#include <QMimeDatabase>
#include <QCryptographicHash>
#include <QFutureWatcher>
#include <QtConcurrent/QtConcurrent>

#include <spdlog/spdlog.h>
#include "proto/kelpieui/v1/kelpieui.grpc.pb.h"
#include "dataplane/v1/dataplane.grpc.pb.h"
#include <utility>

namespace {
template <typename Service>
std::unique_ptr<typename Service::Stub> makeStub(const std::shared_ptr<grpc::Channel>& channel, QString& errorMessage) {
    if (!channel) {
        errorMessage = QStringLiteral("gRPC channel unavailable");
        return nullptr;
    }
    errorMessage.clear();
    return Service::NewStub(channel);
}

constexpr int kProgressThrottleMs = 100;
constexpr int kCancelPollMs = 100;
constexpr int kSnapshotDebounceMs = 600;
constexpr int kDefaultGrpcDeadlineMs = 5000;
constexpr qint64 kMaxUploadBytes = 8LL * 1024 * 1024; // 8MB 防止一次性读爆内存
constexpr qint64 kHashChunkBytes = 512LL * 1024;
constexpr int kDefaultWaitStartedMs = 5000;
constexpr int kSocketTimeoutMs = 30000;
constexpr size_t kMaxProxyQueueBytes = 8U * 1024U * 1024U;
constexpr quint8 kFrameOpen = 1;
constexpr quint8 kFrameData = 2;
constexpr quint8 kFrameClose = 3;

std::unique_ptr<grpc::ClientContext> makeContextFromOptions(const StockmanNamespace::Grpc::ConnectionOptions& options)
{
    auto ctx = std::make_unique<grpc::ClientContext>();
    const auto timeout = options.Timeout.count() > 0 ? options.Timeout : std::chrono::milliseconds(kDefaultGrpcDeadlineMs);
    ctx->set_deadline(std::chrono::system_clock::now() + timeout);
    // Keep auth metadata aligned with StockmanNamespace::Grpc::Client::attachAuth.
    if (!options.Token.empty()) {
        const std::string header = "Bearer " + options.Token;
        ctx->AddMetadata("authorization", header);
        ctx->AddMetadata("x-kelpie-token", options.Token);
    }
    if (!options.ClientName.empty()) {
        ctx->AddMetadata("x-client-user", options.ClientName);
    }
    return ctx;
}

template <typename Service, typename Request, typename Response, typename Invoke>
bool invokeUnaryRpc(const std::shared_ptr<grpc::Channel>& channel,
                    const StockmanNamespace::Grpc::ConnectionOptions& options,
                    QString& errorMessage,
                    const Request& req,
                    Response* resp,
                    Invoke&& invoke)
{
    auto stub = makeStub<Service>(channel, errorMessage);
    if (!stub) {
        return false;
    }
    auto ctx = makeContextFromOptions(options);
    Response localResp;
    grpc::Status status = std::forward<Invoke>(invoke)(*stub, ctx.get(), req, &localResp);
    if (!status.ok()) {
        errorMessage = QString::fromStdString(status.error_message());
        return false;
    }
    if (resp != nullptr) {
        *resp = std::move(localResp);
    }
    return true;
}

bool appendFileToRequest(const QString& path, bool uploadContent, kelpieui::v1::SubmitLootRequest& req)
{
    if ( path.isEmpty() )
    {
        return true;
    }
    QFileInfo info(path);
    if ( !info.exists() )
    {
        return true;
    }
    QMimeDatabase mimeDb;
    const auto mimeType = mimeDb.mimeTypeForFile(path, QMimeDatabase::MatchExtension).name();
    if ( !mimeType.isEmpty() )
    {
        req.set_mime(mimeType.toStdString());
    }
    if ( info.size() > 0 )
    {
        req.set_size(static_cast<uint64_t>(info.size()));
    }

    QFile file(path);
    if ( file.open(QIODevice::ReadOnly) )
    {
        QCryptographicHash hasher(QCryptographicHash::Sha256);
        while ( true )
        {
            QByteArray chunk = file.read(kHashChunkBytes);
            if ( chunk.isEmpty() )
            {
                break;
            }
            hasher.addData(chunk);
            if ( uploadContent && info.size() <= kMaxUploadBytes )
            {
                req.mutable_content()->append(chunk.constData(), chunk.size());
            }
        }
        req.set_hash(hasher.result().toHex().toStdString());
    }
    // 默认仅登记引用，不上传内容；内容流转交给 Flock/外部存储
    req.set_storage_ref(path.toStdString());
    return true;
}

void appendMetadata(const QMap<QString, QString>& metadata, kelpieui::v1::SubmitLootRequest& req)
{
    for (auto it = metadata.constBegin(); it != metadata.constEnd(); ++it)
    {
        (*req.mutable_metadata())[it.key().toStdString()] = it.value().toStdString();
    }
}

template <typename Request>
void appendTags(const QStringList& tags, Request& req)
{
    for (const auto& tag : tags)
    {
        if ( tag.trimmed().isEmpty() ) { continue; }
        req.add_tags(tag.trimmed().toStdString());
    }
}

bool validateLootRequest(const kelpieui::v1::SubmitLootRequest& req, QString& errorMessage)
{
    if ( req.content().empty() && req.storage_ref().empty() )
    {
        errorMessage = QStringLiteral("no content or storage_ref specified");
        return false;
    }
    return true;
}

QByteArray encodeOpenFrame(const QString& token,
                           const QString& direction,
                           const QString& path,
                           qint64 offset,
                           qint64 sizeHint,
                           const QString& hash)
{
    QByteArray payload;
    QDataStream ds(&payload, QIODevice::WriteOnly);
    ds.setByteOrder(QDataStream::BigEndian);
    auto writeString = [&](const QString& s) {
        QByteArray bytes = s.toUtf8();
        ds << static_cast<quint16>(bytes.size());
        ds.writeRawData(bytes.constData(), bytes.size());
    };
    writeString(token);
    writeString(direction);
    writeString(path);
    ds << static_cast<quint64>(offset);
    ds << static_cast<quint64>(sizeHint);
    writeString(hash);
    return payload;
}

struct Frame {
    quint8 type{0};
    quint32 streamId{0};
    QByteArray payload;
};

static bool writeFrame(QIODevice& dev, quint8 type, quint32 streamId, const QByteArray& payload, QString& err)
{
    QByteArray buf;
    QDataStream ds(&buf, QIODevice::WriteOnly);
    ds.setByteOrder(QDataStream::BigEndian);
    quint32 total = 6 + static_cast<quint32>(payload.size());
    ds << total;
    ds << type;
    ds << streamId;
    ds << static_cast<quint8>(0); // flags 保留
    if (!payload.isEmpty()) {
        ds.writeRawData(payload.constData(), payload.size());
    }
    if ( dev.write(buf) != buf.size() ) {
        err = QObject::tr("socket write failed");
        return false;
    }
    if (!dev.waitForBytesWritten(kSocketTimeoutMs)) {
        err = dev.errorString();
        return false;
    }
    return true;
}

static bool readFully(QIODevice& dev, QByteArray& out, qint64 n, QString& err)
{
    out.resize(static_cast<int>(n));
    qint64 read = 0;
    while (read < n) {
        if (!dev.bytesAvailable() && !dev.waitForReadyRead(kSocketTimeoutMs)) {
            err = dev.errorString();
            return false;
        }
        qint64 chunk = dev.read(out.data() + read, n - read);
        if (chunk <= 0) {
            err = QObject::tr("socket closed");
            return false;
        }
        read += chunk;
    }
    return true;
}

static bool readFrame(QIODevice& dev, Frame& frame, QString& err)
{
    QByteArray lenBuf;
    if (!readFully(dev, lenBuf, 4, err)) return false;
    QDataStream dsLen(lenBuf);
    dsLen.setByteOrder(QDataStream::BigEndian);
    quint32 total = 0;
    dsLen >> total;
    if (total < 6) {
        err = QObject::tr("invalid frame length");
        return false;
    }
    QByteArray hdr;
    if (!readFully(dev, hdr, 6, err)) return false;
    QDataStream dsHdr(hdr);
    dsHdr.setByteOrder(QDataStream::BigEndian);
    quint8 type = 0, flags = 0;
    quint32 sid = 0;
    dsHdr >> type;
    dsHdr >> sid;
    dsHdr >> flags; // unused
    int payloadLen = static_cast<int>(total) - 6;
    QByteArray payload;
    if (payloadLen > 0) {
        if (!readFully(dev, payload, payloadLen, err)) return false;
    }
    frame.type = type;
    frame.streamId = sid;
    frame.payload = std::move(payload);
    return true;
}
} // namespace

namespace StockmanNamespace
{
KelpieController::KelpieController(std::weak_ptr<StockmanNamespace::AppContext> ctx, QObject* parent)
    : QObject(parent)
    , context_(std::move(ctx))
{
    snapshotDebounce_ = new QTimer(this);
    snapshotDebounce_->setSingleShot(true);
    // 放宽防抖窗口，减少高频事件时的全量快照次数
    snapshotDebounce_->setInterval(kSnapshotDebounceMs);
    connect(snapshotDebounce_, &QTimer::timeout, this, [this]() {
        refreshSnapshotAsync();
    });
}

std::optional<KelpieController::RpcConfig> KelpieController::rpcConfig(QString& errorMessage) const
{
    std::lock_guard<std::mutex> guard(rpcMutex_);
    if ( !rpcChannel_ )
    {
        errorMessage = QStringLiteral("gRPC client is not connected");
        return std::nullopt;
    }
    RpcConfig cfg;
    cfg.channel = rpcChannel_;
    cfg.options = rpcOptions_;
    cfg.epoch = connectionEpoch_.load();
    errorMessage.clear();
    return cfg;
}

template <typename Service, typename Request, typename Response, typename Method>
bool KelpieController::invokeUnary(QString& errorMessage,
                                   const Request& req,
                                   Response* resp,
                                   Method method) const
{
    auto rpc = rpcConfig(errorMessage);
    if ( !rpc )
    {
        return false;
    }
    return invokeUnaryRpc<Service>(
        rpc->channel,
        rpc->options,
        errorMessage,
        req,
        resp,
        [method](auto& stub, auto* ctx, const auto& request, auto* response) {
            return (stub.*method)(ctx, request, response);
        });
}

    KelpieController::~KelpieController()
    {
        Disconnect();
    }

    bool KelpieController::Connect(const Grpc::ConnectionOptions& options, QString& errorMessage)
    {
        Disconnect();

        auto client = std::make_unique<Grpc::Client>();
        if ( ! client->Connect(options, errorMessage) )
        {
            spdlog::warn("[kelpie][connect] ok=false address={} error={}", options.Address, errorMessage.toStdString());
            return false;
        }
        spdlog::info("[kelpie][connect] ok=true address={} tls={} client={}",
                     options.Address,
                     options.UseTLS,
                     options.ClientName);

        kelpieui::v1::Snapshot snapshot;
        QString snapshotErr;
        if ( !client->FetchSnapshot(&snapshot, snapshotErr) )
        {
            errorMessage = snapshotErr;
            return false;
        }

        {
            std::lock_guard<std::mutex> guard(snapshotMutex_);
            latestSnapshot_ = snapshot;
        }
        if ( auto ctx = context_.lock(); ctx && ctx->kelpieState )
        {
            ctx->kelpieState->UpdateSnapshot(snapshot);
        }
        emit SnapshotUpdated();

        client_ = std::move(client);
        {
            std::lock_guard<std::mutex> guard(rpcMutex_);
            rpcChannel_ = client_ ? client_->Channel() : nullptr;
            rpcOptions_ = options;
            connectionEpoch_.fetch_add(1);
        }
        snapshotRefreshing_.store(false);

        watchActive_.store(true);
        client_->StartWatchEvents(
            [this](const kelpieui::v1::UiEvent& evt) {
                auto clone = std::make_shared<kelpieui::v1::UiEvent>(evt);
                QMetaObject::invokeMethod(
                    this,
                    [this, clone]() { handleUiEvent(*clone); },
                    Qt::QueuedConnection );
            },
            [this](const std::string& err) {
                QMetaObject::invokeMethod(
                    this,
                    [this, err]() {
                        emit LogMessageReceived(QString::fromStdString("event stream error: " + err));
                    },
                    Qt::QueuedConnection );
            } );

        return true;
    }

    void KelpieController::Disconnect()
    {
        watchActive_.store(false);
        {
            std::lock_guard<std::mutex> guard(rpcMutex_);
            rpcChannel_.reset();
            rpcOptions_ = {};
            connectionEpoch_.fetch_add(1);
        }
        snapshotRefreshing_.store(false);
        {
        std::lock_guard<std::mutex> guard(streamMutex_);
        for (auto streamIt = activeStreams_.begin(); streamIt != activeStreams_.end(); ++streamIt)
        {
            if ( auto handle = streamIt.value().lock() )
            {
                handle->Close();
            }
        }
        activeStreams_.clear();
        }
        if ( client_ )
        {
            client_->StopWatchEvents();
            client_->Disconnect();
            client_.reset();
        }
        if ( auto ctx = context_.lock() )
        {
            if ( ctx->kelpieState )
            {
                ctx->kelpieState->Clear();
            }
            // only clear cached runtime; do not recursively disconnect
            ctx->ClearRuntime(false);
        }
    }

    bool KelpieController::Connected() const
    {
        std::lock_guard<std::mutex> guard(rpcMutex_);
        return rpcChannel_ != nullptr;
    }

    uint64_t KelpieController::ConnectionEpoch() const
    {
        return connectionEpoch_.load();
    }

    void KelpieController::RequestSnapshotRefresh()
    {
        // Make it safe to call from any thread.
        QMetaObject::invokeMethod(
            this,
            [this]() {
                if ( snapshotDebounce_ )
                {
                    snapshotDebounce_->start();
                }
            },
            Qt::QueuedConnection );
    }

    kelpieui::v1::Snapshot KelpieController::LatestSnapshot() const
    {
        std::lock_guard<std::mutex> guard(snapshotMutex_);
        return latestSnapshot_;
    }

    bool KelpieController::ListNodes(const QString& targetUuid,
                                     const QString& networkId,
                                     std::vector<kelpieui::v1::NodeInfo>* nodes,
                                     QString& errorMessage)
    {
        kelpieui::v1::ListNodesRequest req;
        if ( !targetUuid.trimmed().isEmpty() )
        {
            req.set_target_uuid(targetUuid.trimmed().toStdString());
        }
        if ( !networkId.trimmed().isEmpty() )
        {
            req.set_network_id(networkId.trimmed().toStdString());
        }
        kelpieui::v1::ListNodesResponse resp;
        if ( !invokeUnary<kelpieui::v1::KelpieUIService>(
                 errorMessage, req, &resp, &kelpieui::v1::KelpieUIService::Stub::ListNodes) )
        {
            return false;
        }
        if ( nodes != nullptr )
        {
            nodes->assign(resp.nodes().begin(), resp.nodes().end());
        }
        return true;
    }

    bool KelpieController::GetTopology(const QString& targetUuid,
                                       const QString& networkId,
                                       kelpieui::v1::GetTopologyResponse* topology,
                                       QString& errorMessage)
    {
        kelpieui::v1::GetTopologyRequest req;
        if ( !targetUuid.trimmed().isEmpty() )
        {
            req.set_target_uuid(targetUuid.trimmed().toStdString());
        }
        if ( !networkId.trimmed().isEmpty() )
        {
            req.set_network_id(networkId.trimmed().toStdString());
        }
        kelpieui::v1::GetTopologyResponse resp;
        if ( !invokeUnary<kelpieui::v1::KelpieUIService>(
                 errorMessage, req, &resp, &kelpieui::v1::KelpieUIService::Stub::GetTopology) )
        {
            return false;
        }
        if ( topology != nullptr )
        {
            *topology = resp;
        }
        return true;
    }

    bool KelpieController::ListSessions(const QStringList& targetUuids,
                                        const std::vector<kelpieui::v1::SessionStatus>& statuses,
                                        bool includeInactive,
                                        std::vector<kelpieui::v1::SessionInfo>* sessions,
                                        QString& errorMessage)
    {
        kelpieui::v1::ListSessionsRequest req;
        for (const auto& uuid : targetUuids)
        {
            const auto trimmed = uuid.trimmed();
            if ( !trimmed.isEmpty() )
            {
                req.add_target_uuids(trimmed.toStdString());
            }
        }
        for (const auto& st : statuses)
        {
            if ( st != kelpieui::v1::SESSION_STATUS_UNSPECIFIED )
            {
                req.add_statuses(st);
            }
        }
        req.set_include_inactive(includeInactive);
        kelpieui::v1::ListSessionsResponse resp;
        if ( !invokeUnary<kelpieui::v1::KelpieUIService>(
                 errorMessage, req, &resp, &kelpieui::v1::KelpieUIService::Stub::ListSessions) )
        {
            return false;
        }
        if ( sessions != nullptr )
        {
            sessions->assign(resp.sessions().begin(), resp.sessions().end());
        }
        return true;
    }

    bool KelpieController::CloseStream(uint32_t streamId, const QString& reason, QString& errorMessage)
    {
        kelpieui::v1::CloseStreamRequest req;
        req.set_stream_id(streamId);
        if ( !reason.trimmed().isEmpty() )
        {
            req.set_reason(reason.trimmed().toStdString());
        }
        kelpieui::v1::CloseStreamResponse resp;
        return invokeUnary<kelpieui::v1::KelpieUIService>(
            errorMessage, req, &resp, &kelpieui::v1::KelpieUIService::Stub::CloseStream);
    }

    bool KelpieController::MarkSession(const QString& targetUuid,
                                       kelpieui::v1::SessionMarkAction action,
                                       const QString& reason,
                                       QString& errorMessage)
    {
        kelpieui::v1::MarkSessionRequest req;
        req.set_target_uuid(targetUuid.toStdString());
        req.set_action(action);
        if ( !reason.isEmpty() )
        {
            req.set_reason(reason.toStdString());
        }
        kelpieui::v1::MarkSessionResponse resp;
        return invokeUnary<kelpieui::v1::KelpieUIService>(
            errorMessage, req, &resp, &kelpieui::v1::KelpieUIService::Stub::MarkSession);
    }

    bool KelpieController::ListDtnBundles(const QString& targetUuid,
                                          int limit,
                                          std::vector<kelpieui::v1::DtnBundle>* bundles,
                                          QString& errorMessage)
    {
        kelpieui::v1::ListDtnBundlesRequest req;
        req.set_target_uuid(targetUuid.toStdString());
        if ( limit > 0 )
        {
            req.set_limit(limit);
        }
        kelpieui::v1::ListDtnBundlesResponse resp;
        if ( !invokeUnary<kelpieui::v1::KelpieUIService>(
                 errorMessage, req, &resp, &kelpieui::v1::KelpieUIService::Stub::ListDtnBundles) )
        {
            return false;
        }
        if ( bundles != nullptr )
        {
            bundles->assign(resp.bundles().begin(), resp.bundles().end());
        }
        return true;
    }

    bool KelpieController::GetDtnQueueStats(const QString& targetUuid,
                                            kelpieui::v1::DtnQueueStats* stats,
                                            QString& errorMessage)
    {
        kelpieui::v1::GetDtnQueueStatsRequest req;
        req.set_target_uuid(targetUuid.toStdString());
        kelpieui::v1::GetDtnQueueStatsResponse resp;
        if ( !invokeUnary<kelpieui::v1::KelpieUIService>(
                 errorMessage, req, &resp, &kelpieui::v1::KelpieUIService::Stub::GetDtnQueueStats) )
        {
            return false;
        }
        if ( stats != nullptr )
        {
            *stats = resp.stats();
        }
        return true;
    }

    bool KelpieController::EnqueueDtnPayload(const QString& targetUuid,
                                             const QString& payload,
                                             kelpieui::v1::DtnPriority priority,
                                             int64_t ttlSeconds,
                                             QString* bundleId,
                                             QString& errorMessage)
    {
        kelpieui::v1::EnqueueDtnPayloadRequest req;
        req.set_target_uuid(targetUuid.toStdString());
        req.set_payload(payload.toStdString());
        req.set_priority(priority);
        if ( ttlSeconds > 0 )
        {
            req.set_ttl_seconds(ttlSeconds);
        }
        kelpieui::v1::EnqueueDtnPayloadResponse resp;
        if ( !invokeUnary<kelpieui::v1::KelpieUIService>(
                 errorMessage, req, &resp, &kelpieui::v1::KelpieUIService::Stub::EnqueueDtnPayload) )
        {
            return false;
        }
        if ( bundleId != nullptr )
        {
            *bundleId = QString::fromStdString(resp.bundle_id());
        }
        return true;
    }

    bool KelpieController::GetSupplementalStatus(kelpieui::v1::SupplementalStatus* statusOut, QString& errorMessage)
    {
        kelpieui::v1::SupplementalEmpty req;
        kelpieui::v1::SupplementalStatus resp;
        if ( !invokeUnary<kelpieui::v1::SupplementalAdminService>(
                 errorMessage, req, &resp, &kelpieui::v1::SupplementalAdminService::Stub::GetSupplementalStatus) )
        {
            return false;
        }
        if ( statusOut != nullptr )
        {
            *statusOut = resp;
        }
        return true;
    }

    bool KelpieController::GetSupplementalMetrics(kelpieui::v1::SupplementalMetrics* metrics, QString& errorMessage)
    {
        kelpieui::v1::SupplementalEmpty req;
        kelpieui::v1::SupplementalMetrics resp;
        if ( !invokeUnary<kelpieui::v1::SupplementalAdminService>(
                 errorMessage, req, &resp, &kelpieui::v1::SupplementalAdminService::Stub::GetSupplementalMetrics) )
        {
            return false;
        }
        if ( metrics != nullptr )
        {
            *metrics = resp;
        }
        return true;
    }

    bool KelpieController::ListSupplementalQuality(int limit,
                                                   const std::vector<QString>& nodeUuids,
                                                   std::vector<kelpieui::v1::SupplementalQuality>* qualities,
                                                   QString& errorMessage)
    {
        kelpieui::v1::ListSupplementalQualityRequest req;
        req.set_limit(limit);
        for (const auto& uuid : nodeUuids)
        {
            req.add_node_uuids(uuid.toStdString());
        }
        kelpieui::v1::ListSupplementalQualityResponse resp;
        if ( !invokeUnary<kelpieui::v1::SupplementalAdminService>(
                 errorMessage,
                 req,
                 &resp,
                 &kelpieui::v1::SupplementalAdminService::Stub::ListSupplementalQuality) )
        {
            return false;
        }
        if ( qualities != nullptr )
        {
            qualities->assign(resp.qualities().begin(), resp.qualities().end());
        }
        return true;
    }

    bool KelpieController::ListRepairs(std::vector<kelpieui::v1::RepairStatus>* repairs, QString& errorMessage)
    {
        kelpieui::v1::ListRepairsRequest req;
        kelpieui::v1::ListRepairsResponse resp;
        if ( !invokeUnary<kelpieui::v1::SupplementalAdminService>(
                 errorMessage, req, &resp, &kelpieui::v1::SupplementalAdminService::Stub::ListRepairs) )
        {
            return false;
        }
        if ( repairs != nullptr )
        {
            repairs->assign(resp.repairs().begin(), resp.repairs().end());
        }
        return true;
    }

    bool KelpieController::RepairSession(const QString& targetUuid,
                                         bool force,
                                         const QString& reason,
                                         QString& errorMessage)
    {
        kelpieui::v1::RepairSessionRequest req;
        req.set_target_uuid(targetUuid.toStdString());
        req.set_force(force);
        if ( !reason.isEmpty() )
        {
            req.set_reason(reason.toStdString());
        }
        kelpieui::v1::RepairSessionResponse resp;
        if ( !invokeUnary<kelpieui::v1::KelpieUIService>(
                 errorMessage, req, &resp, &kelpieui::v1::KelpieUIService::Stub::RepairSession) )
        {
            return false;
        }
        if ( !resp.enqueued() )
        {
            errorMessage = QString::fromStdString(resp.message());
            return false;
        }
        return true;
    }

    bool KelpieController::ReconnectSession(const QString& targetUuid,
                                            const QString& reason,
                                            QString& errorMessage)
    {
        kelpieui::v1::ReconnectSessionRequest req;
        req.set_target_uuid(targetUuid.toStdString());
        if ( !reason.isEmpty() )
        {
            req.set_reason(reason.toStdString());
        }
        kelpieui::v1::ReconnectSessionResponse resp;
        if ( !invokeUnary<kelpieui::v1::KelpieUIService>(
                 errorMessage, req, &resp, &kelpieui::v1::KelpieUIService::Stub::ReconnectSession) )
        {
            return false;
        }
        if ( !resp.accepted() )
        {
            errorMessage = QString::fromStdString(resp.message());
            return false;
        }
        return true;
    }

    bool KelpieController::TerminateSession(const QString& targetUuid,
                                            const QString& reason,
                                            QString& errorMessage)
    {
        kelpieui::v1::TerminateSessionRequest req;
        req.set_target_uuid(targetUuid.toStdString());
        if ( !reason.isEmpty() )
        {
            req.set_reason(reason.toStdString());
        }
        kelpieui::v1::TerminateSessionResponse resp;
        return invokeUnary<kelpieui::v1::KelpieUIService>(
            errorMessage, req, &resp, &kelpieui::v1::KelpieUIService::Stub::TerminateSession);
    }

    bool KelpieController::UpdateNodeMemo(const QString& targetUuid, const QString& memo, QString& errorMessage)
    {
        kelpieui::v1::UpdateNodeMemoRequest req;
        req.set_target_uuid(targetUuid.toStdString());
        req.set_memo(memo.toStdString());
        kelpieui::v1::UpdateNodeMemoResponse resp;
        return invokeUnary<kelpieui::v1::KelpieUIService>(
            errorMessage, req, &resp, &kelpieui::v1::KelpieUIService::Stub::UpdateNodeMemo);
    }

    void KelpieController::refreshSnapshotAsync()
    {
        if ( snapshotRefreshing_.exchange(true) )
        {
            return;
        }
        QString rpcErr;
        auto rpc = rpcConfig(rpcErr);
        if ( !rpc )
        {
            snapshotRefreshing_.store(false);
            return;
        }
	        const uint64_t epoch = rpc->epoch;
	        auto channel = rpc->channel;
	        auto options = rpc->options;
	        QPointer<KelpieController> self(this);
	        auto future = QtConcurrent::run([self, channel = std::move(channel), options = std::move(options), epoch]() {
	            auto snapshotPtr = std::make_shared<kelpieui::v1::Snapshot>();
	            QString err;

            auto stub = kelpieui::v1::KelpieUIService::NewStub(channel);
            auto ctx = makeContextFromOptions(options);
            kelpieui::v1::SnapshotRequest req;
            kelpieui::v1::SnapshotResponse resp;
            grpc::Status status = stub->GetSnapshot(ctx.get(), req, &resp);
            const bool ok = status.ok();
            if ( !ok )
            {
                err = QString::fromStdString(status.error_message());
            }
            else
            {
                *snapshotPtr = std::move(*resp.mutable_snapshot());
            }

            if ( !self )
            {
                return;
            }
            QMetaObject::invokeMethod(
                self,
                [self, ok, snapshotPtr, err, epoch]() {
                    if ( !self )
                    {
                        return;
                    }
                    self->snapshotRefreshing_.store(false);
                    if ( epoch != self->connectionEpoch_.load() )
                    {
                        return;
                    }
                    if ( ok )
                    {
                        {
                            std::lock_guard<std::mutex> guard(self->snapshotMutex_);
                            self->latestSnapshot_ = *snapshotPtr;
                        }
                        if ( auto ctx = self->context_.lock(); ctx && ctx->kelpieState )
                        {
                            ctx->kelpieState->UpdateSnapshot(*snapshotPtr);
                        }
                        emit self->SnapshotUpdated();
                    }
                    else
                    {
                        emit self->LogMessageReceived(QStringLiteral("snapshot refresh failed: %1").arg(err));
                    }
                },
	                Qt::QueuedConnection );
	        });
	        Q_UNUSED(future);
	    }

    bool KelpieController::CreatePivotListener(const QString& targetUuid,
                                               const kelpieui::v1::PivotListenerSpec& spec,
                                               kelpieui::v1::PivotListener* created,
                                               QString& errorMessage)
    {
        kelpieui::v1::CreatePivotListenerRequest req;
        req.set_target_uuid(targetUuid.toStdString());
        *req.mutable_spec() = spec;
        kelpieui::v1::CreatePivotListenerResponse resp;
        if ( !invokeUnary<kelpieui::v1::PivotListenerAdminService>(
                 errorMessage,
                 req,
                 &resp,
                 &kelpieui::v1::PivotListenerAdminService::Stub::CreatePivotListener) )
        {
            return false;
        }
        if ( created != nullptr )
        {
            *created = resp.listener();
        }
        refreshSnapshotAsync();
        return true;
    }

    bool KelpieController::ListPivotListeners(std::vector<kelpieui::v1::PivotListener>* listeners,
                                              QString& errorMessage)
    {
        kelpieui::v1::ListPivotListenersRequest req;
        kelpieui::v1::ListPivotListenersResponse resp;
        if ( !invokeUnary<kelpieui::v1::PivotListenerAdminService>(
                 errorMessage,
                 req,
                 &resp,
                 &kelpieui::v1::PivotListenerAdminService::Stub::ListPivotListeners) )
        {
            return false;
        }
        if ( listeners != nullptr )
        {
            listeners->assign(resp.listeners().begin(), resp.listeners().end());
        }
        return true;
    }

    bool KelpieController::UpdatePivotListener(const QString& listenerId,
                                               const kelpieui::v1::PivotListenerSpec& spec,
                                               const QString& desiredStatus,
                                               kelpieui::v1::PivotListener* updated,
                                               QString& errorMessage)
    {
        kelpieui::v1::UpdatePivotListenerRequest req;
        req.set_listener_id(listenerId.toStdString());
        *req.mutable_spec() = spec;
        if ( !desiredStatus.isEmpty() )
        {
            req.set_desired_status(desiredStatus.toStdString());
        }
        kelpieui::v1::UpdatePivotListenerResponse resp;
        if ( !invokeUnary<kelpieui::v1::PivotListenerAdminService>(
                 errorMessage,
                 req,
                 &resp,
                 &kelpieui::v1::PivotListenerAdminService::Stub::UpdatePivotListener) )
        {
            return false;
        }
        if ( updated != nullptr )
        {
            *updated = resp.listener();
        }
        refreshSnapshotAsync();
        return true;
    }

    bool KelpieController::DeletePivotListener(const QString& listenerId, QString& errorMessage)
    {
        kelpieui::v1::DeletePivotListenerRequest req;
        req.set_listener_id(listenerId.toStdString());
        kelpieui::v1::DeletePivotListenerResponse resp;
        if ( !invokeUnary<kelpieui::v1::PivotListenerAdminService>(
                 errorMessage,
                 req,
                 &resp,
                 &kelpieui::v1::PivotListenerAdminService::Stub::DeletePivotListener) )
        {
            return false;
        }
        refreshSnapshotAsync();
        return true;
    }

    bool KelpieController::ListControllerListeners(std::vector<kelpieui::v1::ControllerListener>* listeners,
                                                   QString& errorMessage)
    {
        kelpieui::v1::ListControllerListenersRequest req;
        kelpieui::v1::ListControllerListenersResponse resp;
        if ( !invokeUnary<kelpieui::v1::ControllerListenerAdminService>(
                 errorMessage,
                 req,
                 &resp,
                 &kelpieui::v1::ControllerListenerAdminService::Stub::ListControllerListeners) )
        {
            return false;
        }
        if ( listeners != nullptr )
        {
            listeners->assign(resp.listeners().begin(), resp.listeners().end());
        }
        return true;
    }

    bool KelpieController::CreateControllerListener(const kelpieui::v1::ControllerListenerSpec& spec,
                                                    kelpieui::v1::ControllerListener* created,
                                                    QString& errorMessage)
    {
        kelpieui::v1::CreateControllerListenerRequest req;
        *req.mutable_spec() = spec;
        kelpieui::v1::CreateControllerListenerResponse resp;
        if ( !invokeUnary<kelpieui::v1::ControllerListenerAdminService>(
                 errorMessage,
                 req,
                 &resp,
                 &kelpieui::v1::ControllerListenerAdminService::Stub::CreateControllerListener) )
        {
            return false;
        }
        if ( created != nullptr )
        {
            *created = resp.listener();
        }
        refreshSnapshotAsync();
        return true;
    }

    bool KelpieController::UpdateControllerListener(const QString& listenerId,
                                                    const kelpieui::v1::ControllerListenerSpec* spec,
                                                    std::optional<kelpieui::v1::ControllerListenerStatus> desiredStatus,
                                                    kelpieui::v1::ControllerListener* updated,
                                                    QString& errorMessage)
    {
        kelpieui::v1::UpdateControllerListenerRequest req;
        req.set_listener_id(listenerId.toStdString());
        if ( spec != nullptr )
        {
            *req.mutable_spec() = *spec;
        }
        if ( desiredStatus.has_value() )
        {
            req.set_desired_status(desiredStatus.value());
        }
        kelpieui::v1::UpdateControllerListenerResponse resp;
        if ( !invokeUnary<kelpieui::v1::ControllerListenerAdminService>(
                 errorMessage,
                 req,
                 &resp,
                 &kelpieui::v1::ControllerListenerAdminService::Stub::UpdateControllerListener) )
        {
            return false;
        }
        if ( updated != nullptr )
        {
            *updated = resp.listener();
        }
        refreshSnapshotAsync();
        return true;
    }

    bool KelpieController::DeleteControllerListener(const QString& listenerId,
                                                    kelpieui::v1::ControllerListener* removed,
                                                    QString& errorMessage)
    {
        kelpieui::v1::DeleteControllerListenerRequest req;
        req.set_listener_id(listenerId.toStdString());
        kelpieui::v1::DeleteControllerListenerResponse resp;
        if ( !invokeUnary<kelpieui::v1::ControllerListenerAdminService>(
                 errorMessage,
                 req,
                 &resp,
                 &kelpieui::v1::ControllerListenerAdminService::Stub::DeleteControllerListener) )
        {
            return false;
        }
        if ( removed != nullptr )
        {
            *removed = resp.listener();
        }
        refreshSnapshotAsync();
        return true;
    }

    bool KelpieController::SendChatMessage(const QString& message, QString& errorMessage)
    {
        kelpieui::v1::SendChatMessageRequest req;
        req.set_message(message.toStdString());
        kelpieui::v1::SendChatMessageResponse resp;
        return invokeUnary<kelpieui::v1::KelpieUIService>(
            errorMessage, req, &resp, &kelpieui::v1::KelpieUIService::Stub::SendChatMessage);
    }

    bool KelpieController::ListChatMessages(int limit,
                                            const QString& beforeId,
                                            std::vector<kelpieui::v1::ChatMessage>* messages,
                                            QString& errorMessage)
    {
        kelpieui::v1::ListChatMessagesRequest req;
        if ( limit > 0 )
        {
            req.set_limit(limit);
        }
        if ( !beforeId.trimmed().isEmpty() )
        {
            req.set_before_id(beforeId.trimmed().toStdString());
        }
        kelpieui::v1::ListChatMessagesResponse resp;
        if ( !invokeUnary<kelpieui::v1::KelpieUIService>(
                 errorMessage, req, &resp, &kelpieui::v1::KelpieUIService::Stub::ListChatMessages) )
        {
            return false;
        }
        if ( messages != nullptr )
        {
            messages->assign(resp.messages().begin(), resp.messages().end());
        }
        return true;
    }

    bool KelpieController::ListProxies(std::vector<kelpieui::v1::ProxyInfo>* proxies, QString& errorMessage)
    {
        kelpieui::v1::ListProxiesRequest req;
        kelpieui::v1::ListProxiesResponse resp;
        if ( !invokeUnary<kelpieui::v1::ProxyAdminService>(
                 errorMessage, req, &resp, &kelpieui::v1::ProxyAdminService::Stub::ListProxies) )
        {
            return false;
        }
        if ( proxies != nullptr )
        {
            proxies->assign(resp.proxies().begin(), resp.proxies().end());
        }
        return true;
    }

    bool KelpieController::StartForwardProxy(const QString& targetUuid,
                                             const QString& localBind,
                                             const QString& remoteAddr,
                                             kelpieui::v1::StartForwardProxyResponse* response,
                                             QString& errorMessage)
    {
        kelpieui::v1::StartForwardProxyRequest req;
        req.set_target_uuid(targetUuid.toStdString());
        req.set_local_bind(localBind.toStdString());
        req.set_remote_addr(remoteAddr.toStdString());
        kelpieui::v1::StartForwardProxyResponse resp;
        if ( !invokeUnary<kelpieui::v1::ProxyAdminService>(
                 errorMessage, req, &resp, &kelpieui::v1::ProxyAdminService::Stub::StartForwardProxy) )
        {
            return false;
        }
        if ( response != nullptr )
        {
            *response = resp;
        }
        return true;
    }

    bool KelpieController::StopForwardProxy(const QString& targetUuid,
                                            const QString& proxyId,
                                            int* stopped,
                                            QString& errorMessage)
    {
        kelpieui::v1::StopForwardProxyRequest req;
        req.set_target_uuid(targetUuid.toStdString());
        req.set_proxy_id(proxyId.toStdString());
        kelpieui::v1::StopForwardProxyResponse resp;
        if ( !invokeUnary<kelpieui::v1::ProxyAdminService>(
                 errorMessage, req, &resp, &kelpieui::v1::ProxyAdminService::Stub::StopForwardProxy) )
        {
            return false;
        }
        if ( stopped != nullptr )
        {
            *stopped = resp.stopped();
        }
        return true;
    }

    bool KelpieController::StartBackwardProxy(const QString& targetUuid,
                                              const QString& remotePort,
                                              const QString& localPort,
                                              kelpieui::v1::StartBackwardProxyResponse* response,
                                              QString& errorMessage)
    {
        kelpieui::v1::StartBackwardProxyRequest req;
        req.set_target_uuid(targetUuid.toStdString());
        req.set_remote_port(remotePort.toStdString());
        req.set_local_port(localPort.toStdString());
        kelpieui::v1::StartBackwardProxyResponse resp;
        if ( !invokeUnary<kelpieui::v1::ProxyAdminService>(
                 errorMessage, req, &resp, &kelpieui::v1::ProxyAdminService::Stub::StartBackwardProxy) )
        {
            return false;
        }
        if ( response != nullptr )
        {
            *response = resp;
        }
        return true;
    }

    bool KelpieController::StopBackwardProxy(const QString& targetUuid,
                                             const QString& proxyId,
                                             int* stopped,
                                             QString& errorMessage)
    {
        kelpieui::v1::StopBackwardProxyRequest req;
        req.set_target_uuid(targetUuid.toStdString());
        req.set_proxy_id(proxyId.toStdString());
        kelpieui::v1::StopBackwardProxyResponse resp;
        if ( !invokeUnary<kelpieui::v1::ProxyAdminService>(
                 errorMessage, req, &resp, &kelpieui::v1::ProxyAdminService::Stub::StopBackwardProxy) )
        {
            return false;
        }
        if ( stopped != nullptr )
        {
            *stopped = resp.stopped();
        }
        return true;
    }

    bool KelpieController::ListSupplementalEvents(int limit,
                                                  std::vector<kelpieui::v1::SupplementalEvent>* events,
                                                  QString& errorMessage)
    {
        kelpieui::v1::ListSupplementalEventsRequest req;
        req.set_limit(limit);
        kelpieui::v1::ListSupplementalEventsResponse resp;
        if ( !invokeUnary<kelpieui::v1::SupplementalAdminService>(
                 errorMessage,
                 req,
                 &resp,
                 &kelpieui::v1::SupplementalAdminService::Stub::ListSupplementalEvents) )
        {
            return false;
        }
        if ( events != nullptr )
        {
            events->assign(resp.events().begin(), resp.events().end());
        }
        return true;
    }

    bool KelpieController::ListAuditLogs(const QString& username,
                                         const QString& method,
                                         const QDateTime& from,
                                         const QDateTime& toTime,
                                         int limit,
                                         std::vector<kelpieui::v1::AuditLogEntry>* entries,
                                         QString& errorMessage)
    {
        kelpieui::v1::ListAuditLogsRequest req;
        req.set_username(username.trimmed().toStdString());
        req.set_method(method.trimmed().toStdString());
        if ( from.isValid() )
        {
            req.set_from_time(from.toUTC().toString(Qt::ISODateWithMs).toStdString());
        }
        if ( toTime.isValid() )
        {
            req.set_to_time(toTime.toUTC().toString(Qt::ISODateWithMs).toStdString());
        }
        if ( limit > 0 )
        {
            req.set_limit(limit);
        }
        kelpieui::v1::ListAuditLogsResponse resp;
        if ( !invokeUnary<kelpieui::v1::KelpieUIService>(
                 errorMessage, req, &resp, &kelpieui::v1::KelpieUIService::Stub::ListAuditLogs) )
        {
            return false;
        }
        if ( entries != nullptr )
        {
            entries->assign(resp.entries().begin(), resp.entries().end());
        }
        return true;
    }

    bool KelpieController::ListLoot(const QString& targetUuid,
                                    kelpieui::v1::LootCategory category,
                                    int limit,
                                    const QString& beforeId,
                                    const QStringList& tags,
                                    std::vector<kelpieui::v1::LootItem>* items,
                                    QString& errorMessage)
    {
        kelpieui::v1::ListLootRequest req;
        if ( !targetUuid.trimmed().isEmpty() )
        {
            req.set_target_uuid(targetUuid.trimmed().toStdString());
        }
        if ( category != kelpieui::v1::LOOT_CATEGORY_UNSPECIFIED )
        {
            req.set_category(category);
        }
        if ( limit > 0 )
        {
            req.set_limit(limit);
        }
        if ( !beforeId.trimmed().isEmpty() )
        {
            req.set_before_id(beforeId.trimmed().toStdString());
        }
        for ( const auto& tag : tags )
        {
            if ( tag.trimmed().isEmpty() )
            {
                continue;
            }
            req.add_tags(tag.trimmed().toStdString());
        }
        kelpieui::v1::ListLootResponse resp;
        if ( !invokeUnary<kelpieui::v1::KelpieUIService>(
                 errorMessage, req, &resp, &kelpieui::v1::KelpieUIService::Stub::ListLoot) )
        {
            return false;
        }
        if ( items )
        {
            items->assign(resp.items().begin(), resp.items().end());
        }
        return true;
    }

    bool KelpieController::SubmitLoot(const QString& targetUuid,
                                      kelpieui::v1::LootCategory category,
                                      const QString& name,
                                      const QString& path,
                                      const QMap<QString, QString>& metadata,
                                      const QStringList& tags,
                                      const QString& hash,
                                      uint64_t size,
                                      bool uploadContent,
                                      kelpieui::v1::LootItem* item,
                                      QString& errorMessage)
    {
        kelpieui::v1::SubmitLootRequest req;
        req.set_target_uuid(targetUuid.trimmed().toStdString());
        req.set_category(category);
        req.set_name(name.trimmed().toStdString());
        const QString trimmedPath = path.trimmed();
        req.set_origin_path(trimmedPath.toStdString());
        appendFileToRequest(trimmedPath, uploadContent, req);
        if ( !hash.trimmed().isEmpty() )
        {
            req.set_hash(hash.trimmed().toStdString());
        }
        if ( size > 0 )
        {
            req.set_size(size);
        }
        appendMetadata(metadata, req);
        appendTags(tags, req);

        // 若既无内容也无引用，无法通过服务端校验。
        if ( !validateLootRequest(req, errorMessage) ) { return false; }

        kelpieui::v1::SubmitLootResponse resp;
        if ( !invokeUnary<kelpieui::v1::KelpieUIService>(
                 errorMessage, req, &resp, &kelpieui::v1::KelpieUIService::Stub::SubmitLoot) )
        {
            return false;
        }
        if ( item )
        {
            *item = resp.item();
        }
        return true;
    }

    bool KelpieController::GetMetrics(kelpieui::v1::GetMetricsResponse* metrics, QString& errorMessage)
    {
        kelpieui::v1::GetMetricsRequest req;
        req.set_include_reconnect(true);
        req.set_include_router(true);
        kelpieui::v1::GetMetricsResponse resp;
        if ( !invokeUnary<kelpieui::v1::KelpieUIService>(
                 errorMessage, req, &resp, &kelpieui::v1::KelpieUIService::Stub::GetMetrics) )
        {
            return false;
        }
        if ( metrics )
        {
            *metrics = resp;
        }
        return true;
    }

    bool KelpieController::CollectLootFile(const QString& targetUuid,
                                           const QString& remotePath,
                                           const QStringList& tags,
                                           kelpieui::v1::LootItem* item,
                                           QString& errorMessage)
    {
        kelpieui::v1::CollectLootFileRequest req;
        req.set_target_uuid(targetUuid.trimmed().toStdString());
        req.set_remote_path(remotePath.trimmed().toStdString());
        appendTags(tags, req);
        kelpieui::v1::CollectLootFileResponse resp;
        if ( !invokeUnary<kelpieui::v1::KelpieUIService>(
                 errorMessage, req, &resp, &kelpieui::v1::KelpieUIService::Stub::CollectLootFile) )
        {
            return false;
        }
        if ( item )
        {
            *item = resp.item();
        }
        return true;
    }

    bool KelpieController::ListRemoteFiles(const QString& targetUuid,
                                           const QString& path,
                                           kelpieui::v1::ListRemoteFilesResponse* listing,
                                           QString& errorMessage)
    {
        kelpieui::v1::ListRemoteFilesRequest req;
        req.set_target_uuid(targetUuid.trimmed().toStdString());
        req.set_path(path.trimmed().toStdString());
        kelpieui::v1::ListRemoteFilesResponse resp;
        if ( !invokeUnary<kelpieui::v1::KelpieUIService>(
                 errorMessage, req, &resp, &kelpieui::v1::KelpieUIService::Stub::ListRemoteFiles) )
        {
            return false;
        }
        if ( listing )
        {
            *listing = resp;
        }
        return true;
    }

    bool KelpieController::SyncLootToFile(const QString& lootId,
                                          const QString& localPath,
                                          kelpieui::v1::LootItem* item,
                                          qint64* bytesWritten,
                                          const std::function<void(qint64, qint64)>& onProgress,
                                          QString& errorMessage)
    {
        auto rpc = rpcConfig(errorMessage);
        if ( !rpc )
        {
            return false;
        }
        auto stub = makeStub<kelpieui::v1::KelpieUIService>(rpc->channel, errorMessage);
        if ( !stub )
        {
            return false;
        }
        auto ctx = makeContextFromOptions(rpc->options);
        kelpieui::v1::SyncLootRequest req;
        req.set_loot_id(lootId.trimmed().toStdString());
        auto reader = stub->SyncLoot(ctx.get(), req);
        if ( !reader )
        {
            errorMessage = QStringLiteral("failed to open loot sync");
            return false;
        }

        QSaveFile out(localPath);
        if ( !out.open(QIODevice::WriteOnly) )
        {
            errorMessage = QStringLiteral("Cannot open file for writing: %1").arg(localPath);
            return false;
        }

        kelpieui::v1::SyncLootChunk chunk;
        qint64 total = -1;
        qint64 written = 0;
        bool sawItem = false;
        while ( reader->Read(&chunk) )
        {
            if ( chunk.has_item() )
            {
                sawItem = true;
                if ( item )
                {
                    *item = chunk.item();
                }
                if ( chunk.item().size() > 0 )
                {
                    total = static_cast<qint64>(chunk.item().size());
                }
            }
            if ( !chunk.data().empty() )
            {
                const QByteArray data(chunk.data().data(), static_cast<int>(chunk.data().size()));
                const qint64 n = out.write(data);
                if ( n != data.size() )
                {
                    out.cancelWriting();
                    errorMessage = QStringLiteral("Loot sync write failed: %1").arg(localPath);
                    return false;
                }
                written += n;
                if ( onProgress )
                {
                    onProgress(written, total);
                }
            }
        }
        grpc::Status status = reader->Finish();
        if ( !status.ok() )
        {
            out.cancelWriting();
            errorMessage = QString::fromStdString(status.error_message());
            return false;
        }
        if ( !sawItem )
        {
            out.cancelWriting();
            errorMessage = QStringLiteral("loot sync returned no metadata");
            return false;
        }
        if ( total >= 0 && written != total )
        {
            out.cancelWriting();
            errorMessage = QStringLiteral("loot sync incomplete: wrote %1 / %2").arg(written).arg(total);
            return false;
        }
        if ( !out.commit() )
        {
            errorMessage = out.errorString();
            return false;
        }
        if ( bytesWritten )
        {
            *bytesWritten = written;
        }
        return true;
    }

    bool KelpieController::StreamStats(std::vector<kelpieui::v1::StreamStatInfo>* stats, QString& errorMessage)
    {
        kelpieui::v1::StreamStatsRequest req;
        kelpieui::v1::StreamStatsResponse resp;
        if ( !invokeUnary<kelpieui::v1::KelpieUIService>(
                 errorMessage, req, &resp, &kelpieui::v1::KelpieUIService::Stub::StreamStats) )
        {
            return false;
        }
        if ( stats != nullptr )
        {
            stats->assign(resp.stats().begin(), resp.stats().end());
        }
        return true;
    }

    bool KelpieController::StreamDiagnostics(std::vector<kelpieui::v1::StreamDiag>* diag, QString& errorMessage)
    {
        kelpieui::v1::StreamDiagnosticsRequest req;
        kelpieui::v1::StreamDiagnosticsResponse resp;
        if ( !invokeUnary<kelpieui::v1::KelpieUIService>(
                 errorMessage, req, &resp, &kelpieui::v1::KelpieUIService::Stub::StreamDiagnostics) )
        {
            return false;
        }
        if ( diag != nullptr )
        {
            diag->assign(resp.streams().begin(), resp.streams().end());
        }
        return true;
    }

    bool KelpieController::StreamPing(const QString& targetUuid, int count, int payloadSize, QString& errorMessage) // NOLINT(bugprone-easily-swappable-parameters)
    {
        kelpieui::v1::StreamPingRequest req;
        req.set_target_uuid(targetUuid.toStdString());
        if ( count > 0 )
        {
            req.set_count(count);
        }
        if ( payloadSize > 0 )
        {
            req.set_payload_size(payloadSize);
        }
        kelpieui::v1::StreamPingResponse resp;
        return invokeUnary<kelpieui::v1::KelpieUIService>(
            errorMessage, req, &resp, &kelpieui::v1::KelpieUIService::Stub::StreamPing);
    }

    bool KelpieController::NodeStatus(const QString& targetUuid, kelpieui::v1::NodeStatusResponse* respOut, QString& errorMessage)
    {
        kelpieui::v1::NodeStatusRequest req;
        req.set_target_uuid(targetUuid.toStdString());
        kelpieui::v1::NodeStatusResponse resp;
        if ( !invokeUnary<kelpieui::v1::KelpieUIService>(
                 errorMessage, req, &resp, &kelpieui::v1::KelpieUIService::Stub::NodeStatus) )
        {
            return false;
        }
        if ( respOut != nullptr )
        {
            *respOut = resp;
        }
        return true;
    }

    bool KelpieController::ShutdownNode(const QString& targetUuid, QString& errorMessage)
    {
        kelpieui::v1::ShutdownNodeRequest req;
        req.set_target_uuid(targetUuid.toStdString());
        kelpieui::v1::ShutdownNodeResponse resp;
        return invokeUnary<kelpieui::v1::KelpieUIService>(
            errorMessage, req, &resp, &kelpieui::v1::KelpieUIService::Stub::ShutdownNode);
    }

    bool KelpieController::ListNetworks(std::vector<kelpieui::v1::NetworkInfo>* networks,
                                        QString* activeNetworkId,
                                        QString& errorMessage)
    {
        kelpieui::v1::ListNetworksRequest req;
        kelpieui::v1::ListNetworksResponse resp;
        if ( !invokeUnary<kelpieui::v1::KelpieUIService>(
                 errorMessage, req, &resp, &kelpieui::v1::KelpieUIService::Stub::ListNetworks) )
        {
            return false;
        }
        if ( networks != nullptr )
        {
            networks->assign(resp.networks().begin(), resp.networks().end());
        }
        if ( activeNetworkId != nullptr )
        {
            *activeNetworkId = QString::fromStdString(resp.active_network_id());
        }
        return true;
    }

    bool KelpieController::UseNetwork(const QString& networkId, QString* activeNetworkId, QString& errorMessage)
    {
        kelpieui::v1::UseNetworkRequest req;
        req.set_network_id(networkId.toStdString());
        kelpieui::v1::UseNetworkResponse resp;
        if ( !invokeUnary<kelpieui::v1::KelpieUIService>(
                 errorMessage, req, &resp, &kelpieui::v1::KelpieUIService::Stub::UseNetwork) )
        {
            return false;
        }
        if ( activeNetworkId != nullptr )
        {
            *activeNetworkId = QString::fromStdString(resp.active_network_id());
        }
        return true;
    }

    bool KelpieController::ResetNetwork(QString* activeNetworkId, QString& errorMessage)
    {
        kelpieui::v1::ResetNetworkRequest req;
        kelpieui::v1::ResetNetworkResponse resp;
        if ( !invokeUnary<kelpieui::v1::KelpieUIService>(
                 errorMessage, req, &resp, &kelpieui::v1::KelpieUIService::Stub::ResetNetwork) )
        {
            return false;
        }
        if ( activeNetworkId != nullptr )
        {
            *activeNetworkId = QString::fromStdString(resp.active_network_id());
        }
        return true;
    }

    bool KelpieController::SetNodeNetwork(const QString& targetUuid, const QString& networkId, QString& errorMessage)
    {
        kelpieui::v1::SetNodeNetworkRequest req;
        req.set_target_uuid(targetUuid.toStdString());
        req.set_network_id(networkId.toStdString());
        kelpieui::v1::SetNodeNetworkResponse resp;
        return invokeUnary<kelpieui::v1::KelpieUIService>(
            errorMessage, req, &resp, &kelpieui::v1::KelpieUIService::Stub::SetNodeNetwork);
    }

    bool KelpieController::PruneOffline(int* removed, QString& errorMessage)
    {
        kelpieui::v1::PruneOfflineRequest req;
        kelpieui::v1::PruneOfflineResponse resp;
        if ( !invokeUnary<kelpieui::v1::KelpieUIService>(
                 errorMessage, req, &resp, &kelpieui::v1::KelpieUIService::Stub::PruneOffline) )
        {
            return false;
        }
        if ( removed != nullptr )
        {
            *removed = resp.removed();
        }
        return true;
    }

    bool KelpieController::ListSleepProfiles(std::vector<kelpieui::v1::SleepProfile>* profiles, QString& errorMessage)
    {
        kelpieui::v1::ListSleepProfilesRequest req;
        kelpieui::v1::ListSleepProfilesResponse resp;
        if ( !invokeUnary<kelpieui::v1::SleepAdminService>(
                 errorMessage, req, &resp, &kelpieui::v1::SleepAdminService::Stub::ListSleepProfiles) )
        {
            return false;
        }
        if ( profiles != nullptr )
        {
            profiles->assign(resp.profiles().begin(), resp.profiles().end());
        }
        return true;
    }

    bool KelpieController::UpdateSleep(const QString& targetUuid,
                                       std::optional<int> sleepSeconds,
                                       std::optional<int> workSeconds,
                                       std::optional<double> jitterPercent,
                                       QString& errorMessage)
    {
        kelpieui::v1::UpdateSleepRequest req;
        req.set_target_uuid(targetUuid.toStdString());
        if ( sleepSeconds.has_value() )
        {
            req.set_sleep_seconds(sleepSeconds.value());
        }
        if ( workSeconds.has_value() )
        {
            req.set_work_seconds(workSeconds.value());
        }
        if ( jitterPercent.has_value() )
        {
            req.set_jitter_percent(jitterPercent.value());
        }
        kelpieui::v1::UpdateSleepResponse resp;
        return invokeUnary<kelpieui::v1::SleepAdminService>(
            errorMessage, req, &resp, &kelpieui::v1::SleepAdminService::Stub::UpdateSleep);
    }

    bool KelpieController::StartDial(const QString& targetUuid,
                                     const QString& address,
                                     const QString& reason,
                                     kelpieui::v1::StartDialResponse* response,
                                     QString& errorMessage)
    {
        kelpieui::v1::StartDialRequest req;
        req.set_target_uuid(targetUuid.toStdString());
        req.set_address(address.toStdString());
        req.set_reason(reason.toStdString());
        kelpieui::v1::StartDialResponse resp;
        if ( !invokeUnary<kelpieui::v1::ConnectAdminService>(
                 errorMessage, req, &resp, &kelpieui::v1::ConnectAdminService::Stub::StartDial) )
        {
            return false;
        }
        if ( response != nullptr )
        {
            *response = resp;
        }
        return resp.accepted();
    }

    bool KelpieController::CancelDial(const QString& dialId, kelpieui::v1::CancelDialResponse* response, QString& errorMessage)
    {
        kelpieui::v1::CancelDialRequest req;
        req.set_dial_id(dialId.toStdString());
        kelpieui::v1::CancelDialResponse resp;
        if ( !invokeUnary<kelpieui::v1::ConnectAdminService>(
                 errorMessage, req, &resp, &kelpieui::v1::ConnectAdminService::Stub::CancelDial) )
        {
            return false;
        }
        if ( response != nullptr )
        {
            *response = resp;
        }
        return resp.canceled();
    }

    bool KelpieController::ListDials(std::vector<kelpieui::v1::DialStatus>* dials, QString& errorMessage)
    {
        kelpieui::v1::ListDialRequest req;
        kelpieui::v1::ListDialResponse resp;
        if ( !invokeUnary<kelpieui::v1::ConnectAdminService>(
                 errorMessage, req, &resp, &kelpieui::v1::ConnectAdminService::Stub::ListDials) )
        {
            return false;
        }
        if ( dials != nullptr )
        {
            dials->assign(resp.dials().begin(), resp.dials().end());
        }
        return true;
    }

    void KelpieController::handleUiEvent(const kelpieui::v1::UiEvent& event)
    {
        if ( auto ctx = context_.lock(); ctx && ctx->kelpieState )
        {
            ctx->kelpieState->EnqueueEvent(event);
        }
        if ( event.has_log_event() )
        {
            const auto& log = event.log_event();
            emit LogMessageReceived(QString::fromStdString(log.level() + ": " + log.message()));
            return;
        }

        const bool requiresSnapshot =
            event.has_node_event() ||
            event.has_stream_event() ||
            event.has_listener_event() ||
            event.has_session_event() ||
            event.has_proxy_event() ||
            event.has_sleep_event() ||
            event.has_supplemental_event() ||
            event.has_dial_event();

        if ( requiresSnapshot )
        {
            if (snapshotDebounce_ != nullptr)
            {
                snapshotDebounce_->start();
            }
            else
            {
                refreshSnapshotAsync();
            }
        }
    }

    ProxyStreamHandle::ProxyStreamHandle(const QString& sessionId,  // NOLINT(bugprone-easily-swappable-parameters)
                                         const QString& targetUuid,
                                         std::unique_ptr<kelpieui::v1::KelpieUIService::Stub> stub,
                                         std::unique_ptr<grpc::ClientReaderWriter<kelpieui::v1::StreamRequest, kelpieui::v1::StreamResponse>> stream,
                                         std::shared_ptr<grpc::ClientContext> context)
        : sessionId_(sessionId)
        , targetUuid_(targetUuid)
        , stub_(std::move(stub))
        , stream_(std::move(stream))
        , context_(std::move(context))
    {
        startReader();
    }

    ProxyStreamHandle::~ProxyStreamHandle()
    {
        Close();
        if ( readerThread_.joinable() )
        {
            readerThread_.join();
        }
    }

    void ProxyStreamHandle::startReader()
    {
        readerThread_ = std::thread([this]() {
            kelpieui::v1::StreamResponse resp;
            while ( !closed_.load(std::memory_order_acquire) && stream_ && stream_->Read(&resp) )
            {
                if ( !resp.data().empty() )
                {
                    QByteArray payload(resp.data().data(), static_cast<int>(resp.data().size()));
                    bool overflow = false;
                    {
                        std::lock_guard<std::mutex> lock(queueMutex_);
                        if ( queuedBytes_ + static_cast<size_t>(payload.size()) > kMaxProxyQueueBytes )
                        {
                            lastError_ = QStringLiteral("proxy stream receive queue overflow");
                            overflow = true;
                        }
                        else
                        {
                            queue_.push_back(payload);
                            queuedBytes_ += static_cast<size_t>(payload.size());
                        }
                    }
                    if ( overflow )
                    {
                        if ( context_ )
                        {
                            context_->TryCancel();
                        }
                        finish(QStringLiteral("proxy stream receive queue overflow"));
                        return;
                    }
                    queueCv_.notify_one();
                    emit DataReceived(payload);
                }
                if ( resp.has_control() )
                {
                    const auto& ctrl = resp.control();
                    if ( ctrl.kind() == kelpieui::v1::StreamControl_Kind_ERROR )
                    {
                        finish(QString::fromStdString(ctrl.error()));
                        return;
                    }
                    if ( ctrl.kind() == kelpieui::v1::StreamControl_Kind_CLOSE )
                    {
                        finish(QString());
                        return;
                    }
                }
            }
            grpc::Status status;
            if ( stream_ )
            {
                status = stream_->Finish();
            }
            if ( status.ok() )
            {
                finish(QString());
            }
            else
            {
                finish(QString::fromStdString(status.error_message()));
            }
        });
    }

    void ProxyStreamHandle::finish(const QString& error)
    {
        bool expected = false;
        if ( !closed_.compare_exchange_strong(expected, true, std::memory_order_acq_rel) )
        {
            return;
        }
        {
            std::lock_guard<std::mutex> lock(queueMutex_);
            lastError_ = error;
        }
        queueCv_.notify_all();
        emit Closed(error);
    }

    bool ProxyStreamHandle::SendData(const QByteArray& data)
    {
        if ( closed_.load(std::memory_order_acquire) || !stream_ )
        {
            return false;
        }
        kelpieui::v1::StreamRequest req;
        req.set_session_id(sessionId_.toStdString());
        req.set_target_uuid(targetUuid_.toStdString());
        req.set_data(std::string(data.constData(), data.size()));
        std::lock_guard<std::mutex> guard(writeMutex_);
        return stream_->Write(req);
    }

    bool ProxyStreamHandle::SendResize(uint16_t rows, uint16_t cols) // NOLINT(bugprone-easily-swappable-parameters)
    {
        if ( closed_.load(std::memory_order_acquire) || !stream_ )
        {
            return false;
        }
        kelpieui::v1::StreamRequest req;
        req.set_session_id(sessionId_.toStdString());
        req.set_target_uuid(targetUuid_.toStdString());
        auto* ctrl = req.mutable_control();
        ctrl->set_kind(kelpieui::v1::StreamControl_Kind_RESIZE);
        ctrl->set_rows(rows);
        ctrl->set_cols(cols);
        std::lock_guard<std::mutex> guard(writeMutex_);
        return stream_->Write(req);
    }

    void ProxyStreamHandle::Close(const QString& reason)
    {
        if ( closed_.exchange(true, std::memory_order_acq_rel) )
        {
            return;
        }
        if ( stream_ )
        {
            kelpieui::v1::StreamRequest req;
            req.set_session_id(sessionId_.toStdString());
            req.set_target_uuid(targetUuid_.toStdString());
            auto* ctrl = req.mutable_control();
            ctrl->set_kind(kelpieui::v1::StreamControl_Kind_CLOSE);
            {
                std::lock_guard<std::mutex> guard(writeMutex_);
                stream_->Write(req);
                stream_->WritesDone();
            }
        }
        if ( context_ )
        {
            context_->TryCancel();
        }
        finish(reason);
        if ( readerThread_.joinable() )
        {
            readerThread_.join();
        }
    }

    bool ProxyStreamHandle::WaitForData(QByteArray& out, QString& errorMessage)
    {
        std::unique_lock<std::mutex> lock(queueMutex_);
        queueCv_.wait(lock, [this]() {
            return !queue_.empty() || closed_.load(std::memory_order_acquire);
        });

        if ( !queue_.empty() )
        {
            out = queue_.front();
            queuedBytes_ -= static_cast<size_t>(queue_.front().size());
            queue_.pop_front();
            return true;
        }

        // closed and no data
        if ( !lastError_.isEmpty() )
        {
            errorMessage = lastError_;
            return false;
        }
        return false;
    }

    std::shared_ptr<ProxyStreamHandle> KelpieController::createProxyStream(const QString& targetUuid,
                                                                           const QString& sessionId,
                                                                           const QMap<QString, QString>& options,
                                                                           QString& errorMessage)
    {
        auto rpc = rpcConfig(errorMessage);
        if ( !rpc )
        {
            return nullptr;
        }
        auto stub = kelpieui::v1::KelpieUIService::NewStub(rpc->channel);
        auto ctx = makeContextFromOptions(rpc->options);
        ctx->set_wait_for_ready(true);
        auto context = std::shared_ptr<grpc::ClientContext>(ctx.release());
        auto stream = stub->ProxyStream(context.get());
        if ( !stream )
        {
            errorMessage = QStringLiteral("failed to open stream");
            return nullptr;
        }
        kelpieui::v1::StreamRequest initial;
        initial.set_session_id(sessionId.toStdString());
        initial.set_target_uuid(targetUuid.toStdString());
        for (auto it = options.constBegin(); it != options.constEnd(); ++it)
        {
            (*initial.mutable_options())[it.key().toStdString()] = it.value().toStdString();
        }
        if ( !stream->Write(initial) )
        {
            errorMessage = QStringLiteral("failed to initialize proxy stream");
            return nullptr;
        }

        auto handle = std::shared_ptr<ProxyStreamHandle>(new ProxyStreamHandle(sessionId, targetUuid, std::move(stub), std::move(stream), context));
        {
            std::lock_guard<std::mutex> guard(streamMutex_);
            activeStreams_.insert(sessionId, handle);
        }

        QObject::connect(handle.get(), &ProxyStreamHandle::Closed, this, [this, sessionId](const QString&) {
            removeStreamHandle(sessionId);
        });

        return handle;
    }

    std::shared_ptr<ProxyStreamHandle> KelpieController::OpenProxyStream(const QString& targetUuid, // NOLINT(bugprone-easily-swappable-parameters)
                                                                         const QString& kind,
                                                                         const QMap<QString, QString>& options,
                                                                         QString& errorMessage)
    {
        QMap<QString, QString> merged = options;
        if ( !merged.contains(QStringLiteral("kind")) )
        {
            merged.insert(QStringLiteral("kind"), kind);
        }
        return createProxyStream(targetUuid, generateSessionId(), merged, errorMessage);
    }

    std::shared_ptr<ProxyStreamHandle> KelpieController::OpenProxyStream(const kelpieui::v1::ProxyStreamHandle& handle,
                                                                         QString& errorMessage)
    {
        QMap<QString, QString> options;
        for (const auto& entry : handle.options())
        {
            options.insert(QString::fromStdString(entry.first), QString::fromStdString(entry.second));
        }
        if ( !options.contains(QStringLiteral("kind")) && !handle.kind().empty() )
        {
            options.insert(QStringLiteral("kind"), QString::fromStdString(handle.kind()));
        }
        return createProxyStream(QString::fromStdString(handle.target_uuid()),
                                 QString::fromStdString(handle.session_id()),
                                 options,
                                 errorMessage);
    }

    std::shared_ptr<ProxyStreamHandle> KelpieController::StartShellStream(const QString& targetUuid,
                                                                          kelpieui::v1::ShellMode mode,
                                                                          QString& errorMessage)
    {
        kelpieui::v1::StartShellRequest req;
        req.set_target_uuid(targetUuid.toStdString());
        req.set_mode(mode);
        kelpieui::v1::StartShellResponse resp;
        if ( !invokeUnary<kelpieui::v1::KelpieUIService>(
                 errorMessage, req, &resp, &kelpieui::v1::KelpieUIService::Stub::StartShell) )
        {
            return nullptr;
        }
        return OpenProxyStream(resp.handle(), errorMessage);
    }

bool KelpieController::UploadFileDataplane(const QString& targetUuid,
                                           const FileUploadSpec& spec,
                                           QString& errorMessage,
                                           const std::function<void(qint64, qint64)>& onProgress,
                                           std::stop_token stopToken)
{
    const auto start = std::chrono::steady_clock::now();
    auto rpc = rpcConfig(errorMessage);
    if ( !rpc )
    {
        return false;
    }

    QFile file(spec.localPath);
    if ( !file.exists() || !file.open(QIODevice::ReadOnly) )
    {
        errorMessage = QStringLiteral("Cannot open %1").arg(spec.localPath);
        return false;
    }

    auto ctx = makeContextFromOptions(rpc->options);
    dataplane::v1::PrepareTransferRequest req;
    req.set_target_uuid(targetUuid.toStdString());
    req.set_direction(dataplane::v1::DIRECTION_UPLOAD);
    req.set_path(spec.remotePath.toStdString());
    req.set_size_hint(file.size());
    req.set_offset(0);
    if ( spec.overwrite )
    {
        (*req.mutable_metadata())["overwrite"] = "true";
    }
    dataplane::v1::PrepareTransferResponse resp;
    auto dpStub = dataplane::v1::DataplaneAdmin::NewStub(rpc->channel);
    grpc::Status status = dpStub->PrepareTransfer(ctx.get(), req, &resp);
    if ( !status.ok() )
    {
        errorMessage = QString::fromStdString(status.error_message());
        return false;
    }
    spdlog::info("[dataplane][upload][prepare] target={} local={} remote={} size={} endpoint={}",
                 targetUuid.toStdString(),
                 spec.localPath.toStdString(),
                 spec.remotePath.toStdString(),
                 file.size(),
                 resp.endpoint());

    TransferEndpoint endpoint;
    if ( !parseDataplaneEndpoint(resp.endpoint(), endpoint.host, endpoint.port, endpoint.useTls) )
    {
        errorMessage = QStringLiteral("invalid dataplane endpoint");
        return false;
    }
    endpoint.token = QString::fromStdString(resp.token());

    auto isCancelled = [&]() {
        return stopToken.stop_possible() && stopToken.stop_requested();
    };
    if ( isCancelled() )
    {
        errorMessage = QStringLiteral("upload canceled");
        return false;
    }

    if ( !doUpload(endpoint, rpc->options, spec.remotePath, 0, file, errorMessage, onProgress, stopToken) )
    {
        return false;
    }

    dataplane::v1::CompleteTransferRequest completeReq;
    completeReq.set_token(resp.token());
    completeReq.set_target_uuid(targetUuid.toStdString());
    completeReq.set_bytes(file.size());
    completeReq.set_status("OK");
    dataplane::v1::CompleteTransferResponse completeResp;
    (void)dpStub->CompleteTransfer(makeContextFromOptions(rpc->options).get(), completeReq, &completeResp);
    auto transferMs = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - start).count();
    spdlog::info("[dataplane][upload][complete] target={} remote={} bytes={} elapsed_ms={}",
                 targetUuid.toStdString(),
                 spec.remotePath.toStdString(),
                 file.size(),
                 transferMs);
    return true;
}

    std::shared_ptr<ProxyStreamHandle> KelpieController::StartSocksProxy(const QString& targetUuid,
                                                                         kelpieui::v1::SocksProxyAuth auth,
                                                                         const QString& username,
                                                                         const QString& password,
                                                                         QString& errorMessage)
    {
        kelpieui::v1::StartSocksProxyRequest req;
        req.set_target_uuid(targetUuid.toStdString());
        req.set_auth(auth);
        if ( !username.isEmpty() )
        {
            req.set_username(username.toStdString());
        }
        if ( !password.isEmpty() )
        {
            req.set_password(password.toStdString());
        }
        kelpieui::v1::StartSocksProxyResponse resp;
        if ( !invokeUnary<kelpieui::v1::KelpieUIService>(
                 errorMessage, req, &resp, &kelpieui::v1::KelpieUIService::Stub::StartSocksProxy) )
        {
            return nullptr;
        }
        return OpenProxyStream(resp.handle(), errorMessage);
    }

    std::shared_ptr<ProxyStreamHandle> KelpieController::StartSshSession(const QString& targetUuid,
                                                                         const QString& serverAddr,
                                                                         const QString& username,
                                                                         const QString& password,
                                                                         QString& errorMessage)
    {
        kelpieui::v1::StartSshSessionRequest req;
        req.set_target_uuid(targetUuid.toStdString());
        req.set_server_addr(serverAddr.toStdString());
        req.set_auth_method(kelpieui::v1::SSH_SESSION_AUTH_METHOD_PASSWORD);
        if ( !username.isEmpty() )
        {
            req.set_username(username.toStdString());
        }
        if ( !password.isEmpty() )
        {
            req.set_password(password.toStdString());
        }
        kelpieui::v1::StartSshSessionResponse resp;
        if ( !invokeUnary<kelpieui::v1::KelpieUIService>(
                 errorMessage, req, &resp, &kelpieui::v1::KelpieUIService::Stub::StartSshSession) )
        {
            return nullptr;
        }
        return OpenProxyStream(resp.handle(), errorMessage);
    }

    bool KelpieController::StartSshTunnel(const QString& targetUuid,
                                          const QString& serverAddr,
                                          const QString& agentPort,
                                          kelpieui::v1::SshTunnelAuthMethod authMethod,
                                          const QString& username,
                                          const QString& password,
                                          const QByteArray& privateKey,
                                          QString& errorMessage)
    {
        kelpieui::v1::StartSshTunnelRequest req;
        req.set_target_uuid(targetUuid.toStdString());
        req.set_server_addr(serverAddr.toStdString());
        req.set_agent_port(agentPort.toStdString());
        req.set_auth_method(authMethod);
        if ( !username.isEmpty() )
        {
            req.set_username(username.toStdString());
        }
        if ( !password.isEmpty() )
        {
            req.set_password(password.toStdString());
        }
        if ( !privateKey.isEmpty() )
        {
            req.set_private_key(privateKey.toStdString());
        }
        kelpieui::v1::StartSshTunnelResponse resp;
        return invokeUnary<kelpieui::v1::KelpieUIService>(
            errorMessage, req, &resp, &kelpieui::v1::KelpieUIService::Stub::StartSshTunnel);
    }

    QString KelpieController::generateSessionId()
    {
        return QUuid::createUuid().toString(QUuid::WithoutBraces);
    }

    void KelpieController::removeStreamHandle(const QString& sessionId)
    {
        std::lock_guard<std::mutex> guard(streamMutex_);
        activeStreams_.remove(sessionId);
    }

    bool KelpieController::parseDataplaneEndpoint(const std::string& endpoint, QString& host, quint16& port, bool& useTls)
    {
        QString ep = QString::fromStdString(endpoint).trimmed();
        useTls = false;
        if ( ep.startsWith(QStringLiteral("tcps://"), Qt::CaseInsensitive) )
        {
            useTls = true;
            ep = ep.mid(7);
        }
        else if ( ep.startsWith(QStringLiteral("tcp://"), Qt::CaseInsensitive) )
        {
            ep = ep.mid(6);
        }
        int idx = ep.lastIndexOf(':');
        if ( idx <= 0 || idx == ep.size() - 1 )
        {
            return false;
        }
        host = ep.left(idx);
        bool ok = false;
        int p = ep.mid(idx + 1).toInt(&ok);
        if ( !ok || p <= 0 || p > 65535 )
        {
            return false;
        }
        port = static_cast<quint16>(p);
        return !host.trimmed().isEmpty();
    }

    bool KelpieController::doUpload(const TransferEndpoint& endpoint,
                                    const Grpc::ConnectionOptions& grpcOptions,
                                    const QString& remotePath,
                                    qint64 offset,
                                    QFile& file,
                                    QString& errorMessage,
                                    const std::function<void(qint64, qint64)>& onProgress,
                                    std::stop_token stopToken)
    {
        auto makeSocket = [&]() -> std::unique_ptr<QIODevice> {
            if (endpoint.useTls)
            {
                auto ssl = std::make_unique<QSslSocket>();
                ssl->setPeerVerifyMode(QSslSocket::VerifyPeer);
                QSslConfiguration conf = ssl->sslConfiguration();
                conf.setProtocol(QSsl::TlsV1_2OrLater);

                // Prefer explicit CA file; else, if the gRPC connection used TOFU (token-derived self-signed),
                // derive the same cert and pin it as the trust root for dataplane TLS.
                QList<QSslCertificate> cas;
                if ( !grpcOptions.CACertPath.empty() )
                {
                    QFile caFile(QString::fromStdString(grpcOptions.CACertPath));
                    if ( !caFile.open(QIODevice::ReadOnly) )
                    {
                        errorMessage = QStringLiteral("Unable to read TLS CA file: %1").arg(QString::fromStdString(grpcOptions.CACertPath));
                        return nullptr;
                    }
                    cas = QSslCertificate::fromData(caFile.readAll(), QSsl::Pem);
                }
                else if ( !grpcOptions.Token.empty() )
                {
                    const QByteArray der = StockmanNamespace::Grpc::Client::DeriveTofuRootCertDer(grpcOptions.Token, grpcOptions.ServerName);
                    cas = QSslCertificate::fromData(der, QSsl::Der);
                }
                if ( !cas.isEmpty() )
                {
                    conf.setCaCertificates(cas);
                }
                ssl->setSslConfiguration(conf);

                QString peerName;
                if ( !grpcOptions.ServerName.empty() )
                {
                    peerName = QString::fromStdString(grpcOptions.ServerName);
                }
                else if ( !grpcOptions.Token.empty() )
                {
                    peerName = QString::fromStdString(StockmanNamespace::Grpc::Client::ComputeTofuCommonName(grpcOptions.Token));
                }
                if ( peerName.isEmpty() )
                {
                    peerName = endpoint.host;
                }

                ssl->connectToHostEncrypted(endpoint.host, endpoint.port, peerName);
                if ( !ssl->waitForEncrypted(kDefaultWaitStartedMs) )
                {
                    errorMessage = ssl->errorString();
                    return nullptr;
                }
                return ssl;
            }
            auto tcp = std::make_unique<QTcpSocket>();
            tcp->connectToHost(endpoint.host, endpoint.port);
            if ( !tcp->waitForConnected(kDefaultWaitStartedMs) )
            {
                errorMessage = tcp->errorString();
                return nullptr;
            }
            return tcp;
        };

        auto socket = makeSocket();
        if ( !socket )
        {
            return false;
        }
        if ( !file.seek(offset) )
        {
            errorMessage = QStringLiteral("failed to seek file");
            return false;
        }

        QByteArray openPayload = encodeOpenFrame(endpoint.token,
                                                 QStringLiteral("upload"),
                                                 remotePath,
                                                 offset,
                                                 file.size(),
                                                 QString());
        if ( !writeFrame(*socket, kFrameOpen, 1, openPayload, errorMessage) )
        {
            return false;
        }

    QElapsedTimer throttle;
    throttle.start();
    qint64 chunkSize = 32 * 1024;
    const qint64 minChunk = 8 * 1024;
    const qint64 maxChunk = 128 * 1024;
    QByteArray buf(chunkSize, Qt::Uninitialized);
    while ( !file.atEnd() )
    {
        if ( stopToken.stop_possible() && stopToken.stop_requested() )
        {
            errorMessage = QStringLiteral("canceled");
            return false;
        }
        if ( buf.size() != chunkSize ) { buf.resize(static_cast<int>(chunkSize)); }
        const qint64 n = file.read(buf.data(), chunkSize);
        if ( n < 0 )
        {
            errorMessage = file.errorString();
            return false;
        }
            if ( n == 0 )
        {
            break;
        }
        QElapsedTimer rt;
        rt.start();
        if ( !writeFrame(*socket, kFrameData, 1, QByteArray(buf.constData(), static_cast<int>(n)), errorMessage) )
        {
            return false;
        }
        const qint64 elapsed = rt.elapsed();
        if ( elapsed > 200 && chunkSize > minChunk )
        {
            chunkSize = qMax(minChunk, chunkSize / 2);
        }
        else if ( elapsed < 50 && chunkSize < maxChunk )
        {
            chunkSize = qMin(maxChunk, chunkSize * 2);
        }
        if ( onProgress && throttle.elapsed() >= kProgressThrottleMs )
        {
            onProgress(file.pos(), file.size());
            throttle.restart();
        }
        }

        if ( !writeFrame(*socket, kFrameClose, 1, QByteArray(), errorMessage) )
        {
            return false;
        }

        Frame frame;
        if ( !readFrame(*socket, frame, errorMessage) )
        {
            return false;
        }
        if ( frame.type != kFrameClose )
        {
            errorMessage = QStringLiteral("unexpected frame type");
            return false;
        }
        if ( frame.payload.size() >= 2 )
        {
            QDataStream ds(frame.payload);
            ds.setByteOrder(QDataStream::BigEndian);
            quint16 code = 0;
            ds >> code;
            QString reason;
            if ( frame.payload.size() > 2 )
            {
                reason = QString::fromUtf8(frame.payload.constData() + 2, frame.payload.size() - 2);
            }
            if ( code != 0 )
            {
                errorMessage = reason.isEmpty() ? QStringLiteral("server rejected upload") : reason;
                return false;
            }
        }
        return true;
    }

    bool KelpieController::GetSessionDiagnostics(const QString& targetUuid,
                                                 bool includeProcesses,
                                                 bool includeMetrics,
                                                 kelpieui::v1::SessionDiagnosticsResponse* response,
                                                 QString& errorMessage)
    {
        kelpieui::v1::SessionDiagnosticsRequest req;
        req.set_target_uuid(targetUuid.toStdString());
        req.set_include_processes(includeProcesses);
        req.set_include_metrics(includeMetrics);
        kelpieui::v1::SessionDiagnosticsResponse resp;
        if ( !invokeUnary<kelpieui::v1::KelpieUIService>(
                 errorMessage, req, &resp, &kelpieui::v1::KelpieUIService::Stub::GetSessionDiagnostics) )
        {
            return false;
        }
        if ( response )
        {
            *response = resp;
        }
        return true;
    }
}
