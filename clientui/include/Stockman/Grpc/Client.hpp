#ifndef STOCKMAN_GRPC_CLIENT_HPP
#define STOCKMAN_GRPC_CLIENT_HPP

#include <atomic>
#include <chrono>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <thread>

#include <QObject>
#include <QString>
#include <QByteArray>

#include <grpcpp/channel.h>
#include <grpcpp/security/credentials.h>

#include "proto/kelpieui/v1/kelpieui.grpc.pb.h"

#include "Util/UiConstants.hpp"

namespace StockmanNamespace::Grpc
{
    struct ConnectionOptions
    {
        std::string Address;
        std::string Token;
        std::string ClientName;
        bool        UseTLS                = false;
        std::string CACertPath;
        std::string ServerName;
        std::chrono::milliseconds Timeout { std::chrono::milliseconds(UiConstants::kGrpcDefaultTimeoutMs) };
    };

    class Client : public QObject
    {
        Q_OBJECT

    public:
        using EventCallback = std::function<void(const kelpieui::v1::UiEvent&)>;
        using ErrorCallback = std::function<void(const std::string&)>;

        explicit Client(QObject* parent = nullptr);
        ~Client() override;

        bool Connect(const ConnectionOptions& options, QString& errorMessage);
        void Disconnect();

        bool FetchSnapshot(kelpieui::v1::Snapshot* snapshot, QString& errorMessage);

        void StartWatchEvents(EventCallback onEvent, ErrorCallback onError);
        void StopWatchEvents();

        bool Connected() const;
        std::shared_ptr<grpc::Channel> Channel() const;
        std::chrono::milliseconds Timeout() const { return options_.Timeout; }
        kelpieui::v1::KelpieUIService::Stub* Service() const { return stub_.get(); }
        std::string CurrentUsername() const;
        std::string CurrentRole() const;

        std::unique_ptr<grpc::ClientContext> CreateContext(QString* errorMessage = nullptr);

        // TOFU helpers (token-derived trust root). Kept here so gRPC and dataplane can share the same derivation.
        static std::string ComputeTofuCommonName(const std::string& token);
        static QByteArray DeriveTofuRootCertDer(const std::string& token, const std::string& serverName);
        static QByteArray DeriveTofuRootCertPem(const std::string& token, const std::string& serverName);

        // TOFU: 从 token 计算预期的服务器证书指纹
        static std::string ComputeExpectedFingerprint(const std::string& token, const std::string& serverName);
        static QString FormatFingerprint(const std::string& hex);

    signals:
        void ConnectionStateChanged(bool connected, QString message);

    private:
        static std::shared_ptr<grpc::ChannelCredentials> BuildCredentials(const ConnectionOptions& options, QString& errorMessage);
        bool attachAuth(grpc::ClientContext* ctx, QString& errorMessage) const;
        static std::chrono::system_clock::time_point parseTimestamp(const std::string& value);

        mutable std::mutex                                            clientMutex_;
        std::shared_ptr<grpc::Channel>                                channel_;
        std::unique_ptr<kelpieui::v1::KelpieUIService::Stub>          stub_;
        ConnectionOptions                                             options_;

        std::atomic<bool>                                             watching_ { false };
        std::unique_ptr<grpc::ClientContext>                          watchContext_;
        std::thread                                                   watchThread_;
        EventCallback                                                 eventCallback_;
        ErrorCallback                                                 errorCallback_;

        mutable std::mutex                                            tokenMutex_;
        std::string                                                   currentUsername_;
        std::string                                                   currentRole_;

        void RunWatchLoop();
    };
}

#endif // STOCKMAN_GRPC_CLIENT_HPP
