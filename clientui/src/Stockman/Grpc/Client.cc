#include <Stockman/Grpc/Client.hpp>

#include <Util/UiConstants.hpp>

#include <QFile>
#include <QFileInfo>
#include <QTextStream>
#include <QDateTime>

#include <grpcpp/create_channel.h>
#include <grpcpp/client_context.h>
#include <grpcpp/security/credentials.h>
#include <grpcpp/support/channel_arguments.h>

#include <openssl/evp.h>
#include <openssl/x509.h>
#include <openssl/x509v3.h>
#include <openssl/sha.h>
#include <openssl/pem.h>

#include <algorithm>
#include <cctype>
#include <cstring>

namespace StockmanNamespace::Grpc
{
    namespace {
        constexpr const char* kTlsIdentitySalt = "shepherd/tls-identity/v1";
        constexpr unsigned char kHexNibbleMask = 0x0F;  // 掩码：提取字节的低4位
        constexpr size_t kEd25519PrivateKeyBytes = 32;  // Ed25519私钥长度（字节）
        constexpr size_t kCertCNHexLength = 32;         // 证书CN字段十六进制截取长度
        constexpr long kCertNotBeforeOffsetSeconds = -300;  // 证书生效时间偏移（-5分钟）

        // SHA256 哈希
        std::vector<unsigned char> Sha256(const void* data, size_t len)
        {
            std::vector<unsigned char> hash(SHA256_DIGEST_LENGTH);
            SHA256(static_cast<const unsigned char*>(data), len, hash.data());
            return hash;
        }

        // 十六进制编码
        std::string ToHex(const std::vector<unsigned char>& data)
        {
            static const char hex[] = "0123456789abcdef";
            std::string result;
            result.reserve(data.size() * 2);
            for (auto byte : data) {
                result.push_back(hex[(byte >> 4) & kHexNibbleMask]);
                result.push_back(hex[byte & kHexNibbleMask]);
            }
            return result;
        }

        // 从 token 派生 Ed25519 密钥对并生成自签名证书，返回证书 DER（与 Go 端 tls.selfSignedCertificate 对齐，包含 serverName SAN）
        // NOLINTNEXTLINE(bugprone-easily-swappable-parameters) serverName 语义与 token 明确区分
        std::vector<unsigned char> DeriveServerCertDER(const std::string& token, const std::string& serverName)
        {
            // 1. seed = SHA256(token + salt)
            std::string seedInput = token + kTlsIdentitySalt;
            auto seed = Sha256(seedInput.data(), seedInput.size());

            // 2. 从 seed 生成 Ed25519 密钥对
            EVP_PKEY* pkey = EVP_PKEY_new_raw_private_key(EVP_PKEY_ED25519, nullptr, seed.data(), kEd25519PrivateKeyBytes);
            if (pkey == nullptr) {
                return {};
            }

            // 3. 生成自签名 X.509 证书
            X509* cert = X509_new();
            if (cert == nullptr) {
                EVP_PKEY_free(pkey);
                return {};
            }

            // 设置证书属性
            X509_set_version(cert, 2);  // X.509 v3
            ASN1_INTEGER_set(X509_get_serialNumber(cert), 1);
            X509_gmtime_adj(X509_get_notBefore(cert), kCertNotBeforeOffsetSeconds);
            X509_gmtime_adj(X509_get_notAfter(cert), 365L * 24 * 3600);  // +1 year
            X509_set_pubkey(cert, pkey);

            // 设置 Subject/Issuer
            X509_NAME* name = X509_get_subject_name(cert);
            std::string identityInput = std::string(kTlsIdentitySalt) + token;
            auto identityHash = Sha256(identityInput.data(), identityInput.size());
            std::string commonName = "shepherd-" + ToHex(identityHash).substr(0, kCertCNHexLength);
            X509_NAME_add_entry_by_txt(name, "CN", MBSTRING_ASC,
                reinterpret_cast<const unsigned char*>(commonName.c_str()), -1, -1, 0);
            X509_set_issuer_name(cert, name);

            // 添加 SAN: identity + serverName（若非空且不同）
            GENERAL_NAMES* sanNames = sk_GENERAL_NAME_new_null();
            auto addDNS = [sanNames](const std::string& dns) -> bool {
                GENERAL_NAME* gen = GENERAL_NAME_new();
                if (gen == nullptr) { return false; }
                ASN1_IA5STRING* ia5 = ASN1_IA5STRING_new();
                if (ia5 == nullptr) {
                    GENERAL_NAME_free(gen);
                    return false;
                }
                if (ASN1_STRING_set(ia5, dns.c_str(), static_cast<int>(dns.size())) != 1) {
                    ASN1_IA5STRING_free(ia5);
                    GENERAL_NAME_free(gen);
                    return false;
                }
                GENERAL_NAME_set0_value(gen, GEN_DNS, ia5);
                sk_GENERAL_NAME_push(sanNames, gen);
                return true;
            };
            const auto toLower = [](std::string s) {
                std::transform(s.begin(), s.end(), s.begin(), [](unsigned char c) { return static_cast<char>(std::tolower(c)); });
                return s;
            };
            (void)addDNS(commonName);
            if (!serverName.empty() && toLower(serverName) != toLower(commonName)) {
                (void)addDNS(serverName);
            }
            if (sanNames != nullptr) {
                X509_add1_ext_i2d(cert, NID_subject_alt_name, sanNames, 0, X509V3_ADD_DEFAULT);
                sk_GENERAL_NAME_pop_free(sanNames, GENERAL_NAME_free);
            }

            // 签名证书
            X509_sign(cert, pkey, nullptr);  // Ed25519 不需要 digest

            // 4. 导出 DER 格式
            unsigned char* der = nullptr;
            int derLen = i2d_X509(cert, &der);
            std::vector<unsigned char> result;
            if (derLen > 0 && (der != nullptr)) {
                result.assign(der, der + derLen);
                OPENSSL_free(der);
            }

            X509_free(cert);
            EVP_PKEY_free(pkey);
            return result;
        }
    } // anonymous namespace

    std::string Client::ComputeTofuCommonName(const std::string& token)
    {
        if (token.empty()) {
            return {};
        }
        std::string identityInput = std::string(kTlsIdentitySalt) + token;
        auto identityHash = Sha256(identityInput.data(), identityInput.size());
        return "shepherd-" + ToHex(identityHash).substr(0, kCertCNHexLength);
    }

    QByteArray Client::DeriveTofuRootCertDer(const std::string& token, const std::string& serverName)
    {
        auto der = DeriveServerCertDER(token, serverName);
        if (der.empty()) {
            return {};
        }
        return QByteArray(reinterpret_cast<const char*>(der.data()), static_cast<int>(der.size()));
    }

    QByteArray Client::DeriveTofuRootCertPem(const std::string& token, const std::string& serverName)
    {
        const QByteArray der = DeriveTofuRootCertDer(token, serverName);
        if (der.isEmpty()) {
            return {};
        }
        const QByteArray b64 = der.toBase64();
        QByteArray pem;
        pem += "-----BEGIN CERTIFICATE-----\n";
        for (int i = 0; i < b64.size(); i += 64) {
            pem += b64.mid(i, 64);
            pem += '\n';
        }
        pem += "-----END CERTIFICATE-----\n";
        return pem;
    }

    // 从 token 计算预期的服务器证书指纹（与 Go 端算法一致）
    std::string Client::ComputeExpectedFingerprint(const std::string& token, const std::string& serverName)
    {
        auto certDER = DeriveServerCertDER(token, serverName);
        if (certDER.empty()) {
            return {};
        }
        auto hash = Sha256(certDER.data(), certDER.size());
        return ToHex(hash);
    }

    // 格式化指纹显示 (AA:BB:CC:DD...)
    QString Client::FormatFingerprint(const std::string& hex)
    {
        if (hex.empty()) { return {};
}
        QString result;
        for (size_t i = 0; i < hex.size(); i += 2) {
            if (i > 0) { result += ':';
}
            result += QString::fromStdString(hex.substr(i, 2)).toUpper();
        }
        return result;
    }

    Client::Client(QObject* parent)
        : QObject(parent)
    {}

    Client::~Client()
    {
        StopWatchEvents();
        Disconnect();
    }

bool Client::Connect(const ConnectionOptions& options, QString& errorMessage)
{
        std::unique_lock<std::mutex> guard(clientMutex_);

        ConnectionOptions normalized = options;
        normalized.Token = QString::fromStdString(options.Token).trimmed().toStdString();

        QString validationError;
        if (normalized.Address.empty())
        {
            validationError = QStringLiteral("Address is required");
        }
        else if (normalized.Token.empty())
        {
            validationError = QStringLiteral("Token is required");
        }

        if (!validationError.isEmpty())
        {
            errorMessage = validationError;
            return false;
        }

        auto creds = BuildCredentials(normalized, errorMessage);
        if (!creds)
        {
            return false;
        }

        grpc::ChannelArguments args;
        args.SetInt(GRPC_ARG_KEEPALIVE_TIME_MS, UiConstants::kGrpcKeepaliveTimeMs);
        args.SetInt(GRPC_ARG_KEEPALIVE_TIMEOUT_MS, UiConstants::kGrpcKeepaliveTimeoutMs);
        if (!normalized.ServerName.empty())
        {
            // 兼容自定义 SNI；当提供 CA 时依然进行证书校验
            args.SetString(GRPC_SSL_TARGET_NAME_OVERRIDE_ARG, normalized.ServerName);
        }

        channel_ = grpc::CreateCustomChannel(normalized.Address, creds, args);
        stub_ = kelpieui::v1::KelpieUIService::NewStub(channel_);
        options_ = normalized;

        guard.unlock();

        kelpieui::v1::Snapshot snapshot;
        QString snapshotErr;
        if ( !FetchSnapshot(&snapshot, snapshotErr) )
        {
            errorMessage = snapshotErr;
            Disconnect();
            return false;
        }

        emit ConnectionStateChanged(true, QStringLiteral("Connected to %1").arg(QString::fromStdString(normalized.Address)));
        return true;
}

    void Client::Disconnect()
    {
        std::lock_guard<std::mutex> guard(clientMutex_);
        channel_.reset();
        stub_.reset();
        {
            std::lock_guard<std::mutex> tokenGuard(tokenMutex_);
            currentUsername_.clear();
            currentRole_.clear();
        }
        emit ConnectionStateChanged(false, QStringLiteral("Disconnected"));
    }

bool Client::Connected() const
{
    return static_cast<bool>(stub_);
}

std::shared_ptr<grpc::Channel> Client::Channel() const
{
    std::lock_guard<std::mutex> guard(clientMutex_);
    return channel_;
}

    bool Client::FetchSnapshot(kelpieui::v1::Snapshot* snapshot, QString& errorMessage)
    {
        std::lock_guard<std::mutex> guard(clientMutex_);
        if (!stub_)
        {
            errorMessage = QStringLiteral("gRPC client is not connected");
            return false;
        }

        auto ctx = CreateContext(&errorMessage);
        if ( !ctx )
        {
            return false;
        }
        kelpieui::v1::SnapshotRequest req;
        kelpieui::v1::SnapshotResponse resp;
        grpc::Status status = stub_->GetSnapshot(ctx.get(), req, &resp);
        if (!status.ok())
        {
            errorMessage = QString::fromStdString(status.error_message());
            return false;
        }
        if (snapshot != nullptr)
        {
            *snapshot = std::move(*resp.mutable_snapshot());
        }
        return true;
    }

    void Client::StartWatchEvents(EventCallback onEvent, ErrorCallback onError)
    {
        StopWatchEvents();
        {
            std::lock_guard<std::mutex> guard(clientMutex_);
            if (!stub_ || !channel_)
            {
                if (onError)
                {
                    onError("client disconnected");
                }
                return;
            }
        }

        eventCallback_ = std::move(onEvent);
        errorCallback_ = std::move(onError);
        watching_.store(true);
        watchThread_ = std::thread([this]() {
            RunWatchLoop();
        });
    }

    void Client::StopWatchEvents()
    {
        watching_.store(false);
        {
            std::lock_guard<std::mutex> guard(clientMutex_);
            if (watchContext_)
            {
                watchContext_->TryCancel();
            }
        }
        if (watchThread_.joinable())
        {
            watchThread_.join();
        }
        std::lock_guard<std::mutex> guard(clientMutex_);
        watchContext_.reset();
    }

    void Client::RunWatchLoop()
    {
        constexpr auto kWatchRetryDelay = std::chrono::seconds(1);
        while (watching_.load())
        {
            std::shared_ptr<grpc::Channel> channelCopy;
            {
                std::lock_guard<std::mutex> guard(clientMutex_);
                if (!stub_ || !channel_)
                {
                    if (errorCallback_)
                    {
                        errorCallback_("client disconnected");
                    }
                    return;
                }
                channelCopy = channel_;
                watchContext_ = std::make_unique<grpc::ClientContext>();
            }

            auto stubCopy = kelpieui::v1::KelpieUIService::NewStub(channelCopy);
            kelpieui::v1::WatchEventsRequest req;

            grpc::ClientContext* watchCtx = nullptr;
            {
                std::lock_guard<std::mutex> guard(clientMutex_);
                if (!watchContext_)
                {
                    return;
                }
                watchCtx = watchContext_.get();
            }

            QString authError;
            if (!attachAuth(watchCtx, authError))
            {
                if (errorCallback_)
                {
                    errorCallback_(authError.toStdString());
                }
                watching_.store(false);
                std::lock_guard<std::mutex> guard(clientMutex_);
                watchContext_.reset();
                return;
            }

            auto reader = stubCopy->WatchEvents(watchCtx, req);
            kelpieui::v1::UiEvent event;
            while (watching_.load() && reader->Read(&event))
            {
                if (eventCallback_)
                {
                    eventCallback_(event);
                }
            }
            grpc::Status status = reader->Finish();
            {
                std::lock_guard<std::mutex> guard(clientMutex_);
                watchContext_.reset();
            }
            if (!watching_.load())
            {
                return;
            }
            if (!status.ok() && errorCallback_)
            {
                errorCallback_(status.error_message());
            }
            std::this_thread::sleep_for(kWatchRetryDelay);
        }
    }

	    std::shared_ptr<grpc::ChannelCredentials> Client::BuildCredentials(const ConnectionOptions& options, QString& errorMessage)
	    {
	        std::shared_ptr<grpc::ChannelCredentials> baseCreds;
	        if (!options.UseTLS)
	        {
	            baseCreds = grpc::InsecureChannelCredentials();
	        }
	        else
	        {
	            grpc::SslCredentialsOptions sslOpts;
	            if (!options.CACertPath.empty())
            {
                // 显式 CA 文件模式
                QFile file(QString::fromStdString(options.CACertPath));
                if (!file.exists() || !file.open(QIODevice::ReadOnly))
                {
                    errorMessage = QStringLiteral("Unable to read TLS CA file: %1").arg(QString::fromStdString(options.CACertPath));
                    return nullptr;
                }
                sslOpts.pem_root_certs = file.readAll().toStdString();
            }
            else if (!options.Token.empty())
            {
                // TOFU 模式：从 token 派生证书作为信任根
                auto certDER = DeriveServerCertDER(options.Token, options.ServerName);
                if (certDER.empty())
                {
                    errorMessage = QStringLiteral("Failed to derive TLS certificate from token.");
                    return nullptr;
                }
                // 将 DER 转换为 PEM
                std::string pem = "-----BEGIN CERTIFICATE-----\n";
                // Base64 编码
                BIO* bio = BIO_new(BIO_s_mem());
                BIO* b64 = BIO_new(BIO_f_base64());
                BIO_push(b64, bio);
                BIO_write(b64, certDER.data(), static_cast<int>(certDER.size()));
                BIO_flush(b64);
                BUF_MEM* bufferPtr;
                BIO_get_mem_ptr(bio, &bufferPtr);
                pem.append(bufferPtr->data, bufferPtr->length);
                BIO_free_all(b64);
                pem += "-----END CERTIFICATE-----\n";
                sslOpts.pem_root_certs = pem;
            }
            baseCreds = grpc::SslCredentials(sslOpts);
        }

        return baseCreds;
    }
} // namespace StockmanNamespace::Grpc

std::unique_ptr<grpc::ClientContext> StockmanNamespace::Grpc::Client::CreateContext(QString* errorMessage)
{
    auto ctx = std::make_unique<grpc::ClientContext>();
    ctx->set_deadline(std::chrono::system_clock::now() + options_.Timeout);
    QString authError;
    if ( !attachAuth(ctx.get(), authError) )
    {
        if ( errorMessage != nullptr )
        {
            *errorMessage = authError;
        }
        return nullptr;
    }
    if ( errorMessage != nullptr )
    {
        errorMessage->clear();
    }
    return ctx;
}

    std::string StockmanNamespace::Grpc::Client::CurrentUsername() const
    {
        std::lock_guard<std::mutex> guard(tokenMutex_);
        return currentUsername_;
    }

    std::string StockmanNamespace::Grpc::Client::CurrentRole() const
    {
        std::lock_guard<std::mutex> guard(tokenMutex_);
        return currentRole_;
    }

bool StockmanNamespace::Grpc::Client::attachAuth(grpc::ClientContext* ctx, QString& errorMessage) const
{
    if ( ctx == nullptr )
    {
        errorMessage = QStringLiteral("context unavailable");
        return false;
    }
    const std::string token = options_.Token;
    if ( token.empty() )
    {
        errorMessage = QStringLiteral("token unavailable");
        return false;
    }
    const std::string header = "Bearer " + token;
    ctx->AddMetadata("authorization", header);
    ctx->AddMetadata("x-kelpie-token", token);
    if ( !options_.ClientName.empty() )
    {
        ctx->AddMetadata("x-client-user", options_.ClientName);
    }
    errorMessage.clear();
    return true;
}

std::chrono::system_clock::time_point StockmanNamespace::Grpc::Client::parseTimestamp(const std::string& value)
{
    if ( value.empty() )
    {
        return std::chrono::system_clock::now();
    }
    const auto datetime = QDateTime::fromString(QString::fromStdString(value), Qt::ISODateWithMs);
    if ( !datetime.isValid() )
    {
        return std::chrono::system_clock::now();
    }
    return std::chrono::system_clock::time_point(std::chrono::milliseconds(datetime.toMSecsSinceEpoch()));
}
