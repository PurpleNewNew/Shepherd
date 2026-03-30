#include <UserInterface/KelpiePanel.hpp>

#include "Internal.hpp"

#include <QHostAddress>
#include <QMetaObject>
#include <QTcpServer>
#include <QTcpSocket>

#include <algorithm>

namespace StockmanNamespace::UserInterface
{
    void KelpiePanel::startSocksBridge()
    {
        if ( currentNodeUuid_.isEmpty() )
        {
            toastWarn(tr("Select a node first"));
            return;
        }
        bool ok = false;
        const int port = (socksPortInput_ != nullptr) ? socksPortInput_->text().toInt(&ok) : 0;
        if ( !ok || port <= 0 || port > 65535 )
        {
            toastWarn(tr("Invalid SOCKS local port"));
            return;
        }
        if ( socksServer_ != nullptr )
        {
            return;
        }

        socksServer_ = new QTcpServer(this);
        connect(socksServer_, &QTcpServer::newConnection, this, &KelpiePanel::onNewSocksConnection);
        if ( !socksServer_->listen(QHostAddress::LocalHost, static_cast<quint16>(port)) )
        {
            toastError(tr("Start SOCKS bridge failed: %1").arg(socksServer_->errorString()));
            socksServer_->deleteLater();
            socksServer_ = nullptr;
            return;
        }
        if ( startSocksButton_ != nullptr ) { startSocksButton_->setEnabled(false);
}
        if ( stopSocksButton_ != nullptr ) { stopSocksButton_->setEnabled(true);
}
        if ( socksStatusLabel_ != nullptr )
        {
            socksStatusLabel_->setText(tr("SOCKS: listening on 127.0.0.1:%1").arg(port));
        }
        toastInfo(tr("SOCKS bridge started on 127.0.0.1:%1").arg(port));
    }

    void KelpiePanel::stopSocksBridge()
    {
        stopSocksServer();
    }

    void KelpiePanel::onNewSocksConnection()
    {
        if ( socksServer_ == nullptr )
        {
            return;
        }
        auto* ctrl = controller();
        if ( ctrl == nullptr )
        {
            stopSocksServer();
            toastWarn(tr("SOCKS bridge stopped: gRPC client unavailable"));
            return;
        }

        const QString targetUuid = currentNodeUuid_;
        const uint64_t epoch = ctrl->ConnectionEpoch();

        while ( socksServer_->hasPendingConnections() )
        {
            QTcpSocket* socket = socksServer_->nextPendingConnection();
            if ( socket == nullptr )
            {
                continue;
            }
            const bool withAuth = (socksAuthCheck_ != nullptr) ? socksAuthCheck_->isChecked() : false;
            const auto auth = withAuth ? kelpieui::v1::SOCKS_PROXY_AUTH_USERPASS
                                       : kelpieui::v1::SOCKS_PROXY_AUTH_NONE;
            const QString user = (withAuth && (socksUserInput_ != nullptr)) ? socksUserInput_->text() : QString();
            const QString pass = (withAuth && (socksPasswordInput_ != nullptr)) ? socksPasswordInput_->text() : QString();

            QPointer<QTcpSocket> rawSocket = socket;
            struct Result {
                uint64_t epoch{0};
                bool ok{false};
                QString error;
                std::shared_ptr<StockmanNamespace::ProxyStreamHandle> handle;
            };
            runAsync<Result>(
                this,
                [ctrl, epoch, targetUuid, auth, user, pass]() {
                    Result res;
                    res.epoch = epoch;
                    QString error;
                    auto handle = ctrl->StartSocksProxy(targetUuid, auth, user, pass, error);
                    res.ok = (handle != nullptr);
                    res.error = error;
                    res.handle = std::move(handle);
                    return res;
                },
                [this, rawSocket](const Result& res) {
                    auto* ctrl = controller();
                    if ( ctrl == nullptr || ctrl->ConnectionEpoch() != res.epoch )
                    {
                        if ( res.handle )
                        {
                            closeHandleAsync(res.handle);
                        }
                        if ( rawSocket != nullptr )
                        {
                            rawSocket->disconnectFromHost();
                            rawSocket->deleteLater();
                        }
                        return;
                    }

                    if ( !res.ok || !res.handle )
                    {
                        toastError(tr("Start remote SOCKS stream failed: %1").arg(res.error));
                        if ( rawSocket != nullptr )
                        {
                            rawSocket->disconnectFromHost();
                            rawSocket->deleteLater();
                        }
                        return;
                    }
                    if ( rawSocket == nullptr || rawSocket->state() != QAbstractSocket::ConnectedState )
                    {
                        closeHandleAsync(res.handle);
                        return;
                    }

                    auto bridge = std::make_unique<SocksBridge>();
                    bridge->socket = rawSocket;
                    bridge->handle = res.handle;
                    auto streamHandle = res.handle;

                    // Flush any buffered client bytes before wiring signals.
                    const QByteArray initial = rawSocket->readAll();
                    if ( !initial.isEmpty() )
                    {
                        streamHandle->SendData(initial);
                    }

                    connect(rawSocket, &QTcpSocket::readyRead, this, [rawSocket, streamHandle]() {
                        if ( rawSocket == nullptr || streamHandle == nullptr )
                        {
                            return;
                        }
                        const QByteArray payload = rawSocket->readAll();
                        if ( !payload.isEmpty() )
                        {
                            streamHandle->SendData(payload);
                        }
                    });
                    connect(rawSocket, &QTcpSocket::disconnected, this, [this, rawSocket, streamHandle]() {
                        if ( streamHandle )
                        {
                            closeHandleAsync(streamHandle);
                        }
                        removeSocksBridge(rawSocket.data());
                        if ( rawSocket != nullptr )
                        {
                            rawSocket->deleteLater();
                        }
                    });
                    connect(streamHandle.get(), &ProxyStreamHandle::DataReceived, this, [rawSocket](const QByteArray& data) {
                        if ( rawSocket != nullptr && rawSocket->state() == QAbstractSocket::ConnectedState && !data.isEmpty() )
                        {
                            rawSocket->write(data);
                        }
                    });
                    connect(streamHandle.get(), &ProxyStreamHandle::Closed, this, [this, rawSocket](const QString&) {
                        if ( rawSocket != nullptr && rawSocket->state() == QAbstractSocket::ConnectedState )
                        {
                            rawSocket->disconnectFromHost();
                        }
                        removeSocksBridge(rawSocket.data());
                    });

                    registerSocksBridge(std::move(bridge));
                });
        }
    }

    void KelpiePanel::registerSocksBridge(std::unique_ptr<SocksBridge> bridge)
    {
        if ( bridge == nullptr )
        {
            return;
        }
        socksBridges_.push_back(std::move(bridge));
    }

    void KelpiePanel::removeSocksBridge(QTcpSocket* socket)
    {
        if ( socket == nullptr )
        {
            return;
        }
        socksBridges_.erase(
            std::remove_if(
                socksBridges_.begin(),
                socksBridges_.end(),
                [socket](const std::unique_ptr<SocksBridge>& bridge) {
                    return bridge == nullptr || bridge->socket == nullptr || bridge->socket.data() == socket;
                }),
            socksBridges_.end());
    }

    void KelpiePanel::stopSocksServer()
    {
        auto bridges = std::move(socksBridges_);
        socksBridges_.clear();
        for ( auto& bridge : bridges )
        {
            if ( bridge == nullptr )
            {
                continue;
            }
            if ( bridge->handle )
            {
                closeHandleAsync(bridge->handle);
            }
            if ( bridge->socket != nullptr )
            {
                bridge->socket->disconnectFromHost();
                bridge->socket->deleteLater();
            }
        }

        if ( socksServer_ != nullptr )
        {
            socksServer_->close();
            socksServer_->deleteLater();
            socksServer_ = nullptr;
        }
        if ( startSocksButton_ != nullptr ) { startSocksButton_->setEnabled(!currentNodeUuid_.isEmpty());
}
        if ( stopSocksButton_ != nullptr ) { stopSocksButton_->setEnabled(false);
}
        if ( socksStatusLabel_ != nullptr ) { socksStatusLabel_->setText(tr("SOCKS: stopped"));
}
    }
}
