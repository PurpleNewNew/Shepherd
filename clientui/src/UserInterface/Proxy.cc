#include <UserInterface/KelpiePanel.hpp>

#include "Internal.hpp"

#include <QTableWidgetItem>

#include <Util/UiConstants.hpp>

namespace StockmanNamespace::UserInterface
{
    void KelpiePanel::refreshProxies()
    {
        auto* ctrl = controller();
        if ( ctrl == nullptr )
        {
            return;
        }
        if ( refreshProxiesButton_ != nullptr ) { refreshProxiesButton_->setEnabled(false);
}
        if ( proxyTable_ != nullptr ) { proxyTable_->setEnabled(false);
}
        const uint64_t epoch = ctrl->ConnectionEpoch();

        struct Result {
            uint64_t epoch{0};
            bool ok{false};
            QString error;
            std::vector<kelpieui::v1::ProxyInfo> proxies;
        };

        runAsync<Result>(
            this,
            [ctrl, epoch]() {
                Result res;
                res.epoch = epoch;
                QString error;
                std::vector<kelpieui::v1::ProxyInfo> proxies;
                res.ok = ctrl->ListProxies(&proxies, error);
                res.error = error;
                res.proxies = std::move(proxies);
                return res;
            },
            [this](const Result& res) {
                if ( refreshProxiesButton_ ) { refreshProxiesButton_->setEnabled(true);
}
                if ( proxyTable_ ) { proxyTable_->setEnabled(true);
}

                auto* ctrl = controller();
                if ( ctrl == nullptr || ctrl->ConnectionEpoch() != res.epoch )
                {
                    return;
                }
                if ( !res.ok )
                {
                    toastError(tr("Proxy refresh failed: %1").arg(res.error));
                    return;
                }
                populateProxies(res.proxies);
            });
    }
    void KelpiePanel::startForwardProxy()
    {
        auto* ctrl = controller();
        if ( ctrl == nullptr )
        {
            return;
        }
        if ( currentNodeUuid_.isEmpty() )
        {
            toastWarn(tr(StockmanNamespace::UiConstants::kDlgMsgSelectNode));
            return;
        }
        const QString bind = (forwardBindInput_ != nullptr) ? forwardBindInput_->text() : QString();
        const QString remote = (forwardRemoteInput_ != nullptr) ? forwardRemoteInput_->text() : QString();
        if ( bind.isEmpty() || remote.isEmpty() )
        {
            toastWarn(tr(StockmanNamespace::UiConstants::kDlgMsgForwardArgs));
            return;
        }
        const QString targetUuid = currentNodeUuid_;
        setWidgetsEnabled({startForwardButton_, startBackwardButton_, stopProxyButton_, proxyTable_, forwardBindInput_, forwardRemoteInput_}, false);
        toastInfo(tr("Starting forward proxy..."));
        const uint64_t epoch = ctrl->ConnectionEpoch();
        struct Result {
            uint64_t epoch{0};
            bool ok{false};
            QString error;
            kelpieui::v1::StartForwardProxyResponse resp;
        };
        runAsync<Result>(
            this,
            [ctrl, epoch, targetUuid, bind, remote]() {
                Result res;
                res.epoch = epoch;
                QString error;
                kelpieui::v1::StartForwardProxyResponse resp;
                res.ok = ctrl->StartForwardProxy(targetUuid, bind, remote, &resp, error);
                res.error = error;
                res.resp = resp;
                return res;
            },
            [this](const Result& res) {
                setWidgetsEnabled({startForwardButton_, startBackwardButton_, stopProxyButton_, proxyTable_, forwardBindInput_, forwardRemoteInput_}, true);
                auto* ctrl = controller();
                if ( ctrl == nullptr || ctrl->ConnectionEpoch() != res.epoch )
                {
                    return;
                }
                if ( !res.ok )
                {
                    toastError(tr(StockmanNamespace::UiConstants::kDlgMsgStartForwardFailed).arg(res.error));
                    return;
                }
                toastInfo(tr(StockmanNamespace::UiConstants::kDlgMsgStartForwardOk).arg(QString::fromStdString(res.resp.proxy_id())));
                refreshProxies();
                ctrl->RequestSnapshotRefresh();
            });
    }

    void KelpiePanel::startBackwardProxy()
    {
        auto* ctrl = controller();
        if ( ctrl == nullptr )
        {
            return;
        }
        if ( currentNodeUuid_.isEmpty() )
        {
            toastWarn(tr(StockmanNamespace::UiConstants::kDlgMsgSelectNode));
            return;
        }
        const QString remotePort = (backwardRemotePortInput_ != nullptr) ? backwardRemotePortInput_->text() : QString();
        const QString localPort = (backwardLocalPortInput_ != nullptr) ? backwardLocalPortInput_->text() : QString();
        if ( remotePort.isEmpty() || localPort.isEmpty() )
        {
            toastWarn(tr(StockmanNamespace::UiConstants::kDlgMsgBackwardArgs));
            return;
        }
        const QString targetUuid = currentNodeUuid_;
        setWidgetsEnabled({startForwardButton_, startBackwardButton_, stopProxyButton_, proxyTable_, backwardRemotePortInput_, backwardLocalPortInput_}, false);
        toastInfo(tr("Starting backward proxy..."));
        const uint64_t epoch = ctrl->ConnectionEpoch();
        struct Result {
            uint64_t epoch{0};
            bool ok{false};
            QString error;
            kelpieui::v1::StartBackwardProxyResponse resp;
        };
        runAsync<Result>(
            this,
            [ctrl, epoch, targetUuid, remotePort, localPort]() {
                Result res;
                res.epoch = epoch;
                QString error;
                kelpieui::v1::StartBackwardProxyResponse resp;
                res.ok = ctrl->StartBackwardProxy(targetUuid, remotePort, localPort, &resp, error);
                res.error = error;
                res.resp = resp;
                return res;
            },
            [this](const Result& res) {
                setWidgetsEnabled({startForwardButton_, startBackwardButton_, stopProxyButton_, proxyTable_, backwardRemotePortInput_, backwardLocalPortInput_}, true);
                auto* ctrl = controller();
                if ( ctrl == nullptr || ctrl->ConnectionEpoch() != res.epoch )
                {
                    return;
                }
                if ( !res.ok )
                {
                    toastError(tr(StockmanNamespace::UiConstants::kDlgMsgStartBackwardFailed).arg(res.error));
                    return;
                }
                toastInfo(tr(StockmanNamespace::UiConstants::kDlgMsgStartBackwardOk).arg(QString::fromStdString(res.resp.proxy_id())));
                refreshProxies();
                ctrl->RequestSnapshotRefresh();
            });
    }

    void KelpiePanel::stopSelectedProxy()
    {
        auto* ctrl = controller();
        if ( ctrl == nullptr )
        {
            return;
        }
        if ( proxyTable_ == nullptr )
        {
            return;
        }
        auto sel = proxyTable_->currentRow();
        if ( sel < 0 )
        {
            toastWarn(tr(StockmanNamespace::UiConstants::kDlgMsgSelectProxyRow));
            return;
        }
        auto* idItem = proxyTable_->item(sel, 0);
        auto* targetItem = proxyTable_->item(sel, 1);
        auto* kindItem = proxyTable_->item(sel, 2);
        if ( (idItem == nullptr) || (targetItem == nullptr) || (kindItem == nullptr) )
        {
            return;
        }
        const QString id = idItem->text();
        const QString target = targetItem->text();
        const QString kind = kindItem->text().toLower();
        setWidgetsEnabled({stopProxyButton_, startForwardButton_, startBackwardButton_, proxyTable_}, false);
        toastInfo(tr("Stopping proxy %1...").arg(id));
        const uint64_t epoch = ctrl->ConnectionEpoch();
        struct Result {
            uint64_t epoch{0};
            QString id;
            int stopped{0};
            bool ok{false};
            QString error;
        };
        runAsync<Result>(
            this,
            [ctrl, epoch, target, id, kind]() {
                Result res;
                res.epoch = epoch;
                res.id = id;
                QString error;
                int stopped = 0;
                bool ok = false;
                if ( kind.contains(QStringLiteral("back")) )
                {
                    ok = ctrl->StopBackwardProxy(target, id, &stopped, error);
                }
                else
                {
                    ok = ctrl->StopForwardProxy(target, id, &stopped, error);
                }
                res.ok = ok;
                res.error = error;
                res.stopped = stopped;
                return res;
            },
            [this](const Result& res) {
                setWidgetsEnabled({stopProxyButton_, startForwardButton_, startBackwardButton_, proxyTable_}, true);
                auto* ctrl = controller();
                if ( ctrl == nullptr || ctrl->ConnectionEpoch() != res.epoch )
                {
                    return;
                }
                if ( !res.ok )
                {
                    toastError(tr(StockmanNamespace::UiConstants::kDlgMsgStopProxyFailed).arg(res.error));
                    return;
                }
                toastInfo(tr(StockmanNamespace::UiConstants::kDlgMsgStopProxyOk).arg(res.id).arg(res.stopped));
                refreshProxies();
                ctrl->RequestSnapshotRefresh();
            });
    }

    void KelpiePanel::populateProxies(const std::vector<kelpieui::v1::ProxyInfo>& proxies)
    {
        proxyTable_->setRowCount(static_cast<int>(proxies.size()));
        proxyRowIndex_.clear();
        for (size_t i = 0; i < proxies.size(); ++i)
        {
            const auto& proxy = proxies[i];
            const QString proxyId = QString::fromStdString(proxy.proxy_id());
            proxyRowIndex_.insert(proxyId, static_cast<int>(i));
            proxyTable_->setItem(static_cast<int>(i), 0, new QTableWidgetItem(proxyId));
            proxyTable_->setItem(static_cast<int>(i), 1, new QTableWidgetItem(QString::fromStdString(proxy.target_uuid())));
            proxyTable_->setItem(static_cast<int>(i), 2, new QTableWidgetItem(QString::fromStdString(proxy.kind())));
            proxyTable_->setItem(static_cast<int>(i), 3, new QTableWidgetItem(QString::fromStdString(proxy.bind())));
            proxyTable_->setItem(static_cast<int>(i), 4, new QTableWidgetItem(QString::fromStdString(proxy.remote())));
        }
    }

    void KelpiePanel::updateProxyRow(const kelpieui::v1::ProxyEvent& event)
    {
        if ( proxyTable_->rowCount() == 0 )
        {
            refreshProxies();
            return;
        }
        const QString proxyId = QString::fromStdString(event.proxy().proxy_id());
        int row = proxyRowIndex_.value(proxyId, -1);
        if ( row < 0 )
        {
            refreshProxies();
            return;
        }
        proxyTable_->setItem(row, 0, new QTableWidgetItem(proxyId));
        proxyTable_->setItem(row, 1, new QTableWidgetItem(QString::fromStdString(event.proxy().target_uuid())));
        proxyTable_->setItem(row, 2, new QTableWidgetItem(QString::fromStdString(event.proxy().kind())));
        proxyTable_->setItem(row, 3, new QTableWidgetItem(QString::fromStdString(event.proxy().bind())));
        QString remote = QString::fromStdString(event.proxy().remote());
        if ( !event.reason().empty() )
        {
            remote += QStringLiteral(" (%1)").arg(QString::fromStdString(event.reason()));
        }
        proxyTable_->setItem(row, 4, new QTableWidgetItem(remote));
    }
}
