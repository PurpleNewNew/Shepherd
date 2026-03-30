#include <UserInterface/KelpiePanel.hpp>

#include "Internal.hpp"

#include <QTableWidgetItem>

namespace StockmanNamespace::UserInterface
{
    void KelpiePanel::refreshNetworks()
    {
        auto* ctrl = controller();
        if ( (ctrl == nullptr) || (networkTable_ == nullptr) )
        {
            return;
        }
        networkTable_->setEnabled(false);
        const uint64_t epoch = ctrl->ConnectionEpoch();

        struct Result {
            uint64_t epoch{0};
            bool ok{false};
            QString error;
            std::vector<kelpieui::v1::NetworkInfo> networks;
            QString active;
        };

        runAsync<Result>(
            this,
            [ctrl, epoch]() {
                Result res;
                res.epoch = epoch;
                QString error;
                std::vector<kelpieui::v1::NetworkInfo> networks;
                QString active;
                res.ok = ctrl->ListNetworks(&networks, &active, error);
                res.error = error;
                res.networks = std::move(networks);
                res.active = active;
                return res;
            },
            [this](const Result& res) {
                if ( networkTable_ ) { networkTable_->setEnabled(true);
}

                auto* ctrl = controller();
                if ( ctrl == nullptr || ctrl->ConnectionEpoch() != res.epoch )
                {
                    return;
                }
                if ( !res.ok )
                {
                    toastError(tr("Network refresh failed: %1").arg(res.error));
                    if ( networkTable_ ) { networkTable_->setRowCount(0);
}
                    return;
                }

                const auto networkCount = res.networks.size();
                networkTable_->setRowCount(static_cast<int>(networkCount));
                for ( std::size_t index = 0; index < networkCount; ++index )
                {
                    const int row = static_cast<int>(index);
                    const auto& net = res.networks[index];
                    const QString id = QString::fromStdString(net.network_id());
                    networkTable_->setItem(row, 0, new QTableWidgetItem(id));
                    networkTable_->setItem(row, 1, new QTableWidgetItem(net.active() ? tr("Yes") : tr("No")));
                    networkTable_->setItem(row, 2, new QTableWidgetItem(QString::number(net.target_uuids_size())));
                    if ( id == res.active )
                    {
                        networkTable_->selectRow(row);
                    }
                }
            });
    }

    void KelpiePanel::useSelectedNetwork()
    {
        auto* ctrl = controller();
        if ( ctrl == nullptr )
        {
            return;
        }
        if ( networkTable_ == nullptr )
        {
            return;
        }
        auto sel = networkTable_->currentRow();
        if ( sel < 0 )
        {
            toastWarn(tr("Select a network first"));
            return;
        }
        auto* idItem = networkTable_->item(sel, 0);
        if ( idItem == nullptr )
        {
            return;
        }
        const QString networkId = idItem->text().trimmed();
        if ( networkId.isEmpty() )
        {
            toastWarn(tr("Invalid network id"));
            return;
        }
        setWidgetsEnabled({useNetworkButton_, resetNetworkButton_, setNodeNetworkButton_, networkTable_}, false);
        toastInfo(tr("Switching active network..."));
        const uint64_t epoch = ctrl->ConnectionEpoch();
        struct Result {
            uint64_t epoch{0};
            QString active;
            bool ok{false};
            QString error;
        };
        runAsync<Result>(
            this,
            [ctrl, epoch, networkId]() {
                Result res;
                res.epoch = epoch;
                QString error;
                QString active;
                res.ok = ctrl->UseNetwork(networkId, &active, error);
                res.error = error;
                res.active = active;
                return res;
            },
            [this](const Result& res) {
                setWidgetsEnabled({useNetworkButton_, resetNetworkButton_, setNodeNetworkButton_, networkTable_}, true);
                auto* ctrl = controller();
                if ( ctrl == nullptr || ctrl->ConnectionEpoch() != res.epoch )
                {
                    return;
                }
                if ( !res.ok )
                {
                    toastError(tr("Use network failed: %1").arg(res.error));
                    return;
                }
                toastInfo(tr("Active network: %1").arg(res.active));
                refreshNetworks();
            });
    }

    void KelpiePanel::resetNetwork()
    {
        auto* ctrl = controller();
        if ( ctrl == nullptr )
        {
            return;
        }
        if ( networkTable_ == nullptr )
        {
            return;
        }
        setWidgetsEnabled({useNetworkButton_, resetNetworkButton_, setNodeNetworkButton_, networkTable_}, false);
        toastInfo(tr("Resetting active network..."));
        const uint64_t epoch = ctrl->ConnectionEpoch();
        struct Result {
            uint64_t epoch{0};
            QString active;
            bool ok{false};
            QString error;
        };
        runAsync<Result>(
            this,
            [ctrl, epoch]() {
                Result res;
                res.epoch = epoch;
                QString error;
                QString active;
                res.ok = ctrl->ResetNetwork(&active, error);
                res.error = error;
                res.active = active;
                return res;
            },
            [this](const Result& res) {
                setWidgetsEnabled({useNetworkButton_, resetNetworkButton_, setNodeNetworkButton_, networkTable_}, true);
                auto* ctrl = controller();
                if ( ctrl == nullptr || ctrl->ConnectionEpoch() != res.epoch )
                {
                    return;
                }
                if ( !res.ok )
                {
                    toastError(tr("Reset network failed: %1").arg(res.error));
                    return;
                }
                toastInfo(tr("Active network reset to %1").arg(res.active));
                refreshNetworks();
            });
    }

    void KelpiePanel::setNodeNetwork()
    {
        auto* ctrl = controller();
        if ( ctrl == nullptr )
        {
            return;
        }
        if ( currentNodeUuid_.isEmpty() )
        {
            toastWarn(tr("Select a node first"));
            return;
        }
        if ( networkTable_ == nullptr )
        {
            return;
        }
        auto sel = networkTable_->currentRow();
        if ( sel < 0 )
        {
            toastWarn(tr("Select a network first"));
            return;
        }
        auto* idItem = networkTable_->item(sel, 0);
        if ( idItem == nullptr )
        {
            return;
        }
        const QString targetUuid = currentNodeUuid_;
        const QString networkId = idItem->text().trimmed();
        if ( networkId.isEmpty() )
        {
            toastWarn(tr("Invalid network id"));
            return;
        }
        setWidgetsEnabled({useNetworkButton_, resetNetworkButton_, setNodeNetworkButton_, networkTable_, nodesTree_}, false);
        toastInfo(tr("Binding %1 to network %2...").arg(targetUuid, networkId));
        const uint64_t epoch = ctrl->ConnectionEpoch();
        struct Result {
            uint64_t epoch{0};
            QString targetUuid;
            QString networkId;
            bool ok{false};
            QString error;
        };
        runAsync<Result>(
            this,
            [ctrl, epoch, targetUuid, networkId]() {
                Result res;
                res.epoch = epoch;
                res.targetUuid = targetUuid;
                res.networkId = networkId;
                QString error;
                res.ok = ctrl->SetNodeNetwork(targetUuid, networkId, error);
                res.error = error;
                return res;
            },
            [this](const Result& res) {
                setWidgetsEnabled({useNetworkButton_, resetNetworkButton_, setNodeNetworkButton_, networkTable_, nodesTree_}, true);
                auto* ctrl = controller();
                if ( ctrl == nullptr || ctrl->ConnectionEpoch() != res.epoch )
                {
                    return;
                }
                if ( !res.ok )
                {
                    toastError(tr("Set node network failed: %1").arg(res.error));
                    return;
                }
                toastInfo(tr("Node %1 bound to network %2").arg(res.targetUuid, res.networkId));
                refreshNetworks();
                ctrl->RequestSnapshotRefresh();
            });
    }
    void KelpiePanel::pruneOffline()
    {
        auto* ctrl = controller();
        if ( ctrl == nullptr )
        {
            return;
        }
        setWidgetsEnabled({pruneOfflineButton_, refreshNetworksButton_, networkTable_}, false);
        toastInfo(tr("Pruning offline nodes..."));
        const uint64_t epoch = ctrl->ConnectionEpoch();
        struct Result {
            uint64_t epoch{0};
            int removed{0};
            bool ok{false};
            QString error;
        };
        runAsync<Result>(
            this,
            [ctrl, epoch]() {
                Result res;
                res.epoch = epoch;
                QString error;
                int removed = 0;
                res.ok = ctrl->PruneOffline(&removed, error);
                res.error = error;
                res.removed = removed;
                return res;
            },
            [this](const Result& res) {
                setWidgetsEnabled({pruneOfflineButton_, refreshNetworksButton_, networkTable_}, true);
                auto* ctrl = controller();
                if ( ctrl == nullptr || ctrl->ConnectionEpoch() != res.epoch )
                {
                    return;
                }
                if ( !res.ok )
                {
                    toastError(tr("Prune offline failed: %1").arg(res.error));
                    return;
                }
                toastInfo(tr("Pruned %1 offline nodes").arg(res.removed));
                refreshNetworks();
                refreshSupplemental();
                ctrl->RequestSnapshotRefresh();
            });
    }
}
