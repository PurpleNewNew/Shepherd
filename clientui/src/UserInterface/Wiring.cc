#include <UserInterface/KelpiePanel.hpp>

#include <QGraphicsItem>
#include <QGraphicsScene>
#include <QSignalBlocker>

namespace StockmanNamespace::UserInterface
{
    void KelpiePanel::wireStateActions()
    {
        connect(nodesTree_, &QTreeWidget::itemSelectionChanged, this, &KelpiePanel::updateSelectedNode);
        connect(nodeMemoInput_, &QLineEdit::returnPressed, this, &KelpiePanel::updateNodeMemo);
        connect(updateNodeMemoButton_, &QPushButton::clicked, this, &KelpiePanel::updateNodeMemo);
        connect(nodeCommandInput_, &QLineEdit::returnPressed, this, &KelpiePanel::sendNodeCommand);
        connect(nodeCommandButton_, &QPushButton::clicked, this, &KelpiePanel::sendNodeCommand);
        connect(chatInput_, &QLineEdit::returnPressed, this, &KelpiePanel::sendChatMessage);
        connect(sendChatButton_, &QPushButton::clicked, this, &KelpiePanel::sendChatMessage);
        connect(refreshChatButton_, &QPushButton::clicked, this, &KelpiePanel::refreshChat);
        connect(refreshLootButton_, &QPushButton::clicked, this, &KelpiePanel::refreshLoot);
        connect(submitLootButton_, &QPushButton::clicked, this, &KelpiePanel::submitLootFromFile);
        connect(downloadLootButton_, &QPushButton::clicked, this, &KelpiePanel::downloadSelectedLoot);
        connect(refreshProxiesButton_, &QPushButton::clicked, this, &KelpiePanel::refreshProxies);
        connect(refreshListenersButton_, &QPushButton::clicked, this, &KelpiePanel::refreshListeners);
        connect(closeStreamButton_, &QPushButton::clicked, this, &KelpiePanel::closeSelectedStream);

        connect(createPivotListenerButton_, &QPushButton::clicked, this, &KelpiePanel::createPivotListener);
        connect(updatePivotListenerButton_, &QPushButton::clicked, this, &KelpiePanel::editPivotListener);
        connect(deletePivotListenerButton_, &QPushButton::clicked, this, &KelpiePanel::deletePivotListener);
        connect(createControllerListenerButton_, &QPushButton::clicked, this, &KelpiePanel::createControllerListener);
        connect(updateControllerListenerButton_, &QPushButton::clicked, this, &KelpiePanel::editControllerListener);
        connect(deleteControllerListenerButton_, &QPushButton::clicked, this, &KelpiePanel::deleteControllerListener);

        connect(refreshTopologyButton_, &QPushButton::clicked, this, &KelpiePanel::refreshTopology);
        connect(fitTopologyButton_, &QPushButton::clicked, this, &KelpiePanel::fitTopologyGraph);
        connect(refreshSessionListButton_, &QPushButton::clicked, this, &KelpiePanel::refreshSessionList);
        connect(refreshSupplementalButton_, &QPushButton::clicked, this, &KelpiePanel::refreshSupplemental);
        connect(refreshAuditButton_, &QPushButton::clicked, this, &KelpiePanel::refreshAudit);
        connect(refreshNetworksButton_, &QPushButton::clicked, this, &KelpiePanel::refreshNetworks);
        connect(useNetworkButton_, &QPushButton::clicked, this, &KelpiePanel::useSelectedNetwork);
        connect(resetNetworkButton_, &QPushButton::clicked, this, &KelpiePanel::resetNetwork);
        connect(setNodeNetworkButton_, &QPushButton::clicked, this, &KelpiePanel::setNodeNetwork);
        connect(pruneOfflineButton_, &QPushButton::clicked, this, &KelpiePanel::pruneOffline);

        connect(refreshSleepButton_, &QPushButton::clicked, this, &KelpiePanel::refreshSleep);
        connect(updateSleepButton_, &QPushButton::clicked, this, &KelpiePanel::updateSleep);
        connect(startDialButton_, &QPushButton::clicked, this, &KelpiePanel::startDial);
        connect(cancelDialButton_, &QPushButton::clicked, this, &KelpiePanel::cancelDial);
        connect(startSshSessionButton_, &QPushButton::clicked, this, &KelpiePanel::startSshSession);
        connect(startSshTunnelButton_, &QPushButton::clicked, this, &KelpiePanel::startSshTunnel);

        connect(refreshDtnButton_, &QPushButton::clicked, this, &KelpiePanel::refreshDtn);
        connect(refreshDtnPolicyButton_, &QPushButton::clicked, this, &KelpiePanel::refreshDtnPolicy);
        connect(applyDtnPolicyButton_, &QPushButton::clicked, this, &KelpiePanel::applyDtnPolicy);
        connect(enqueueDtnButton_, &QPushButton::clicked, this, &KelpiePanel::enqueueDtn);

        connect(startForwardButton_, &QPushButton::clicked, this, &KelpiePanel::startForwardProxy);
        connect(startBackwardButton_, &QPushButton::clicked, this, &KelpiePanel::startBackwardProxy);
        connect(stopProxyButton_, &QPushButton::clicked, this, &KelpiePanel::stopSelectedProxy);
        connect(applyRoutingButton_, &QPushButton::clicked, this, &KelpiePanel::applyRoutingStrategy);

        connect(markAliveButton_, &QPushButton::clicked, this, [this]() {
            markCurrentSession(kelpieui::v1::SESSION_MARK_ACTION_ALIVE);
        });
        connect(markDeadButton_, &QPushButton::clicked, this, [this]() {
            markCurrentSession(kelpieui::v1::SESSION_MARK_ACTION_DEAD);
        });
        connect(nodeStatusButton_, &QPushButton::clicked, this, &KelpiePanel::queryNodeStatus);
        connect(repairSessionButton_, &QPushButton::clicked, this, &KelpiePanel::repairCurrentSession);
        connect(reconnectSessionButton_, &QPushButton::clicked, this, &KelpiePanel::reconnectCurrentSession);
        connect(terminateSessionButton_, &QPushButton::clicked, this, &KelpiePanel::terminateCurrentSession);
        connect(shutdownNodeButton_, &QPushButton::clicked, this, &KelpiePanel::shutdownCurrentNode);

        connect(openShellButton_, &QPushButton::clicked, this, &KelpiePanel::startShell);
        connect(closeShellButton_, &QPushButton::clicked, this, &KelpiePanel::stopShell);
        connect(shellInput_, &QLineEdit::returnPressed, this, &KelpiePanel::sendShellInput);
        connect(browseDownloadButton_, &QPushButton::clicked, this, &KelpiePanel::browseDownloadPath);
        connect(startDownloadButton_, &QPushButton::clicked, this, &KelpiePanel::startDownloadFile);
        connect(browseUploadButton_, &QPushButton::clicked, this, &KelpiePanel::browseUploadPath);
        connect(startUploadButton_, &QPushButton::clicked, this, &KelpiePanel::startUploadFile);
        connect(startSocksButton_, &QPushButton::clicked, this, &KelpiePanel::startSocksBridge);
        connect(stopSocksButton_, &QPushButton::clicked, this, &KelpiePanel::stopSocksBridge);

        connect(supplementalFilter_, &QLineEdit::textChanged, this, &KelpiePanel::applySupplementalFilter);
        connect(refreshRepairsButton_, &QPushButton::clicked, this, &KelpiePanel::refreshRepairs);
        connect(refreshDiagnosticsButton_, &QPushButton::clicked, this, [this]() {
            refreshMetrics();
            refreshStreamDiagnostics();
        });
        connect(streamPingButton_, &QPushButton::clicked, this, &KelpiePanel::streamPing);

        connect(dtnPolicyTable_, &QTableWidget::itemSelectionChanged, this, [this]() {
            if ( !dtnPolicyTable_ || !dtnPolicyKeyInput_ || !dtnPolicyValueInput_ )
            {
                return;
            }
            const int row = dtnPolicyTable_->currentRow();
            if ( row < 0 )
            {
                return;
            }
            auto* keyItem = dtnPolicyTable_->item(row, 0);
            auto* valueItem = dtnPolicyTable_->item(row, 1);
            if ( keyItem ) { dtnPolicyKeyInput_->setText(keyItem->text());
}
            if ( valueItem ) { dtnPolicyValueInput_->setText(valueItem->text());
}
        });
    }

    void KelpiePanel::wireWorkspaceActions()
    {
        connect(streamsTable_, &QTableWidget::itemSelectionChanged, this, [this]() {
            if ( closeStreamButton_ && streamsTable_ )
            {
                closeStreamButton_->setEnabled(streamsTable_->currentRow() >= 0);
            }
        });

        connect(stateTabs_, &QTabWidget::currentChanged, this, [this](int) {
            refreshStatePage(stateTabs_ ? stateTabs_->currentWidget() : nullptr);
        });
        connect(workspaceTabs_, &QTabWidget::currentChanged, this, [this](int) {
            refreshWorkspacePage(workspaceTabs_ ? workspaceTabs_->currentWidget() : nullptr);
        });
    }

    void KelpiePanel::wireTopologyInteractions()
    {
        connect(topologyScene_, &QGraphicsScene::selectionChanged, this, [this]() {
            if ( topologyScene_ == nullptr )
            {
                return;
            }
            const auto selected = topologyScene_->selectedItems();
            for ( auto* item : selected )
            {
                if ( item == nullptr )
                {
                    continue;
                }
                const QString kind = item->data(1).toString();
                if ( kind == QStringLiteral("edge") )
                {
                    const QString parent = item->data(2).toString();
                    const QString child = item->data(3).toString();
                    setTopologyHighlightEdge(parent, child);
                    if ( topologyEdgesTable_ != nullptr )
                    {
                        for ( int row = 0; row < topologyEdgesTable_->rowCount(); ++row )
                        {
                            auto* parentItem = topologyEdgesTable_->item(row, 0);
                            auto* childItem = topologyEdgesTable_->item(row, 1);
                            if ( (parentItem != nullptr) && (childItem != nullptr) &&
                                 parentItem->text() == parent && childItem->text() == child )
                            {
                                const QSignalBlocker blocker(topologyEdgesTable_);
                                topologyEdgesTable_->setCurrentCell(row, 0);
                                break;
                            }
                        }
                    }
                    return;
                }
                const QVariant uuid = item->data(0);
                if ( uuid.isValid() && !uuid.toString().isEmpty() )
                {
                    const QString selectedUuid = uuid.toString();
                    setTopologyHighlightNode(selectedUuid);
                    selectNodeByUuid(selectedUuid);
                    return;
                }
            }
        });

        connect(topologyEdgesTable_, &QTableWidget::itemSelectionChanged, this, [this]() {
            if ( !topologyEdgesTable_ )
            {
                return;
            }
            const int row = topologyEdgesTable_->currentRow();
            if ( row < 0 )
            {
                setTopologyHighlightNode(currentNodeUuid_);
                return;
            }
            auto* parentItem = topologyEdgesTable_->item(row, 0);
            auto* childItem = topologyEdgesTable_->item(row, 1);
            const QString parent = parentItem ? parentItem->text() : QString();
            const QString child = childItem ? childItem->text() : QString();
            setTopologyHighlightEdge(parent, child);
        });

        connect(topologyFilterInput_, &QLineEdit::textChanged, this, [this](const QString&) {
            scheduleTopologyViewRefresh();
        });
        connect(topologyLayoutBox_, QOverload<int>::of(&QComboBox::currentIndexChanged), this, [this](int) {
            scheduleTopologyViewRefresh();
        });
        connect(topologyShowSupplementalCheck_, &QCheckBox::toggled, this, [this](bool) {
            scheduleTopologyViewRefresh();
        });
        connect(topologyLocateButton_, &QPushButton::clicked, this, &KelpiePanel::locateTopologyNode);
        connect(topologyLocateInput_, &QLineEdit::returnPressed, this, &KelpiePanel::locateTopologyNode);
    }
}
