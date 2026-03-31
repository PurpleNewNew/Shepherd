#include <UserInterface/KelpiePanel.hpp>
#include <UserInterface/Pages/ChatPage.hpp>
#include <UserInterface/Pages/LootPage.hpp>
#include <UserInterface/Pages/ShellPage.hpp>
#include <UserInterface/Pages/TaskingPage.hpp>

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
        connect(chatPage_->input, &QLineEdit::returnPressed, this, &KelpiePanel::sendChatMessage);
        connect(chatPage_->sendButton, &QPushButton::clicked, this, &KelpiePanel::sendChatMessage);
        connect(chatPage_->refreshButton, &QPushButton::clicked, this, &KelpiePanel::refreshChat);
        connect(lootPage_->refreshButton, &QPushButton::clicked, this, &KelpiePanel::refreshLoot);
        connect(lootPage_->submitButton, &QPushButton::clicked, this, &KelpiePanel::submitLootFromFile);
        connect(lootPage_->downloadButton, &QPushButton::clicked, this, &KelpiePanel::downloadSelectedLoot);
        connect(workspace_.refreshProxiesButton, &QPushButton::clicked, this, &KelpiePanel::refreshProxies);
        connect(refreshListenersButton_, &QPushButton::clicked, this, &KelpiePanel::refreshListeners);
        connect(streams_.closeButton, &QPushButton::clicked, this, &KelpiePanel::closeSelectedStream);

        connect(createPivotListenerButton_, &QPushButton::clicked, this, &KelpiePanel::createPivotListener);
        connect(updatePivotListenerButton_, &QPushButton::clicked, this, &KelpiePanel::editPivotListener);
        connect(deletePivotListenerButton_, &QPushButton::clicked, this, &KelpiePanel::deletePivotListener);
        connect(createControllerListenerButton_, &QPushButton::clicked, this, &KelpiePanel::createControllerListener);
        connect(updateControllerListenerButton_, &QPushButton::clicked, this, &KelpiePanel::editControllerListener);
        connect(deleteControllerListenerButton_, &QPushButton::clicked, this, &KelpiePanel::deleteControllerListener);

        connect(topology_.refreshButton, &QPushButton::clicked, this, &KelpiePanel::refreshTopology);
        connect(topology_.fitButton, &QPushButton::clicked, this, &KelpiePanel::fitTopologyGraph);
        connect(refreshSessionListButton_, &QPushButton::clicked, this, &KelpiePanel::refreshSessionList);
        connect(refreshSupplementalButton_, &QPushButton::clicked, this, &KelpiePanel::refreshSupplemental);
        connect(refreshAuditButton_, &QPushButton::clicked, this, &KelpiePanel::refreshAudit);
        connect(refreshNetworksButton_, &QPushButton::clicked, this, &KelpiePanel::refreshNetworks);
        connect(useNetworkButton_, &QPushButton::clicked, this, &KelpiePanel::useSelectedNetwork);
        connect(resetNetworkButton_, &QPushButton::clicked, this, &KelpiePanel::resetNetwork);
        connect(setNodeNetworkButton_, &QPushButton::clicked, this, &KelpiePanel::setNodeNetwork);
        connect(pruneOfflineButton_, &QPushButton::clicked, this, &KelpiePanel::pruneOffline);

        connect(taskingPage_->refreshSleepButton, &QPushButton::clicked, this, &KelpiePanel::refreshSleep);
        connect(taskingPage_->updateSleepButton, &QPushButton::clicked, this, &KelpiePanel::updateSleep);
        connect(taskingPage_->startDialButton, &QPushButton::clicked, this, &KelpiePanel::startDial);
        connect(taskingPage_->cancelDialButton, &QPushButton::clicked, this, &KelpiePanel::cancelDial);
        connect(taskingPage_->startSshSessionButton, &QPushButton::clicked, this, &KelpiePanel::startSshSession);
        connect(taskingPage_->startSshTunnelButton, &QPushButton::clicked, this, &KelpiePanel::startSshTunnel);

        connect(refreshDtnButton_, &QPushButton::clicked, this, &KelpiePanel::refreshDtn);
        connect(enqueueDtnButton_, &QPushButton::clicked, this, &KelpiePanel::enqueueDtn);

        connect(startForwardButton_, &QPushButton::clicked, this, &KelpiePanel::startForwardProxy);
        connect(startBackwardButton_, &QPushButton::clicked, this, &KelpiePanel::startBackwardProxy);
        connect(stopProxyButton_, &QPushButton::clicked, this, &KelpiePanel::stopSelectedProxy);

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

        connect(shellPage_->openButton, &QPushButton::clicked, this, &KelpiePanel::startShell);
        connect(shellPage_->closeButton, &QPushButton::clicked, this, &KelpiePanel::stopShell);
        connect(shellPage_->input, &QLineEdit::returnPressed, this, &KelpiePanel::sendShellInput);
        connect(shellPage_->browseDownloadButton, &QPushButton::clicked, this, &KelpiePanel::browseDownloadPath);
        connect(shellPage_->startDownloadButton, &QPushButton::clicked, this, &KelpiePanel::startDownloadFile);
        connect(shellPage_->browseUploadButton, &QPushButton::clicked, this, &KelpiePanel::browseUploadPath);
        connect(shellPage_->startUploadButton, &QPushButton::clicked, this, &KelpiePanel::startUploadFile);
        connect(shellPage_->startSocksButton, &QPushButton::clicked, this, &KelpiePanel::startSocksBridge);
        connect(shellPage_->stopSocksButton, &QPushButton::clicked, this, &KelpiePanel::stopSocksBridge);

        connect(supplementalFilter_, &QLineEdit::textChanged, this, &KelpiePanel::applySupplementalFilter);
        connect(refreshRepairsButton_, &QPushButton::clicked, this, &KelpiePanel::refreshRepairs);
        connect(streams_.refreshDiagnosticsButton, &QPushButton::clicked, this, [this]() {
            refreshMetrics();
            refreshStreamDiagnostics();
        });
        connect(streams_.pingButton, &QPushButton::clicked, this, &KelpiePanel::streamPing);

    }

    void KelpiePanel::wireWorkspaceActions()
    {
        connect(streams_.table, &QTableWidget::itemSelectionChanged, this, [this]() {
            if ( streams_.closeButton && streams_.table )
            {
                streams_.closeButton->setEnabled(streams_.table->currentRow() >= 0);
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
        connect(topology_.scene, &QGraphicsScene::selectionChanged, this, [this]() {
            if ( topology_.scene == nullptr )
            {
                return;
            }
            const auto selected = topology_.scene->selectedItems();
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
                    if ( topology_.edgesTable != nullptr )
                    {
                        for ( int row = 0; row < topology_.edgesTable->rowCount(); ++row )
                        {
                            auto* parentItem = topology_.edgesTable->item(row, 0);
                            auto* childItem = topology_.edgesTable->item(row, 1);
                            if ( (parentItem != nullptr) && (childItem != nullptr) &&
                                 parentItem->text() == parent && childItem->text() == child )
                            {
                                const QSignalBlocker blocker(topology_.edgesTable);
                                topology_.edgesTable->setCurrentCell(row, 0);
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

        connect(topology_.edgesTable, &QTableWidget::itemSelectionChanged, this, [this]() {
            if ( !topology_.edgesTable )
            {
                return;
            }
            const int row = topology_.edgesTable->currentRow();
            if ( row < 0 )
            {
                setTopologyHighlightNode(currentNodeUuid_);
                return;
            }
            auto* parentItem = topology_.edgesTable->item(row, 0);
            auto* childItem = topology_.edgesTable->item(row, 1);
            const QString parent = parentItem ? parentItem->text() : QString();
            const QString child = childItem ? childItem->text() : QString();
            setTopologyHighlightEdge(parent, child);
        });

        connect(topology_.filterInput, &QLineEdit::textChanged, this, [this](const QString&) {
            scheduleTopologyViewRefresh();
        });
        connect(topology_.layoutBox, QOverload<int>::of(&QComboBox::currentIndexChanged), this, [this](int) {
            scheduleTopologyViewRefresh();
        });
        connect(topology_.showSupplementalCheck, &QCheckBox::toggled, this, [this](bool) {
            scheduleTopologyViewRefresh();
        });
        connect(topology_.locateButton, &QPushButton::clicked, this, &KelpiePanel::locateTopologyNode);
        connect(topology_.locateInput, &QLineEdit::returnPressed, this, &KelpiePanel::locateTopologyNode);
    }
}
