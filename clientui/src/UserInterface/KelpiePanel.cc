#include <UserInterface/KelpiePanel.hpp>
#include <UserInterface/Pages/ChatPage.hpp>
#include <UserInterface/Pages/LootPage.hpp>
#include <UserInterface/Pages/ShellPage.hpp>
#include <UserInterface/Pages/TaskingPage.hpp>
#include <UserInterface/StockmanUI.hpp>
#include "Internal.hpp"

#include <QSignalBlocker>
#include <QTimer>

#include <Stockman/AppContext.hpp>
#include <Stockman/KelpieController.hpp>
#include <Stockman/KelpieState.hpp>

namespace
{
    constexpr int kStreamRefreshDebounceMs = 250;
    constexpr int kDialRefreshCooldownMs = 1500;
    constexpr int kTopologyViewDebounceMs = 120;
}

namespace StockmanNamespace::UserInterface
{
    KelpiePanel::KelpiePanel(std::shared_ptr<StockmanNamespace::AppContext> ctx, QWidget* parent)
        : QWidget(parent), context_(std::move(ctx))
    {
        setupUi();
        streamRefreshDebounce_ = new QTimer(this);
        streamRefreshDebounce_->setSingleShot(true);
        streamRefreshDebounce_->setInterval(kStreamRefreshDebounceMs);
        connect(streamRefreshDebounce_, &QTimer::timeout, this, [this]() {
            const bool taskingVisible = (stateTabs_ != nullptr) && (stateTabs_->currentWidget() == taskingPage_);
            const bool streamDiagnosticsVisible =
                ((workspaceTabs_ != nullptr) && (workspaceTabs_->currentWidget() == streams_.page)) ||
                ((stateTabs_ != nullptr) && (stateTabs_->currentWidget() == hostIntelPage_));
            if ( taskingVisible )
            {
                refreshSsh();
            }
            if ( streamDiagnosticsVisible )
            {
                refreshStreamDiagnostics();
            }
        });

        dialRefreshCooldown_ = new QTimer(this);
        dialRefreshCooldown_->setSingleShot(true);
        dialRefreshCooldown_->setInterval(kDialRefreshCooldownMs);

        topology_.refreshDebounce = new QTimer(this);
        topology_.refreshDebounce->setSingleShot(true);
        topology_.refreshDebounce->setInterval(kTopologyViewDebounceMs);
        connect(topology_.refreshDebounce, &QTimer::timeout, this, &KelpiePanel::refreshTopologyView);
    }

    KelpiePanel::~KelpiePanel()
    {
        stopShell();
        stopUpload(true);
        stopSocksServer();
    }

    StockmanNamespace::KelpieController* KelpiePanel::controller() const
    {
        return context_ ? context_->kelpieController.get() : nullptr;
    }

    StockmanNamespace::KelpieState* KelpiePanel::state() const
    {
        return context_ ? context_->kelpieState.get() : nullptr;
    }

    void KelpiePanel::toastInfo(const QString& message, int timeoutMs)
    {
        if ( context_ && (context_->ui != nullptr) )
        {
            context_->ui->ToastInfo(message, timeoutMs);
        }
        AppendLog(message);
    }

    void KelpiePanel::toastWarn(const QString& message, int timeoutMs)
    {
        if ( context_ && (context_->ui != nullptr) )
        {
            context_->ui->ToastWarn(message, timeoutMs);
        }
        AppendLog(message);
    }

    void KelpiePanel::toastError(const QString& message, int timeoutMs)
    {
        if ( context_ && (context_->ui != nullptr) )
        {
            context_->ui->ToastError(message, timeoutMs);
        }
        AppendLog(message);
    }


    void KelpiePanel::UpdateSnapshot(const kelpieui::v1::Snapshot& snapshot)
    {
        // Snapshot 只做本地视图更新。重 RPC 的刷新改为手动触发或事件驱动，
        // 避免在高频事件下阻塞 UI 主线程导致卡顿。
        populateNodes(snapshot);
        populateStreams(snapshot);
        refreshSsh();
    }

    void KelpiePanel::populateNodes(const kelpieui::v1::Snapshot& snapshot)
    {
        QString previousSelection = currentNodeUuid_;
        const QSignalBlocker blocker(nodesTree_);
        nodesTree_->setUpdatesEnabled(false);
        nodesTree_->clear();
        QHash<QString, QTreeWidgetItem*> itemMap;

        QHash<QString, const kelpieui::v1::SessionInfo*> sessionsByUuid;
        sessionsByUuid.reserve(snapshot.sessions_size());
        for (const auto& sess : snapshot.sessions())
        {
            const QString uuid = QString::fromStdString(sess.target_uuid());
            if (!uuid.isEmpty())
            {
                sessionsByUuid.insert(uuid, &sess);
            }
        }

        QHash<QString, int> streamCounts;
        streamCounts.reserve(snapshot.streams_size());
        for (const auto& stream : snapshot.streams())
        {
            const QString uuid = QString::fromStdString(stream.target_uuid());
            if (!uuid.isEmpty())
            {
                streamCounts[uuid] = streamCounts.value(uuid, 0) + 1;
            }
        }

        for (const auto& node : snapshot.nodes())
        {
            QStringList row;
            const QString uuid = QString::fromStdString(node.uuid());
            const QString alias = QString::fromStdString(node.alias());
            const QString topoStatus = QString::fromStdString(node.status());
            const QString network = QString::fromStdString(node.network());
            const QString workProfile = QString::fromStdString(node.work_profile());
            const QString memo = QString::fromStdString(node.memo());
            QString tagsText;
            if ( node.tags_size() > 0 )
            {
                QStringList tags;
                tags.reserve(node.tags_size());
                for (const auto& t : node.tags())
                {
                    tags << QString::fromStdString(t);
                }
                tagsText = tags.join(QStringLiteral(","));
            }
            QString sleep = QString::fromStdString(node.sleep());
            QString lastSeen;
            QString statusText = topoStatus;
            const auto* session = sessionsByUuid.value(uuid, nullptr);
            if (session != nullptr)
            {
                statusText = sessionStatusText(*session);
                lastSeen = QString::fromStdString(session->last_seen());
                if (sleep.isEmpty() && (session->has_sleep_seconds() || session->has_work_seconds()))
                {
                    const int s = session->has_sleep_seconds() ? session->sleep_seconds() : 0;
                    const int w = session->has_work_seconds() ? session->work_seconds() : 0;
                    sleep = QStringLiteral("%1s/%2s").arg(s).arg(w);
                }
            }
            const int streams = streamCounts.value(uuid, 0);

            row << uuid
                << alias
                << statusText
                << network
                << QString::number(node.depth())
                << workProfile
                << sleep
                << lastSeen
                << memo
                << tagsText
                << QString::number(streams);
            auto* item = new QTreeWidgetItem(nodesTree_, row);
            item->setData(0, Qt::UserRole, QString::fromStdString(node.parent_uuid()));
            item->setData(0, Qt::UserRole + 1, alias);
            item->setToolTip(8, memo);
            item->setToolTip(9, tagsText);
            itemMap.insert(uuid, item);
        }

        QList<QTreeWidgetItem*> toRemove;
        for (auto it = itemMap.begin(); it != itemMap.end(); ++it)
        {
            auto* item = it.value();
            const QString parentUuid = item->data(0, Qt::UserRole).toString();
            if (parentUuid.isEmpty())
            {
                continue;
            }
            auto* parentItem = itemMap.value(parentUuid, nullptr);
            if ((parentItem != nullptr) && parentItem != item)
            {
                toRemove.append(item);
                parentItem->addChild(item);
            }
        }
        for (auto* item : toRemove)
        {
            nodesTree_->takeTopLevelItem(nodesTree_->indexOfTopLevelItem(item));
        }
        nodesTree_->expandAll();
        if ( !previousSelection.isEmpty() )
        {
            if ( auto it = itemMap.find(previousSelection); it != itemMap.end() )
            {
                nodesTree_->setCurrentItem(it.value());
            }
        }
        nodesTree_->setUpdatesEnabled(true);
        updateSelectedNode();
    }

    void KelpiePanel::populateStreams(const kelpieui::v1::Snapshot& snapshot)
    {
        streams_.table->setRowCount(snapshot.streams_size());
        for (int i = 0; i < snapshot.streams_size(); ++i)
        {
            const auto& stream = snapshot.streams(i);
            streams_.table->setItem(i, 0, new QTableWidgetItem(QString::number(stream.stream_id())));
            streams_.table->setItem(i, 1, new QTableWidgetItem(QString::fromStdString(stream.target_uuid())));
            streams_.table->setItem(i, 2, new QTableWidgetItem(QString::fromStdString(stream.kind())));
            streams_.table->setItem(i, 3, new QTableWidgetItem(QString::number(stream.pending())));
            streams_.table->setItem(i, 4, new QTableWidgetItem(QString::number(stream.inflight())));
            streams_.table->setItem(i, 5, new QTableWidgetItem(QString::number(stream.window())));
        }
    }

    void KelpiePanel::AppendLog(const QString& message)
    {
        workspace_.logView->appendPlainText(message);
    }

    void KelpiePanel::FocusNodes()
    {
        if ( (workspaceTabs_ != nullptr) && (overviewPage_ != nullptr) )
        {
            workspaceTabs_->setCurrentWidget(overviewPage_);
        }
        nodesTree_->setFocus();
        if ( (stateTabs_ != nullptr) && (sessionsPage_ != nullptr) )
        {
            stateTabs_->setCurrentWidget(sessionsPage_);
        }
    }

    void KelpiePanel::FocusStreams()
    {
        if ( (workspaceTabs_ != nullptr) && (streams_.page != nullptr) )
        {
            workspaceTabs_->setCurrentWidget(streams_.page);
        }
        streams_.table->setFocus();
    }

    void KelpiePanel::FocusLog()
    {
        if ( (workspaceTabs_ != nullptr) && (workspace_.consolePage != nullptr) )
        {
            workspaceTabs_->setCurrentWidget(workspace_.consolePage);
        }
        workspace_.logView->setFocus();
    }

    void KelpiePanel::FocusChat()
    {
        if ( (workspaceTabs_ != nullptr) && (chatPage_ != nullptr) )
        {
            workspaceTabs_->setCurrentWidget(chatPage_);
        }
        if ( chatPage_ != nullptr && chatPage_->input != nullptr )
        {
            chatPage_->input->setFocus();
        }
    }

    void KelpiePanel::FocusLoot()
    {
        if ( (workspaceTabs_ != nullptr) && (lootPage_ != nullptr) )
        {
            workspaceTabs_->setCurrentWidget(lootPage_);
        }
        if ( lootPage_ != nullptr && lootPage_->table != nullptr )
        {
            lootPage_->table->setFocus();
        }
    }

    void KelpiePanel::FocusPivotListeners()
    {
        if ( (workspaceTabs_ != nullptr) && (overviewPage_ != nullptr) )
        {
            workspaceTabs_->setCurrentWidget(overviewPage_);
        }
        if ( (stateTabs_ != nullptr) && (listenersPage_ != nullptr) )
        {
            stateTabs_->setCurrentWidget(listenersPage_);
        }
        if ( pivotListenersTable_ != nullptr )
        {
            pivotListenersTable_->setFocus();
        }
    }

    void KelpiePanel::FocusControllerListeners()
    {
        if ( (workspaceTabs_ != nullptr) && (overviewPage_ != nullptr) )
        {
            workspaceTabs_->setCurrentWidget(overviewPage_);
        }
        if ( (stateTabs_ != nullptr) && (listenersPage_ != nullptr) )
        {
            stateTabs_->setCurrentWidget(listenersPage_);
        }
        if ( controllerListenersTable_ != nullptr )
        {
            controllerListenersTable_->setFocus();
        }
    }

    void KelpiePanel::FocusProxies()
    {
        if ( (workspaceTabs_ != nullptr) && (overviewPage_ != nullptr) )
        {
            workspaceTabs_->setCurrentWidget(overviewPage_);
        }
        proxyTable_->setFocus();
        if ( (stateTabs_ != nullptr) && (pivotingPage_ != nullptr) )
        {
            stateTabs_->setCurrentWidget(pivotingPage_);
        }
    }

    void KelpiePanel::FocusSupplemental()
    {
        if ( (workspaceTabs_ != nullptr) && (overviewPage_ != nullptr) )
        {
            workspaceTabs_->setCurrentWidget(overviewPage_);
        }
        supplementalTable_->setFocus();
        if ( (stateTabs_ != nullptr) && (resiliencePage_ != nullptr) )
        {
            stateTabs_->setCurrentWidget(resiliencePage_);
        }
    }

    void KelpiePanel::setNodeScopedActionsEnabled(bool enabled)
    {
        if ( nodeCommandInput_ != nullptr ) { nodeCommandInput_->setEnabled(enabled);
}
        if ( nodeCommandButton_ != nullptr ) { nodeCommandButton_->setEnabled(enabled);
}
        if ( setNodeNetworkButton_ != nullptr ) { setNodeNetworkButton_->setEnabled(enabled);
}
        if ( shellPage_ != nullptr && shellPage_->openButton != nullptr ) { shellPage_->openButton->setEnabled(enabled);
}
        if ( enqueueDtnButton_ != nullptr ) { enqueueDtnButton_->setEnabled(enabled);
}
        if ( startForwardButton_ != nullptr ) { startForwardButton_->setEnabled(enabled);
}
        if ( startBackwardButton_ != nullptr ) { startBackwardButton_->setEnabled(enabled);
}
        if ( taskingPage_ != nullptr && taskingPage_->refreshSleepButton != nullptr ) { taskingPage_->refreshSleepButton->setEnabled(enabled);
}
        if ( taskingPage_ != nullptr && taskingPage_->updateSleepButton != nullptr ) { taskingPage_->updateSleepButton->setEnabled(enabled);
}
        if ( taskingPage_ != nullptr && taskingPage_->startDialButton != nullptr ) { taskingPage_->startDialButton->setEnabled(enabled);
}
        if ( taskingPage_ != nullptr && taskingPage_->startSshSessionButton != nullptr ) { taskingPage_->startSshSessionButton->setEnabled(enabled);
}
        if ( taskingPage_ != nullptr && taskingPage_->startSshTunnelButton != nullptr ) { taskingPage_->startSshTunnelButton->setEnabled(enabled);
}
        if ( markAliveButton_ != nullptr ) { markAliveButton_->setEnabled(enabled);
}
        if ( markDeadButton_ != nullptr ) { markDeadButton_->setEnabled(enabled);
}
        if ( nodeStatusButton_ != nullptr ) { nodeStatusButton_->setEnabled(enabled);
}
        if ( repairSessionButton_ != nullptr ) { repairSessionButton_->setEnabled(enabled);
}
        if ( reconnectSessionButton_ != nullptr ) { reconnectSessionButton_->setEnabled(enabled);
}
        if ( terminateSessionButton_ != nullptr ) { terminateSessionButton_->setEnabled(enabled);
}
        if ( shutdownNodeButton_ != nullptr ) { shutdownNodeButton_->setEnabled(enabled);
}
        if ( shellPage_ != nullptr && shellPage_->browsePathInput != nullptr ) { shellPage_->browsePathInput->setEnabled(enabled);
}
        if ( shellPage_ != nullptr && shellPage_->browseGoButton != nullptr ) { shellPage_->browseGoButton->setEnabled(enabled);
}
        if ( shellPage_ != nullptr && shellPage_->browseRefreshButton != nullptr ) { shellPage_->browseRefreshButton->setEnabled(enabled);
}
        if ( shellPage_ != nullptr && shellPage_->browseTable != nullptr ) { shellPage_->browseTable->setEnabled(enabled);
}
        if ( shellPage_ != nullptr && shellPage_->downloadButton != nullptr ) { shellPage_->downloadButton->setEnabled(enabled);
}
        if ( shellPage_ != nullptr && shellPage_->startUploadButton != nullptr ) { shellPage_->startUploadButton->setEnabled(enabled && !uploadActive_.load());
}
        if ( shellPage_ != nullptr && shellPage_->startSocksButton != nullptr ) { shellPage_->startSocksButton->setEnabled(enabled && socksServer_ == nullptr);
}
    }

    void KelpiePanel::refreshStatePage(QWidget* page)
    {
        if ( page == nullptr )
        {
            return;
        }
        if ( page == pivotingPage_ )
        {
            refreshProxies();
            return;
        }
        if ( page == listenersPage_ )
        {
            refreshListeners();
            return;
        }
        if ( page == topology_.page )
        {
            refreshTopology();
            return;
        }
        if ( page == sessionListPage_ )
        {
            refreshSessionList();
            return;
        }
        if ( page == infrastructurePage_ )
        {
            refreshNetworks();
            return;
        }
        if ( page == transportPage_ )
        {
            refreshDtn();
            return;
        }
        if ( page == resiliencePage_ )
        {
            refreshSupplemental();
            return;
        }
        if ( page == strategyPage_ )
        {
            refreshMetrics();
            return;
        }
        if ( page == auditTrailPage_ )
        {
            refreshAudit();
            return;
        }
        if ( page == hostIntelPage_ )
        {
            refreshMetrics();
            refreshStreamDiagnostics();
            return;
        }
        if ( page == recoveryPage_ )
        {
            refreshRepairs();
            return;
        }
        if ( page == taskingPage_ )
        {
            refreshSleep();
            refreshDials();
            refreshSsh();
            return;
        }
        if ( page == sessionsPage_ )
        {
            refreshDtn();
        }
    }

    void KelpiePanel::refreshWorkspacePage(QWidget* page)
    {
        if ( page == nullptr )
        {
            return;
        }
        if ( page == streams_.page )
        {
            refreshStreamDiagnostics();
            return;
        }
        if ( page == chatPage_ )
        {
            refreshChat();
            return;
        }
        if ( page == lootPage_ )
        {
            refreshLoot();
            return;
        }
        if ( page == shellPage_ )
        {
            if ( !currentNodeUuid_.isEmpty() )
            {
                const QString browsePath = remoteBrowsePathByNode_.value(currentNodeUuid_);
                refreshRemoteFiles(browsePath);
            }
            if ( (shellPage_ != nullptr) && (shellPage_->input != nullptr) && shellPage_->input->isEnabled() )
            {
                shellPage_->input->setFocus();
            }
        }
    }

    void KelpiePanel::streamPing()
    {
        auto* ctrl = controller();
        if ( ctrl == nullptr )
        {
            toastWarn(tr("gRPC client not connected"));
            return;
        }
        if ( currentNodeUuid_.isEmpty() )
        {
            toastWarn(tr("Select a node first"));
            return;
        }
        const int count = (streams_.pingCountInput != nullptr) ? streams_.pingCountInput->text().toInt() : 0;
        const int size = (streams_.pingSizeInput != nullptr) ? streams_.pingSizeInput->text().toInt() : 0;
        setWidgetsEnabled({streams_.pingButton, streams_.pingCountInput, streams_.pingSizeInput}, false);
        toastInfo(tr("Pinging streams..."));

        const QString targetUuid = currentNodeUuid_;
        const uint64_t epoch = ctrl->ConnectionEpoch();
        struct Result {
            uint64_t epoch{0};
            QString targetUuid;
            bool ok{false};
            QString error;
        };
        runAsync<Result>(
            this,
            [ctrl, epoch, targetUuid, count, size]() {
                Result res;
                res.epoch = epoch;
                res.targetUuid = targetUuid;
                QString error;
                res.ok = ctrl->StreamPing(targetUuid, count, size, error);
                res.error = error;
                return res;
            },
            [this](const Result& res) {
                setWidgetsEnabled({streams_.pingButton, streams_.pingCountInput, streams_.pingSizeInput}, true);
                auto* ctrl = controller();
                if ( ctrl == nullptr || ctrl->ConnectionEpoch() != res.epoch )
                {
                    return;
                }
                if ( !res.ok )
                {
                    toastError(tr("Stream ping failed: %1").arg(res.error));
                    return;
                }
                toastInfo(tr("Stream ping sent"));
            });
    }

    void KelpiePanel::refreshNodeScopedData()
    {
        if ( currentNodeUuid_.isEmpty() )
        {
            return;
        }
        refreshDtn();
        refreshSleep();
        refreshDials();
        refreshDiagnosticsForNode(currentNodeUuid_);
        refreshSsh();
        refreshRemoteFiles(remoteBrowsePathByNode_.value(currentNodeUuid_));
    }

    void KelpiePanel::scheduleDialRefresh()
    {
        if ( dialRefreshCooldown_ == nullptr )
        {
            refreshDials();
            return;
        }
        if ( dialRefreshCooldown_->isActive() )
        {
            return;
        }
        refreshDials();
        dialRefreshCooldown_->start();
    }

    void KelpiePanel::scheduleStreamRefresh()
    {
        const bool taskingVisible = (stateTabs_ != nullptr) && (stateTabs_->currentWidget() == taskingPage_);
        const bool streamDiagnosticsVisible =
            ((workspaceTabs_ != nullptr) && (workspaceTabs_->currentWidget() == streams_.page)) ||
            ((stateTabs_ != nullptr) && (stateTabs_->currentWidget() == hostIntelPage_));
        if ( !taskingVisible && !streamDiagnosticsVisible )
        {
            return;
        }
        if ( streamRefreshDebounce_ == nullptr )
        {
            if ( taskingVisible )
            {
                refreshSsh();
            }
            if ( streamDiagnosticsVisible )
            {
                refreshStreamDiagnostics();
            }
            return;
        }
        streamRefreshDebounce_->start();
    }

    void KelpiePanel::updateSelectedNode()
    {
        QString previous = currentNodeUuid_;
        auto items = nodesTree_->selectedItems();
        if ( items.isEmpty() )
        {
            currentNodeUuid_.clear();
            selectedNodeLabel_->setText(tr("Selected node: <none>"));
            setTopologyHighlightNode(QString());
            setNodeScopedActionsEnabled(false);
            stopSocksServer();
            if ( shellPage_ != nullptr )
            {
                shellPage_->pendingTarget.clear();
                shellPage_->pendingLine.clear();
                shellPage_->remoteRootPath.clear();
                shellPage_->remoteResolvedPath.clear();
                shellPage_->remoteParentPath.clear();
                if ( shellPage_->browsePathInput != nullptr )
                {
                    shellPage_->browsePathInput->clear();
                }
                if ( shellPage_->browseStatusLabel != nullptr )
                {
                    shellPage_->browseStatusLabel->setText(tr("Files: idle"));
                }
                if ( shellPage_->downloadStatusLabel != nullptr )
                {
                    shellPage_->downloadStatusLabel->setText(tr("Download: idle"));
                }
                if ( shellPage_->browseTable != nullptr )
                {
                    shellPage_->browseTable->setRowCount(0);
                }
            }
            if ( nodeMemoInput_ != nullptr ) { nodeMemoInput_->setEnabled(false);
}
            if ( updateNodeMemoButton_ != nullptr ) { updateNodeMemoButton_->setEnabled(false);
}
            return;
        }
        auto* item = items.first();
        currentNodeUuid_ = item->text(0);
        const QString alias = item->text(1);
        QString display = currentNodeUuid_;
        if ( !alias.isEmpty() )
        {
            display = QStringLiteral("%1 (%2)").arg(alias, currentNodeUuid_);
        }
        selectedNodeLabel_->setText(tr("Selected node: %1").arg(display));
        setTopologyHighlightNode(currentNodeUuid_);
        setNodeScopedActionsEnabled(true);
        if ( nodeMemoInput_ != nullptr )
        {
            nodeMemoInput_->setEnabled(true);
            // Memo column index: 8 (UUID,Alias,Status,Network,Depth,Work,Sleep,LastSeen,Memo,Tags,Streams)
            nodeMemoInput_->setText(item->text(8));
        }
        if ( updateNodeMemoButton_ != nullptr ) { updateNodeMemoButton_->setEnabled(true);
}
        if ( previous != currentNodeUuid_ )
        {
            stopSocksServer();
            stopShell();
            if ( shellPage_ != nullptr )
            {
                shellPage_->pendingTarget.clear();
                shellPage_->pendingLine.clear();
                shellPage_->remoteRootPath.clear();
                shellPage_->remoteResolvedPath.clear();
                shellPage_->remoteParentPath.clear();
                if ( shellPage_->browsePathInput != nullptr )
                {
                    shellPage_->browsePathInput->setText(remoteBrowsePathByNode_.value(currentNodeUuid_));
                }
                if ( shellPage_->browseTable != nullptr )
                {
                    shellPage_->browseTable->setRowCount(0);
                }
            }
            refreshNodeScopedData();
        }
        updateRemoteFileSelection();
    }

    void KelpiePanel::updateNodeMemo()
    {
        if ( currentNodeUuid_.isEmpty() )
        {
            toastWarn(tr("Select a node first"));
            return;
        }
        auto* ctrl = controller();
        if ( ctrl == nullptr )
        {
            toastWarn(tr("gRPC client not connected"));
            return;
        }
        const QString targetUuid = currentNodeUuid_;
        const QString memo = (nodeMemoInput_ != nullptr) ? nodeMemoInput_->text() : QString();
        setWidgetsEnabled({nodeMemoInput_, updateNodeMemoButton_}, false);
        toastInfo(tr("Updating memo for %1...").arg(targetUuid));

        const uint64_t epoch = ctrl->ConnectionEpoch();
        struct Result {
            uint64_t epoch{0};
            QString targetUuid;
            QString memo;
            bool ok{false};
            QString error;
        };

        runAsync<Result>(
            this,
            [ctrl, epoch, targetUuid, memo]() {
                Result res;
                res.epoch = epoch;
                res.targetUuid = targetUuid;
                res.memo = memo;
                QString error;
                res.ok = ctrl->UpdateNodeMemo(targetUuid, memo, error);
                res.error = error;
                return res;
            },
            [this](const Result& res) {
                setWidgetsEnabled({nodeMemoInput_, updateNodeMemoButton_}, true);
                auto* ctrl = controller();
                if ( ctrl == nullptr || ctrl->ConnectionEpoch() != res.epoch )
                {
                    return;
                }
                if ( !res.ok )
                {
                    toastError(tr("Update memo failed: %1").arg(res.error));
                    return;
                }
                toastInfo(tr("Updated memo for %1").arg(res.targetUuid));
                if ( res.targetUuid == currentNodeUuid_ )
                {
                    if ( auto* item = nodesTree_ ? nodesTree_->currentItem() : nullptr )
                    {
                        item->setText(8, res.memo);
                        item->setToolTip(8, res.memo);
                    }
                }
                ctrl->RequestSnapshotRefresh();
            });
    }

	    void KelpiePanel::sendNodeCommand()
	    {
	        if ( currentNodeUuid_.isEmpty() )
	        {
	            toastWarn(tr("Select a node first"));
	            return;
	        }
	        const QString cmd = (nodeCommandInput_ != nullptr) ? nodeCommandInput_->text().trimmed() : QString();
	        if ( cmd.isEmpty() )
	        {
	            return;
	        }
	        if ( (workspaceTabs_ != nullptr) && (shellPage_ != nullptr) )
	        {
	            workspaceTabs_->setCurrentWidget(shellPage_);
	        }
	        if ( shellPage_ == nullptr || !shellPage_->stream )
	        {
	            if ( shellPage_ != nullptr )
	            {
	                shellPage_->pendingTarget = currentNodeUuid_;
	                shellPage_->pendingLine = cmd;
	                if ( !shellPage_->pendingLine.endsWith('\n') )
	                {
	                    shellPage_->pendingLine.append('\n');
	                }
	            }
	            startShell(); // async; will send pending line when connected
	            if ( nodeCommandInput_ != nullptr ) { nodeCommandInput_->clear();
}
	            return;
	        }
	        QString line = cmd;
	        if ( !line.endsWith('\n') )
	        {
	            line.append('\n');
	        }
	        shellPage_->stream->SendData(line.toUtf8());
	        AppendLog(tr("[%1] >> %2").arg(currentNodeUuid_, cmd));
	        nodeCommandInput_->clear();
	    }

    void KelpiePanel::closeSelectedStream()
    {
        auto* ctrl = controller();
        if ( ctrl == nullptr )
        {
            toastWarn(tr("gRPC client not connected"));
            return;
        }
        if ( streams_.table == nullptr )
        {
            return;
        }
        const int row = streams_.table->currentRow();
        if ( row < 0 )
        {
            toastWarn(tr("Select a stream row first"));
            return;
        }
        auto* idItem = streams_.table->item(row, 0);
        if ( idItem == nullptr )
        {
            return;
        }
        bool ok = false;
        const uint32_t streamId = idItem->text().trimmed().toUInt(&ok);
        if ( !ok || streamId == 0 )
        {
            toastWarn(tr("Invalid stream id"));
            return;
        }
        const QString reason = (streams_.closeReasonInput != nullptr) ? streams_.closeReasonInput->text().trimmed() : QString();
        setWidgetsEnabled({streams_.closeButton, streams_.table, streams_.closeReasonInput}, false);
        toastInfo(tr("Closing stream %1...").arg(streamId));

        const uint64_t epoch = ctrl->ConnectionEpoch();
        struct Result {
            uint64_t epoch{0};
            uint32_t streamId{0};
            bool ok{false};
            QString error;
        };

        runAsync<Result>(
            this,
            [ctrl, epoch, streamId, reason]() {
                Result res;
                res.epoch = epoch;
                res.streamId = streamId;
                QString error;
                res.ok = ctrl->CloseStream(streamId, reason, error);
                res.error = error;
                return res;
            },
            [this](const Result& res) {
                setWidgetsEnabled({streams_.closeButton, streams_.table, streams_.closeReasonInput}, true);
                auto* ctrl = controller();
                if ( ctrl == nullptr || ctrl->ConnectionEpoch() != res.epoch )
                {
                    return;
                }
                if ( !res.ok )
                {
                    toastError(tr("Close stream failed: %1").arg(res.error));
                    return;
                }
                toastInfo(tr("Close stream requested: %1").arg(res.streamId));
                ctrl->RequestSnapshotRefresh();
            });
    }


    void KelpiePanel::ProcessEvent(const kelpieui::v1::UiEvent& event)
    {
        if ( event.has_proxy_event() )
        {
            const auto& proxyEvt = event.proxy_event();
            if ( proxyEvt.kind() == kelpieui::v1::ProxyEvent_Kind_PROXY_EVENT_STOPPED )
            {
                refreshProxies();
            }
            else
            {
                updateProxyRow(proxyEvt);
            }
        }
        if ( event.has_listener_event() )
        {
            refreshListeners();
        }
        if ( event.has_supplemental_event() )
        {
            appendSupplementalEvent(event.supplemental_event());
            if ( supplementalSummary_ != nullptr )
            {
                supplementalSummary_->setText(tr("Supplemental events: latest seq %1")
                                              .arg(event.supplemental_event().seq()));
            }
        }
        if ( event.has_dial_event() )
        {
            scheduleDialRefresh();
        }
        if ( event.has_stream_event() )
        {
            // Stream events can arrive in bursts; coalesce UI refreshes to avoid
            // rebuilding diagnostics tables on every single event.
            scheduleStreamRefresh();
        }
        if ( event.has_chat_event() )
        {
            appendChatMessage(event.chat_event().message());
        }
        if ( event.has_audit_event() )
        {
            appendAuditEntry(event.audit_event().entry());
        }
        if ( event.has_loot_event() &&
             event.loot_event().kind() == kelpieui::v1::LootEvent_Kind_LOOT_EVENT_ADDED )
        {
            appendLootItem(event.loot_event().item());
        }
        if ( event.has_sleep_event() )
        {
            const QString target = QString::fromStdString(event.sleep_event().target_uuid());
            if ( !target.isEmpty() && target == currentNodeUuid_ )
            {
                refreshSleep();
            }
        }
    }

}
