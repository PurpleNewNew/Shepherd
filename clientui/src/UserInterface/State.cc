#include <UserInterface/KelpiePanel.hpp>

#include <QHeaderView>
#include <QGraphicsScene>
#include <QGraphicsView>
#include <QPainter>
#include <QSplitter>
#include <QWheelEvent>

#include <Util/UiConstants.hpp>

namespace
{
class ZoomableGraphicsView final : public QGraphicsView
{
public:
    explicit ZoomableGraphicsView(QWidget* parent = nullptr)
        : QGraphicsView(parent)
    {
        setRenderHint(QPainter::Antialiasing, true);
        setDragMode(QGraphicsView::ScrollHandDrag);
        setViewportUpdateMode(QGraphicsView::SmartViewportUpdate);
        setTransformationAnchor(QGraphicsView::AnchorUnderMouse);
        setResizeAnchor(QGraphicsView::AnchorUnderMouse);
    }

protected:
    void wheelEvent(QWheelEvent* event) override
    {
        if ( event == nullptr )
        {
            return;
        }
        constexpr double kZoomIn = 1.15;
        constexpr double kZoomOut = 1.0 / kZoomIn;
        if ( event->angleDelta().y() > 0 )
        {
            scale(kZoomIn, kZoomIn);
        }
        else if ( event->angleDelta().y() < 0 )
        {
            scale(kZoomOut, kZoomOut);
        }
        event->accept();
    }
};
}

namespace StockmanNamespace::UserInterface
{
    void KelpiePanel::buildOverviewWorkspaceTab()
    {
        overviewPage_ = new QWidget(this);
        auto* overviewLayout = new QVBoxLayout(overviewPage_);
        overviewLayout->setContentsMargins(0, 0, 0, 0);

        stateTabs_ = new QTabWidget(overviewPage_);
        stateTabs_->setDocumentMode(true);

        buildSessionsStateTab();
        buildTopologyStateTab();
        buildSessionListStateTab();
        buildPivotingStateTab();
        buildListenersStateTab();
        buildInfrastructureStateTab();
        buildTransportStateTab();
        buildResilienceStateTab();
        buildStrategyStateTab();
        buildAuditTrailStateTab();
        buildHostIntelStateTab();
        buildRecoveryStateTab();
        buildTaskingStateTab();

        selectedNodeLabel_ = new QLabel(tr("Selected node: <none>"), overviewPage_);
        nodeCommandInput_ = new QLineEdit(overviewPage_);
        nodeCommandInput_->setPlaceholderText(tr("Shell command for selected node (e.g., whoami)"));
        nodeCommandButton_ = new QPushButton(tr("Send to node"), overviewPage_);
        nodeCommandInput_->setEnabled(false);
        nodeCommandButton_->setEnabled(false);

        auto* nodeCommandLayout = new QHBoxLayout();
        nodeCommandLayout->addWidget(nodeCommandInput_);
        nodeCommandLayout->addWidget(nodeCommandButton_);

        overviewLayout->addWidget(stateTabs_, 1);
        overviewLayout->addWidget(selectedNodeLabel_);
        overviewLayout->addLayout(nodeCommandLayout);

        workspaceTabs_->addTab(overviewPage_, tr("Targets"));
    }

    void KelpiePanel::buildSessionsStateTab()
    {
        sessionsPage_ = new QWidget(overviewPage_);
        auto* nodesLayout = new QVBoxLayout(sessionsPage_);
        nodesLayout->setContentsMargins(0, 0, 0, 0);

        nodesTree_ = new QTreeWidget(sessionsPage_);
        nodesTree_->setColumnCount(11);
        nodesTree_->setHeaderLabels({tr("UUID"),
                                     tr("Alias"),
                                     tr("Status"),
                                     tr("Network"),
                                     tr("Depth"),
                                     tr("Work Profile"),
                                     tr("Sleep"),
                                     tr("Last Seen"),
                                     tr("Memo"),
                                     tr("Tags"),
                                     tr("Streams")});
        nodesTree_->setUniformRowHeights(true);
        nodesTree_->setAlternatingRowColors(true);
        nodesLayout->addWidget(nodesTree_, 1);

        auto* memoLayout = new QHBoxLayout();
        memoLayout->setContentsMargins(0, 0, 0, 0);
        memoLayout->addWidget(new QLabel(tr("Memo:"), sessionsPage_));
        nodeMemoInput_ = new QLineEdit(sessionsPage_);
        nodeMemoInput_->setPlaceholderText(tr("Operator memo for selected node"));
        updateNodeMemoButton_ = new QPushButton(tr("Update Memo"), sessionsPage_);
        updateNodeMemoButton_->setEnabled(false);
        memoLayout->addWidget(nodeMemoInput_, 1);
        memoLayout->addWidget(updateNodeMemoButton_);
        nodesLayout->addLayout(memoLayout);

        auto* sessionActionLayout = new QHBoxLayout();
        sessionActionLayout->setContentsMargins(0, 0, 0, 0);
        sessionReasonInput_ = new QLineEdit(sessionsPage_);
        sessionReasonInput_->setPlaceholderText(tr("Session action reason (optional)"));
        repairForceCheck_ = new QCheckBox(tr("Force repair"), sessionsPage_);
        markAliveButton_ = new QPushButton(tr("Mark Alive"), sessionsPage_);
        markDeadButton_ = new QPushButton(tr("Mark Dead"), sessionsPage_);
        nodeStatusButton_ = new QPushButton(tr("Node Status"), sessionsPage_);
        repairSessionButton_ = new QPushButton(tr("Repair"), sessionsPage_);
        reconnectSessionButton_ = new QPushButton(tr("Reconnect"), sessionsPage_);
        terminateSessionButton_ = new QPushButton(tr("Terminate"), sessionsPage_);
        shutdownNodeButton_ = new QPushButton(tr("Shutdown Node"), sessionsPage_);
        sessionActionLayout->addWidget(sessionReasonInput_, 1);
        sessionActionLayout->addWidget(repairForceCheck_);
        sessionActionLayout->addWidget(markAliveButton_);
        sessionActionLayout->addWidget(markDeadButton_);
        sessionActionLayout->addWidget(nodeStatusButton_);
        sessionActionLayout->addWidget(repairSessionButton_);
        sessionActionLayout->addWidget(reconnectSessionButton_);
        sessionActionLayout->addWidget(terminateSessionButton_);
        sessionActionLayout->addWidget(shutdownNodeButton_);
        nodesLayout->addLayout(sessionActionLayout);

        stateTabs_->addTab(sessionsPage_, tr("Sessions"));
    }

    void KelpiePanel::buildTopologyStateTab()
    {
        topology_.page = new QWidget(overviewPage_);
        auto* topologyLayout = new QVBoxLayout(topology_.page);
        topologyLayout->setContentsMargins(0, 0, 0, 0);

        auto* topologyControls = new QHBoxLayout();
        topologyControls->setContentsMargins(0, 0, 0, 0);
        topologyControls->addWidget(new QLabel(tr("Target:"), topology_.page));
        topology_.targetInput = new QLineEdit(topology_.page);
        topology_.targetInput->setPlaceholderText(tr("Target UUID (optional)"));
        topologyControls->addWidget(topology_.targetInput, 1);
        topologyControls->addWidget(new QLabel(tr("Network:"), topology_.page));
        topology_.networkInput = new QLineEdit(topology_.page);
        topology_.networkInput->setPlaceholderText(tr("Network ID (optional)"));
        topologyControls->addWidget(topology_.networkInput);
        topology_.refreshButton = new QPushButton(tr("Refresh"), topology_.page);
        topologyControls->addWidget(topology_.refreshButton);
        topology_.fitButton = new QPushButton(tr("Fit"), topology_.page);
        topologyControls->addWidget(topology_.fitButton);
        topologyLayout->addLayout(topologyControls);

        auto* topologyViewControls = new QHBoxLayout();
        topologyViewControls->setContentsMargins(0, 0, 0, 0);
        topologyViewControls->addWidget(new QLabel(tr("Layout:"), topology_.page));
        topology_.layoutBox = new QComboBox(topology_.page);
        topology_.layoutBox->addItem(tr("Tree"), QStringLiteral("tree"));
        topology_.layoutBox->addItem(tr("Force"), QStringLiteral("force"));
        topologyViewControls->addWidget(topology_.layoutBox);
        topology_.showSupplementalCheck = new QCheckBox(tr("Show supplemental"), topology_.page);
        topology_.showSupplementalCheck->setChecked(true);
        topologyViewControls->addWidget(topology_.showSupplementalCheck);
        topology_.filterInput = new QLineEdit(topology_.page);
        topology_.filterInput->setPlaceholderText(tr("Filter by uuid/alias/status"));
        topologyViewControls->addWidget(topology_.filterInput, 1);
        topology_.locateInput = new QLineEdit(topology_.page);
        topology_.locateInput->setPlaceholderText(tr("Locate node"));
        topology_.locateButton = new QPushButton(tr("Locate"), topology_.page);
        topologyViewControls->addWidget(topology_.locateInput);
        topologyViewControls->addWidget(topology_.locateButton);
        topologyLayout->addLayout(topologyViewControls);

        topology_.statusLabel = new QLabel(tr("Topology: -"), topology_.page);
        topologyLayout->addWidget(topology_.statusLabel);
        topology_.legendLabel = new QLabel(
            tr("Legend: node color = status (green online, yellow degraded, red failed, gray offline, blue unknown); "
               "edge style = solid primary / dashed supplemental."),
            topology_.page);
        topology_.legendLabel->setWordWrap(true);
        topologyLayout->addWidget(topology_.legendLabel);

        topology_.graphView = new ZoomableGraphicsView(topology_.page);
        topology_.graphView->setObjectName(QStringLiteral("TopologyGraphView"));
        topology_.scene = new QGraphicsScene(topology_.graphView);
        topology_.graphView->setScene(topology_.scene);
        topology_.graphView->setBackgroundBrush(QBrush(QColor(20, 24, 28)));

        topology_.edgesTable = new QTableWidget(topology_.page);
        topology_.edgesTable->setColumnCount(3);
        topology_.edgesTable->setHorizontalHeaderLabels({tr("Parent"), tr("Child"), tr("Supplemental")});
        topology_.edgesTable->horizontalHeader()->setStretchLastSection(true);
        topology_.edgesTable->setEditTriggers(QAbstractItemView::NoEditTriggers);

        auto* topoSplit = new QSplitter(Qt::Vertical, topology_.page);
        topoSplit->setObjectName(QStringLiteral("TopologySplit"));
        topoSplit->addWidget(topology_.graphView);
        topoSplit->addWidget(topology_.edgesTable);
        topoSplit->setStretchFactor(0, 3);
        topoSplit->setStretchFactor(1, 1);
        topologyLayout->addWidget(topoSplit, 1);

        stateTabs_->addTab(topology_.page, tr("Topology"));
    }

    void KelpiePanel::buildSessionListStateTab()
    {
        sessionListPage_ = new QWidget(overviewPage_);
        auto* sessionListLayout = new QVBoxLayout(sessionListPage_);
        sessionListLayout->setContentsMargins(0, 0, 0, 0);

        auto* sessionListControls = new QHBoxLayout();
        sessionListControls->setContentsMargins(0, 0, 0, 0);
        sessionListTargetsInput_ = new QLineEdit(sessionListPage_);
        sessionListTargetsInput_->setPlaceholderText(tr("Target UUIDs (comma separated, optional)"));
        sessionStatusBox_ = new QComboBox(sessionListPage_);
        sessionStatusBox_->addItem(tr("All"), kelpieui::v1::SESSION_STATUS_UNSPECIFIED);
        sessionStatusBox_->addItem(tr("Active"), kelpieui::v1::SESSION_STATUS_ACTIVE);
        sessionStatusBox_->addItem(tr("Degraded"), kelpieui::v1::SESSION_STATUS_DEGRADED);
        sessionStatusBox_->addItem(tr("Failed"), kelpieui::v1::SESSION_STATUS_FAILED);
        sessionStatusBox_->addItem(tr("Marked dead"), kelpieui::v1::SESSION_STATUS_MARKED_DEAD);
        sessionStatusBox_->addItem(tr("Repairing"), kelpieui::v1::SESSION_STATUS_REPAIRING);
        sessionIncludeInactiveCheck_ = new QCheckBox(tr("Include inactive"), sessionListPage_);
        refreshSessionListButton_ = new QPushButton(tr("Refresh"), sessionListPage_);
        sessionListControls->addWidget(sessionListTargetsInput_, 1);
        sessionListControls->addWidget(sessionStatusBox_);
        sessionListControls->addWidget(sessionIncludeInactiveCheck_);
        sessionListControls->addWidget(refreshSessionListButton_);
        sessionListLayout->addLayout(sessionListControls);

        sessionListTable_ = new QTableWidget(sessionListPage_);
        sessionListTable_->setColumnCount(11);
        sessionListTable_->setHorizontalHeaderLabels({tr("Target"),
                                                      tr("Status"),
                                                      tr("Active"),
                                                      tr("Connected"),
                                                      tr("Remote"),
                                                      tr("Upstream"),
                                                      tr("Downstream"),
                                                      tr("Network"),
                                                      tr("Last Seen"),
                                                      tr("Last Error"),
                                                      tr("Sleep Profile")});
        sessionListTable_->horizontalHeader()->setStretchLastSection(true);
        sessionListTable_->setEditTriggers(QAbstractItemView::NoEditTriggers);
        sessionListLayout->addWidget(sessionListTable_, 1);

        stateTabs_->addTab(sessionListPage_, tr("Session List"));
    }

    void KelpiePanel::buildPivotingStateTab()
    {
        pivotingPage_ = new QWidget(overviewPage_);
        auto* proxyLayout = new QVBoxLayout(pivotingPage_);
        proxyLayout->setContentsMargins(0, 0, 0, 0);

        proxyTable_ = new QTableWidget(pivotingPage_);
        proxyTable_->setColumnCount(5);
        proxyTable_->setHorizontalHeaderLabels(
            {tr(StockmanNamespace::UiConstants::kProxyColId),
             tr(StockmanNamespace::UiConstants::kProxyColTarget),
             tr(StockmanNamespace::UiConstants::kProxyColKind),
             tr(StockmanNamespace::UiConstants::kProxyColBind),
             tr("Remote")});
        proxyTable_->horizontalHeader()->setStretchLastSection(true);
        proxyTable_->setEditTriggers(QAbstractItemView::NoEditTriggers);
        proxyLayout->addWidget(proxyTable_, 1);

        auto* proxyControlLayout = new QHBoxLayout();
        forwardBindInput_ = new QLineEdit(pivotingPage_);
        forwardBindInput_->setPlaceholderText(tr("Forward bind (e.g., 127.0.0.1:9001)"));
        forwardRemoteInput_ = new QLineEdit(pivotingPage_);
        forwardRemoteInput_->setPlaceholderText(tr("Forward remote (e.g., host:port)"));
        startForwardButton_ = new QPushButton(tr("Start Forward"), pivotingPage_);
        backwardRemotePortInput_ = new QLineEdit(pivotingPage_);
        backwardRemotePortInput_->setPlaceholderText(tr("Backward remote port"));
        backwardLocalPortInput_ = new QLineEdit(pivotingPage_);
        backwardLocalPortInput_->setPlaceholderText(tr("Backward local port"));
        startBackwardButton_ = new QPushButton(tr("Start Backward"), pivotingPage_);
        stopProxyButton_ = new QPushButton(tr("Stop Selected"), pivotingPage_);
        proxyControlLayout->addWidget(forwardBindInput_);
        proxyControlLayout->addWidget(forwardRemoteInput_);
        proxyControlLayout->addWidget(startForwardButton_);
        proxyControlLayout->addWidget(backwardRemotePortInput_);
        proxyControlLayout->addWidget(backwardLocalPortInput_);
        proxyControlLayout->addWidget(startBackwardButton_);
        proxyControlLayout->addWidget(stopProxyButton_);
        proxyControlLayout->addStretch();
        proxyLayout->addLayout(proxyControlLayout);

        stateTabs_->addTab(pivotingPage_, tr("Pivoting"));
    }

    void KelpiePanel::buildListenersStateTab()
    {
        listenersPage_ = new QWidget(overviewPage_);
        auto* listenersLayout = new QVBoxLayout(listenersPage_);
        listenersLayout->setContentsMargins(0, 0, 0, 0);

        auto* listenersHeader = new QHBoxLayout();
        listenersHeader->addWidget(new QLabel(tr("Pivot / Kelpie listeners"), listenersPage_));
        refreshListenersButton_ = new QPushButton(tr("Refresh"), listenersPage_);
        listenersHeader->addWidget(refreshListenersButton_);
        listenersHeader->addStretch();
        listenersLayout->addLayout(listenersHeader);

        listenersLayout->addWidget(new QLabel(tr("Pivot Listeners"), listenersPage_));
        pivotListenersTable_ = new QTableWidget(listenersPage_);
        pivotListenersTable_->setColumnCount(6);
        pivotListenersTable_->setHorizontalHeaderLabels(
            {tr("ID"), tr("Protocol"), tr("Bind"), tr("Mode"), tr("Status"), tr("Target")});
        pivotListenersTable_->horizontalHeader()->setStretchLastSection(true);
        pivotListenersTable_->setEditTriggers(QAbstractItemView::NoEditTriggers);
        listenersLayout->addWidget(pivotListenersTable_, 1);

        auto* pivotButtons = new QHBoxLayout();
        createPivotListenerButton_ = new QPushButton(tr("Create Pivot"), listenersPage_);
        updatePivotListenerButton_ = new QPushButton(tr("Edit Pivot"), listenersPage_);
        deletePivotListenerButton_ = new QPushButton(tr("Delete Pivot"), listenersPage_);
        pivotButtons->addWidget(createPivotListenerButton_);
        pivotButtons->addWidget(updatePivotListenerButton_);
        pivotButtons->addWidget(deletePivotListenerButton_);
        pivotButtons->addStretch();
        listenersLayout->addLayout(pivotButtons);

        listenersLayout->addWidget(new QLabel(tr("Controller Listeners"), listenersPage_));
        controllerListenersTable_ = new QTableWidget(listenersPage_);
        controllerListenersTable_->setColumnCount(5);
        controllerListenersTable_->setHorizontalHeaderLabels(
            {tr("ID"), tr("Protocol"), tr("Bind"), tr("Status"), tr("Updated")});
        controllerListenersTable_->horizontalHeader()->setStretchLastSection(true);
        controllerListenersTable_->setEditTriggers(QAbstractItemView::NoEditTriggers);
        listenersLayout->addWidget(controllerListenersTable_, 1);

        auto* controllerButtons = new QHBoxLayout();
        createControllerListenerButton_ = new QPushButton(tr("Create Kelpie"), listenersPage_);
        updateControllerListenerButton_ = new QPushButton(tr("Edit Kelpie"), listenersPage_);
        deleteControllerListenerButton_ = new QPushButton(tr("Delete Kelpie"), listenersPage_);
        controllerButtons->addWidget(createControllerListenerButton_);
        controllerButtons->addWidget(updateControllerListenerButton_);
        controllerButtons->addWidget(deleteControllerListenerButton_);
        controllerButtons->addStretch();
        listenersLayout->addLayout(controllerButtons);

        stateTabs_->addTab(listenersPage_, tr("Listeners"));
    }

    void KelpiePanel::buildInfrastructureStateTab()
    {
        infrastructurePage_ = new QWidget(overviewPage_);
        auto* networkLayout = new QVBoxLayout(infrastructurePage_);
        networkLayout->setContentsMargins(0, 0, 0, 0);

        networkTable_ = new QTableWidget(infrastructurePage_);
        networkTable_->setColumnCount(3);
        networkTable_->setHorizontalHeaderLabels({tr("Network ID"), tr("Active"), tr("Nodes")});
        networkTable_->horizontalHeader()->setStretchLastSection(true);
        networkTable_->setEditTriggers(QAbstractItemView::NoEditTriggers);
        networkLayout->addWidget(networkTable_, 1);

        auto* networkButtons = new QHBoxLayout();
        refreshNetworksButton_ = new QPushButton(tr("Refresh"), infrastructurePage_);
        useNetworkButton_ = new QPushButton(tr("Use Selected"), infrastructurePage_);
        resetNetworkButton_ = new QPushButton(tr("Reset"), infrastructurePage_);
        setNodeNetworkButton_ = new QPushButton(tr("Set Node -> Network"), infrastructurePage_);
        pruneOfflineButton_ = new QPushButton(tr("Prune Offline"), infrastructurePage_);
        networkButtons->addWidget(refreshNetworksButton_);
        networkButtons->addWidget(useNetworkButton_);
        networkButtons->addWidget(resetNetworkButton_);
        networkButtons->addWidget(setNodeNetworkButton_);
        networkButtons->addWidget(pruneOfflineButton_);
        networkButtons->addStretch();
        networkLayout->addLayout(networkButtons);

        stateTabs_->addTab(infrastructurePage_, tr("Infrastructure"));
    }

    void KelpiePanel::buildTransportStateTab()
    {
        transportPage_ = new QWidget(overviewPage_);
        auto* dtnLayout = new QVBoxLayout(transportPage_);
        dtnLayout->setContentsMargins(0, 0, 0, 0);

        dtnStatsLabel_ = new QLabel(tr("DTN stats: -"), transportPage_);
        dtnLayout->addWidget(dtnStatsLabel_);
        dtnBundleTable_ = new QTableWidget(transportPage_);
        dtnBundleTable_->setColumnCount(7);
        dtnBundleTable_->setHorizontalHeaderLabels({tr("Bundle ID"), tr("Target"), tr("Priority"), tr("Attempts"), tr("Age"), tr("Deliver By"), tr("Preview")});
        dtnBundleTable_->horizontalHeader()->setStretchLastSection(true);
        dtnBundleTable_->setEditTriggers(QAbstractItemView::NoEditTriggers);
        dtnLayout->addWidget(dtnBundleTable_, 1);

        auto* dtnControls = new QHBoxLayout();
        dtnLimitSpin_ = new QSpinBox(transportPage_);
        dtnLimitSpin_->setRange(1, 500);
        dtnLimitSpin_->setValue(50);
        refreshDtnButton_ = new QPushButton(tr("Refresh DTN"), transportPage_);
        dtnControls->addWidget(new QLabel(tr("Limit:"), transportPage_));
        dtnControls->addWidget(dtnLimitSpin_);
        dtnControls->addWidget(refreshDtnButton_);
        dtnControls->addStretch();
        dtnLayout->addLayout(dtnControls);

        auto* enqueueLayout = new QHBoxLayout();
        dtnPayloadInput_ = new QLineEdit(transportPage_);
        dtnPayloadInput_->setPlaceholderText(tr("Payload (text)"));
        dtnTtlInput_ = new QLineEdit(transportPage_);
        dtnTtlInput_->setPlaceholderText(tr("TTL seconds (optional)"));
        dtnPriorityBox_ = new QComboBox(transportPage_);
        dtnPriorityBox_->addItem(tr("Normal"), kelpieui::v1::DTN_PRIORITY_NORMAL);
        dtnPriorityBox_->addItem(tr("High"), kelpieui::v1::DTN_PRIORITY_HIGH);
        dtnPriorityBox_->addItem(tr("Low"), kelpieui::v1::DTN_PRIORITY_LOW);
        enqueueDtnButton_ = new QPushButton(tr("Enqueue"), transportPage_);
        enqueueLayout->addWidget(new QLabel(tr("Payload:"), transportPage_));
        enqueueLayout->addWidget(dtnPayloadInput_, 2);
        enqueueLayout->addWidget(new QLabel(tr("TTL:"), transportPage_));
        enqueueLayout->addWidget(dtnTtlInput_);
        enqueueLayout->addWidget(new QLabel(tr("Priority:"), transportPage_));
        enqueueLayout->addWidget(dtnPriorityBox_);
        enqueueLayout->addWidget(enqueueDtnButton_);
        dtnLayout->addLayout(enqueueLayout);

        dtnLayout->addWidget(new QLabel(tr("Policy"), transportPage_));
        dtnPolicyTable_ = new QTableWidget(transportPage_);
        dtnPolicyTable_->setColumnCount(2);
        dtnPolicyTable_->setHorizontalHeaderLabels({tr("Key"), tr("Value")});
        dtnPolicyTable_->horizontalHeader()->setStretchLastSection(true);
        dtnPolicyTable_->setEditTriggers(QAbstractItemView::NoEditTriggers);
        dtnPolicyTable_->setSelectionBehavior(QAbstractItemView::SelectRows);
        dtnPolicyTable_->setSelectionMode(QAbstractItemView::SingleSelection);
        dtnLayout->addWidget(dtnPolicyTable_);

        auto* dtnPolicyLayout = new QHBoxLayout();
        dtnPolicyKeyInput_ = new QLineEdit(transportPage_);
        dtnPolicyKeyInput_->setPlaceholderText(tr("Policy key (e.g., max_inflight_per_target)"));
        dtnPolicyValueInput_ = new QLineEdit(transportPage_);
        dtnPolicyValueInput_->setPlaceholderText(tr("Policy value"));
        applyDtnPolicyButton_ = new QPushButton(tr("Apply Policy"), transportPage_);
        refreshDtnPolicyButton_ = new QPushButton(tr("Refresh Policy"), transportPage_);
        dtnPolicyLayout->addWidget(new QLabel(tr("Key:"), transportPage_));
        dtnPolicyLayout->addWidget(dtnPolicyKeyInput_);
        dtnPolicyLayout->addWidget(new QLabel(tr("Value:"), transportPage_));
        dtnPolicyLayout->addWidget(dtnPolicyValueInput_);
        dtnPolicyLayout->addWidget(applyDtnPolicyButton_);
        dtnPolicyLayout->addWidget(refreshDtnPolicyButton_);
        dtnLayout->addLayout(dtnPolicyLayout);

        stateTabs_->addTab(transportPage_, tr("Transport"));
    }

    void KelpiePanel::buildResilienceStateTab()
    {
        resiliencePage_ = new QWidget(overviewPage_);
        auto* supplementalLayout = new QVBoxLayout(resiliencePage_);
        supplementalLayout->setContentsMargins(0, 0, 0, 0);

        supplementalFilter_ = new QLineEdit(resiliencePage_);
        supplementalFilter_->setPlaceholderText(tr("Filter by kind/action/target"));
        supplementalSummary_ = new QLabel(tr("Supplemental: -"), resiliencePage_);
        supplementalLayout->addWidget(supplementalSummary_);
        supplementalLayout->addWidget(supplementalFilter_);

        auto* supplementalButtons = new QHBoxLayout();
        refreshSupplementalButton_ = new QPushButton(tr("Refresh"), resiliencePage_);
        supplementalButtons->addWidget(refreshSupplementalButton_);
        supplementalButtons->addStretch();
        supplementalLayout->addLayout(supplementalButtons);

        supplementalTable_ = new QTableWidget(resiliencePage_);
        supplementalTable_->setColumnCount(6);
        supplementalTable_->setHorizontalHeaderLabels({tr("Seq"), tr("Kind"), tr("Action"), tr("Source"), tr("Target"), tr("Detail")});
        supplementalTable_->horizontalHeader()->setStretchLastSection(true);
        supplementalTable_->setEditTriggers(QAbstractItemView::NoEditTriggers);
        supplementalLayout->addWidget(supplementalTable_, 2);

        supplementalLayout->addWidget(new QLabel(tr("Node Quality"), resiliencePage_));
        supplementalQualityTable_ = new QTableWidget(resiliencePage_);
        supplementalQualityTable_->setColumnCount(9);
        supplementalQualityTable_->setHorizontalHeaderLabels({tr("Node"),
                                                              tr("Health"),
                                                              tr("Latency"),
                                                              tr("Failure"),
                                                              tr("Queue"),
                                                              tr("Stale"),
                                                              tr("OK"),
                                                              tr("Fail"),
                                                              tr("Last Heartbeat")});
        supplementalQualityTable_->horizontalHeader()->setStretchLastSection(true);
        supplementalQualityTable_->setEditTriggers(QAbstractItemView::NoEditTriggers);
        supplementalLayout->addWidget(supplementalQualityTable_, 1);

        stateTabs_->addTab(resiliencePage_, tr("Resilience"));
    }

    void KelpiePanel::buildStrategyStateTab()
    {
        strategyPage_ = new QWidget(overviewPage_);
        auto* metricsLayout = new QVBoxLayout(strategyPage_);
        metricsLayout->setContentsMargins(0, 0, 0, 0);

        auto* routingLayout = new QHBoxLayout();
        routingStrategyBox_ = new QComboBox(strategyPage_);
        routingStrategyBox_->addItem(tr("Hops (BFS)"), kelpieui::v1::ROUTING_STRATEGY_HOPS);
        routingStrategyBox_->addItem(tr("Weight (Dijkstra)"), kelpieui::v1::ROUTING_STRATEGY_WEIGHT);
        routingStrategyBox_->addItem(tr("Latency (ETTD)"), kelpieui::v1::ROUTING_STRATEGY_LATENCY);
        applyRoutingButton_ = new QPushButton(tr("Apply Routing"), strategyPage_);
        routingLayout->addWidget(new QLabel(tr("Routing:"), strategyPage_));
        routingLayout->addWidget(routingStrategyBox_);
        routingLayout->addWidget(applyRoutingButton_);
        routingLayout->addStretch();
        metricsLayout->addLayout(routingLayout);

        metricsView_ = new QPlainTextEdit(strategyPage_);
        metricsView_->setReadOnly(true);
        metricsLayout->addWidget(metricsView_, 1);

        stateTabs_->addTab(strategyPage_, tr("Strategy"));
    }

    void KelpiePanel::buildAuditTrailStateTab()
    {
        auditTrailPage_ = new QWidget(overviewPage_);
        auto* auditLayout = new QVBoxLayout(auditTrailPage_);
        auditLayout->setContentsMargins(0, 0, 0, 0);

        auto* auditFilterLayout = new QHBoxLayout();
        auditUserFilter_ = new QLineEdit(auditTrailPage_);
        auditUserFilter_->setPlaceholderText(tr("User (optional)"));
        auditMethodFilter_ = new QLineEdit(auditTrailPage_);
        auditMethodFilter_->setPlaceholderText(tr("Method (optional)"));
        auditFromInput_ = new QLineEdit(auditTrailPage_);
        auditFromInput_->setPlaceholderText(tr("From ISO time (optional)"));
        auditToInput_ = new QLineEdit(auditTrailPage_);
        auditToInput_->setPlaceholderText(tr("To ISO time (optional)"));
        auditLimitSpin_ = new QSpinBox(auditTrailPage_);
        auditLimitSpin_->setRange(1, 500);
        auditLimitSpin_->setValue(100);
        refreshAuditButton_ = new QPushButton(tr("Refresh"), auditTrailPage_);
        auditFilterLayout->addWidget(new QLabel(tr("User:"), auditTrailPage_));
        auditFilterLayout->addWidget(auditUserFilter_);
        auditFilterLayout->addWidget(new QLabel(tr("Method:"), auditTrailPage_));
        auditFilterLayout->addWidget(auditMethodFilter_);
        auditFilterLayout->addWidget(new QLabel(tr("From:"), auditTrailPage_));
        auditFilterLayout->addWidget(auditFromInput_);
        auditFilterLayout->addWidget(new QLabel(tr("To:"), auditTrailPage_));
        auditFilterLayout->addWidget(auditToInput_);
        auditFilterLayout->addWidget(new QLabel(tr("Limit:"), auditTrailPage_));
        auditFilterLayout->addWidget(auditLimitSpin_);
        auditFilterLayout->addWidget(refreshAuditButton_);
        auditLayout->addLayout(auditFilterLayout);

        auditTable_ = new QTableWidget(auditTrailPage_);
        auditTable_->setColumnCount(7);
        auditTable_->setHorizontalHeaderLabels({tr("Time"), tr("User"), tr("Role"), tr("Method"), tr("Target"), tr("Status"), tr("Error")});
        auditTable_->horizontalHeader()->setStretchLastSection(true);
        auditTable_->setEditTriggers(QAbstractItemView::NoEditTriggers);
        auditLayout->addWidget(auditTable_, 1);

        stateTabs_->addTab(auditTrailPage_, tr("Audit Trail"));
    }

    void KelpiePanel::buildHostIntelStateTab()
    {
        hostIntelPage_ = new QWidget(overviewPage_);
        auto* diagLayout = new QVBoxLayout(hostIntelPage_);
        diagLayout->setContentsMargins(0, 0, 0, 0);

        diagnosticsTable_ = new QTableWidget(hostIntelPage_);
        diagnosticsTable_->setColumnCount(6);
        diagnosticsTable_->setHorizontalHeaderLabels({tr("Target"), tr("Issue"), tr("Detail"), tr("Metric"), tr("Value"), tr("Process")});
        diagnosticsTable_->horizontalHeader()->setStretchLastSection(true);
        diagnosticsTable_->setEditTriggers(QAbstractItemView::NoEditTriggers);
        diagLayout->addWidget(diagnosticsTable_, 1);

        streamDiagTable_ = new QTableWidget(hostIntelPage_);
        streamDiagTable_->setColumnCount(6);
        streamDiagTable_->setHorizontalHeaderLabels({tr("Stream ID"), tr("Target"), tr("Kind"), tr("Pending"), tr("Inflight"), tr("RTO/Last Activity")});
        streamDiagTable_->horizontalHeader()->setStretchLastSection(true);
        streamDiagTable_->setEditTriggers(QAbstractItemView::NoEditTriggers);
        diagLayout->addWidget(streamDiagTable_, 1);

        auto* diagButtons = new QHBoxLayout();
        refreshDiagnosticsButton_ = new QPushButton(tr("Refresh Diagnostics"), hostIntelPage_);
        streamPingButton_ = new QPushButton(tr("Ping Streams"), hostIntelPage_);
        streamPingCount_ = new QLineEdit(hostIntelPage_);
        streamPingCount_->setPlaceholderText(tr("Count"));
        streamPingSize_ = new QLineEdit(hostIntelPage_);
        streamPingSize_->setPlaceholderText(tr("Payload size"));
        diagButtons->addWidget(refreshDiagnosticsButton_);
        diagButtons->addWidget(streamPingButton_);
        diagButtons->addWidget(streamPingCount_);
        diagButtons->addWidget(streamPingSize_);
        diagButtons->addStretch();
        diagLayout->addLayout(diagButtons);

        stateTabs_->addTab(hostIntelPage_, tr("Host Intel"));
    }

    void KelpiePanel::buildRecoveryStateTab()
    {
        recoveryPage_ = new QWidget(overviewPage_);
        auto* repairsLayout = new QVBoxLayout(recoveryPage_);
        repairsLayout->setContentsMargins(0, 0, 0, 0);

        auto* repairsButtons = new QHBoxLayout();
        refreshRepairsButton_ = new QPushButton(tr("Refresh Repairs"), recoveryPage_);
        repairsButtons->addWidget(refreshRepairsButton_);
        repairsButtons->addStretch();
        repairsLayout->addLayout(repairsButtons);

        repairsTable_ = new QTableWidget(recoveryPage_);
        repairsTable_->setColumnCount(6);
        repairsTable_->setHorizontalHeaderLabels({tr("Target"),
                                                  tr("Attempts"),
                                                  tr("Next Attempt"),
                                                  tr("Broken"),
                                                  tr("Last Error"),
                                                  tr("Reason")});
        repairsTable_->horizontalHeader()->setStretchLastSection(true);
        repairsTable_->setEditTriggers(QAbstractItemView::NoEditTriggers);
        repairsLayout->addWidget(repairsTable_, 1);

        stateTabs_->addTab(recoveryPage_, tr("Recovery"));
    }

    void KelpiePanel::buildTaskingStateTab()
    {
        taskingPage_ = new QWidget(overviewPage_);
        auto* opsLayout = new QVBoxLayout(taskingPage_);
        opsLayout->setContentsMargins(0, 0, 0, 0);

        auto* taskingTabs = new QTabWidget(taskingPage_);
        taskingTabs->setDocumentMode(true);

        auto* sleepPage = new QWidget(taskingPage_);
        auto* sleepPageLayout = new QVBoxLayout(sleepPage);
        sleepPageLayout->setContentsMargins(0, 0, 0, 0);
        auto* sleepLayout = new QHBoxLayout();
        sleepSecondsInput_ = new QLineEdit(sleepPage);
        sleepSecondsInput_->setPlaceholderText(tr("Sleep seconds"));
        workSecondsInput_ = new QLineEdit(sleepPage);
        workSecondsInput_->setPlaceholderText(tr("Work seconds"));
        jitterInput_ = new QLineEdit(sleepPage);
        jitterInput_->setPlaceholderText(tr("Jitter %"));
        refreshSleepButton_ = new QPushButton(tr("Refresh Sleep"), sleepPage);
        updateSleepButton_ = new QPushButton(tr("Update Sleep"), sleepPage);
        sleepLayout->addWidget(new QLabel(tr("Sleep:"), sleepPage));
        sleepLayout->addWidget(sleepSecondsInput_);
        sleepLayout->addWidget(new QLabel(tr("Work:"), sleepPage));
        sleepLayout->addWidget(workSecondsInput_);
        sleepLayout->addWidget(new QLabel(tr("Jitter:"), sleepPage));
        sleepLayout->addWidget(jitterInput_);
        sleepLayout->addWidget(updateSleepButton_);
        sleepLayout->addWidget(refreshSleepButton_);
        sleepPageLayout->addLayout(sleepLayout);
        sleepPageLayout->addStretch();
        taskingTabs->addTab(sleepPage, tr("Sleep"));

        auto* dialPage = new QWidget(taskingPage_);
        auto* dialPageLayout = new QVBoxLayout(dialPage);
        dialPageLayout->setContentsMargins(0, 0, 0, 0);
        auto* dialLayout = new QHBoxLayout();
        dialAddressInput_ = new QLineEdit(dialPage);
        dialAddressInput_->setPlaceholderText(tr("Dial address (host:port)"));
        dialReasonInput_ = new QLineEdit(dialPage);
        dialReasonInput_->setPlaceholderText(tr("Reason (optional)"));
        startDialButton_ = new QPushButton(tr("Start Dial"), dialPage);
        cancelDialButton_ = new QPushButton(tr("Cancel Dial"), dialPage);
        dialLayout->addWidget(dialAddressInput_);
        dialLayout->addWidget(dialReasonInput_);
        dialLayout->addWidget(startDialButton_);
        dialLayout->addWidget(cancelDialButton_);
        dialPageLayout->addLayout(dialLayout);
        dialTable_ = new QTableWidget(dialPage);
        dialTable_->setColumnCount(5);
        dialTable_->setHorizontalHeaderLabels({tr("Dial ID"), tr("Target"), tr("Address"), tr("State"), tr("Reason/Error")});
        dialTable_->horizontalHeader()->setStretchLastSection(true);
        dialTable_->setEditTriggers(QAbstractItemView::NoEditTriggers);
        dialPageLayout->addWidget(dialTable_, 1);
        taskingTabs->addTab(dialPage, tr("Dial"));

        auto* sshPage = new QWidget(taskingPage_);
        auto* sshPageLayout = new QVBoxLayout(sshPage);
        sshPageLayout->setContentsMargins(0, 0, 0, 0);
        sshTable_ = new QTableWidget(sshPage);
        sshTable_->setColumnCount(4);
        sshTable_->setHorizontalHeaderLabels({tr("Type"), tr("Target"), tr("Server/Port"), tr("Status")});
        sshTable_->horizontalHeader()->setStretchLastSection(true);
        sshTable_->setEditTriggers(QAbstractItemView::NoEditTriggers);
        sshPageLayout->addWidget(sshTable_, 1);

        auto* sshLayout = new QHBoxLayout();
        sshServerInput_ = new QLineEdit(sshPage);
        sshServerInput_->setPlaceholderText(tr("SSH server (host:port)"));
        sshAuthCombo_ = new QComboBox(sshPage);
        sshAuthCombo_->addItem(tr("Password"), kelpieui::v1::SSH_TUNNEL_AUTH_METHOD_PASSWORD);
        sshAuthCombo_->addItem(tr("Cert"), kelpieui::v1::SSH_TUNNEL_AUTH_METHOD_CERT);
        sshUserInput_ = new QLineEdit(sshPage);
        sshUserInput_->setPlaceholderText(tr("Username"));
        sshPassInput_ = new QLineEdit(sshPage);
        sshPassInput_->setPlaceholderText(tr("Password / PrivateKey"));
        sshPassInput_->setEchoMode(QLineEdit::Password);
        sshTunnelPortInput_ = new QLineEdit(sshPage);
        sshTunnelPortInput_->setPlaceholderText(tr("Agent port (for tunnel)"));
        startSshSessionButton_ = new QPushButton(tr("Start SSH Session"), sshPage);
        startSshTunnelButton_ = new QPushButton(tr("Start SSH Tunnel"), sshPage);
        sshLayout->addWidget(sshServerInput_);
        sshLayout->addWidget(sshAuthCombo_);
        sshLayout->addWidget(sshUserInput_);
        sshLayout->addWidget(sshPassInput_);
        sshLayout->addWidget(sshTunnelPortInput_);
        sshLayout->addWidget(startSshSessionButton_);
        sshLayout->addWidget(startSshTunnelButton_);
        sshPageLayout->addLayout(sshLayout);
        taskingTabs->addTab(sshPage, tr("SSH"));

        opsLayout->addWidget(taskingTabs, 1);
        stateTabs_->addTab(taskingPage_, tr("Tasking"));
    }

}
