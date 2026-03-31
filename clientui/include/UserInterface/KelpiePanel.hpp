#ifndef STOCKMAN_KELPIE_PANEL_HPP
#define STOCKMAN_KELPIE_PANEL_HPP

#include <QWidget>
#include <QTreeWidget>
#include <QTableWidget>
#include <QPlainTextEdit>
#include <QVBoxLayout>
#include <QLineEdit>
#include <QPushButton>
#include <QHBoxLayout>
#include <QLabel>
#include <QTabWidget>
#include <QHash>
#include <QComboBox>
#include <QSpinBox>
#include <QSet>
#include <QCheckBox>
#include <QPointer>
#include <vector>
#include <memory>
#include <atomic>
#include <thread>

#include <Stockman/AppContext.hpp>
#include <Stockman/KelpieController.hpp>
#include "proto/kelpieui/v1/kelpieui.pb.h"

class QTcpServer;
class QTcpSocket;
class QGraphicsView;
class QGraphicsScene;
class QTimer;

namespace StockmanNamespace::UserInterface
{
    class KelpiePanel : public QWidget
    {
        Q_OBJECT

    public:
        explicit KelpiePanel(std::shared_ptr<StockmanNamespace::AppContext> ctx = nullptr, QWidget* parent = nullptr);
        ~KelpiePanel() override;
        void setContext(std::shared_ptr<StockmanNamespace::AppContext> ctx) { context_ = std::move(ctx); }

        void UpdateSnapshot(const kelpieui::v1::Snapshot& snapshot);
        void AppendLog(const QString& message);
        void FocusNodes();
        void FocusStreams();
        void FocusLog();
        void FocusChat();
        void FocusLoot();
        void FocusPivotListeners();
        void FocusControllerListeners();
        void FocusProxies();
        void FocusSupplemental();
        void ProcessEvent(const kelpieui::v1::UiEvent& event);

    private:
        QTabWidget*       workspaceTabs_;
        QWidget*          overviewPage_;
        QWidget*          streamsPage_;
        QWidget*          consolePage_;
        QWidget*          chatPage_;
        QWidget*          lootPage_;
        QWidget*          shellPage_;
        QWidget*          sessionsPage_;
        QWidget*          topologyPage_;
        QWidget*          sessionListPage_;
        QWidget*          pivotingPage_;
        QWidget*          listenersPage_;
        QWidget*          infrastructurePage_;
        QWidget*          transportPage_;
        QWidget*          resiliencePage_;
        QWidget*          strategyPage_;
        QWidget*          auditTrailPage_;
        QWidget*          hostIntelPage_;
        QWidget*          recoveryPage_;
        QWidget*          taskingPage_;
        QTabWidget*       stateTabs_;
        QTreeWidget*      nodesTree_;
        QLineEdit*        nodeMemoInput_;
        QPushButton*      updateNodeMemoButton_;
        QLineEdit*        sessionReasonInput_;
        QCheckBox*        repairForceCheck_;
        QPushButton*      markAliveButton_;
        QPushButton*      markDeadButton_;
        QPushButton*      nodeStatusButton_;
        QPushButton*      repairSessionButton_;
        QPushButton*      reconnectSessionButton_;
        QPushButton*      terminateSessionButton_;
        QPushButton*      shutdownNodeButton_;
        QTableWidget*     proxyTable_;
        QTableWidget*     supplementalTable_;
        QTableWidget*     supplementalQualityTable_;
        QTableWidget*     auditTable_;
        QTableWidget*     streamsTable_;
        QLineEdit*        closeStreamReasonInput_;
        QPushButton*      closeStreamButton_;
        QTableWidget*     chatTable_;
        QTableWidget*     lootTable_;
        QTableWidget*     pivotListenersTable_;
        QTableWidget*     controllerListenersTable_;
        QTableWidget*     networkTable_;
        QPlainTextEdit*   metricsView_;
        QComboBox*        routingStrategyBox_;
        QPushButton*      applyRoutingButton_;
        QTableWidget*     diagnosticsTable_;
        QTableWidget*     streamDiagTable_;
        QPushButton*      refreshDiagnosticsButton_;
        QPushButton*      streamPingButton_;
        QLineEdit*        streamPingCount_;
        QLineEdit*        streamPingSize_;
        QTableWidget*     dtnBundleTable_;
        QTableWidget*     dtnPolicyTable_;
        QLabel*           dtnStatsLabel_;
        QLineEdit*        dtnPayloadInput_;
        QLineEdit*        dtnTtlInput_;
        QLineEdit*        dtnPolicyKeyInput_;
        QLineEdit*        dtnPolicyValueInput_;
        QComboBox*        dtnPriorityBox_;
        QSpinBox*         dtnLimitSpin_;
        QPushButton*      refreshDtnPolicyButton_;
        QPushButton*      applyDtnPolicyButton_;
        QPlainTextEdit*   logView_;
        QLineEdit*        chatInput_;
        QLineEdit*        lootTargetInput_;
        QLineEdit*        lootTagInput_;
        QComboBox*        lootCategoryBox_;
        QLabel*           selectedNodeLabel_;
        QLineEdit*        nodeCommandInput_;
        QPushButton*      nodeCommandButton_;
        QPushButton*      refreshProxiesButton_;
        QPushButton*      refreshChatButton_;
        QPushButton*      sendChatButton_;
        QPushButton*      refreshLootButton_;
        QPushButton*      submitLootButton_;
        QPushButton*      downloadLootButton_;
        QPushButton*      refreshListenersButton_;
        QPushButton*      createPivotListenerButton_;
        QPushButton*      updatePivotListenerButton_;
        QPushButton*      deletePivotListenerButton_;
        QPushButton*      createControllerListenerButton_;
        QPushButton*      updateControllerListenerButton_;
        QPushButton*      deleteControllerListenerButton_;
        QPushButton*      refreshSupplementalButton_;
        QPushButton*      refreshAuditButton_;

        // Topology (RPC) page
        QLineEdit*        topologyTargetInput_;
        QLineEdit*        topologyNetworkInput_;
        QLineEdit*        topologyFilterInput_;
        QLineEdit*        topologyLocateInput_;
        QPushButton*      refreshTopologyButton_;
        QPushButton*      fitTopologyButton_;
        QPushButton*      topologyLocateButton_;
        QLabel*           topologyStatusLabel_;
        QLabel*           topologyLegendLabel_;
        QComboBox*        topologyLayoutBox_;
        QCheckBox*        topologyShowSupplementalCheck_;
        QGraphicsView*    topologyGraphView_;
        QGraphicsScene*   topologyScene_;
        QTableWidget*     topologyEdgesTable_;

        // Sessions list (RPC) page
        QLineEdit*        sessionListTargetsInput_;
        QComboBox*        sessionStatusBox_;
        QCheckBox*        sessionIncludeInactiveCheck_;
        QPushButton*      refreshSessionListButton_;
        QTableWidget*     sessionListTable_;
        QLineEdit*        supplementalFilter_;
        QLineEdit*        auditUserFilter_;
        QLineEdit*        auditMethodFilter_;
        QLineEdit*        auditFromInput_;
        QLineEdit*        auditToInput_;
        QSpinBox*         auditLimitSpin_;
        QSpinBox*         lootLimitSpin_;
        QPushButton*      pruneOfflineButton_;
        QPushButton*      refreshNetworksButton_;
        QPushButton*      useNetworkButton_;
        QPushButton*      resetNetworkButton_;
        QPushButton*      setNodeNetworkButton_;
        // Sleep / Dial / SSH controls
        QPushButton*      refreshSleepButton_;
        QPushButton*      updateSleepButton_;
        QLineEdit*        sleepSecondsInput_;
        QLineEdit*        workSecondsInput_;
        QLineEdit*        jitterInput_;

        QLineEdit*        dialAddressInput_;
        QLineEdit*        dialReasonInput_;
        QPushButton*      startDialButton_;
        QPushButton*      cancelDialButton_;
        QTableWidget*     dialTable_;

        QLineEdit*        sshServerInput_;
        QLineEdit*        sshUserInput_;
        QLineEdit*        sshPassInput_;
        QComboBox*        sshAuthCombo_;
        QPushButton*      startSshSessionButton_;
        QPushButton*      startSshTunnelButton_;
        QLineEdit*        sshTunnelPortInput_;
        QTableWidget*     sshTable_;
        QLabel*           opsStatusLabel_;
        QLabel*           supplementalSummary_;
        // Repairs
        QPushButton*      refreshRepairsButton_;
        QTableWidget*     repairsTable_;

        QPushButton*      refreshDtnButton_;
        QPushButton*      enqueueDtnButton_;
        QLineEdit*        forwardBindInput_;
        QLineEdit*        forwardRemoteInput_;
        QPushButton*      startForwardButton_;
        QLineEdit*        backwardRemotePortInput_;
        QLineEdit*        backwardLocalPortInput_;
        QPushButton*      startBackwardButton_;
        QPushButton*      stopProxyButton_;
        QPlainTextEdit*   shellView_;
        QLineEdit*        shellInput_;
        QPushButton*      openShellButton_;
        QPushButton*      closeShellButton_;
        QLabel*           shellStatusLabel_;
        QLineEdit*        downloadRemotePathInput_;
        QLineEdit*        downloadLocalPathInput_;
        QPushButton*      browseDownloadButton_;
        QPushButton*      startDownloadButton_;
        QLabel*           downloadStatusLabel_;
        QLineEdit*        uploadLocalPathInput_;
        QLineEdit*        uploadRemotePathInput_;
        QCheckBox*        uploadOverwriteCheck_;
        QPushButton*      browseUploadButton_;
        QPushButton*      startUploadButton_;
        QLabel*           uploadStatusLabel_;
        QLineEdit*        socksPortInput_;
        QCheckBox*        socksAuthCheck_;
        QLineEdit*        socksUserInput_;
        QLineEdit*        socksPasswordInput_;
        QPushButton*      startSocksButton_;
        QPushButton*      stopSocksButton_;
        QLabel*           socksStatusLabel_;
        QString           currentNodeUuid_;
        QHash<QString, int> proxyRowIndex_;
        QSet<QString>     chatIds_;
        QSet<QString>     lootIds_;
        QString           pendingShellTarget_;
        QString           pendingShellLine_;
        std::shared_ptr<StockmanNamespace::ProxyStreamHandle> shellStream_;
        std::vector<std::shared_ptr<StockmanNamespace::ProxyStreamHandle>> sshStreams_;
        QTcpServer*       socksServer_ = nullptr;
        struct SocksBridge {
            QPointer<QTcpSocket> socket;
            std::shared_ptr<StockmanNamespace::ProxyStreamHandle> handle;
        };
        std::vector<std::unique_ptr<SocksBridge>> socksBridges_;
        std::jthread       downloadThread_;
        std::jthread       uploadThread_;
        std::atomic<bool>  downloadActive_{false};
        std::atomic<bool>  uploadActive_{false};
        std::atomic<uint64_t> downloadGeneration_{0};
        std::atomic<uint64_t> uploadGeneration_{0};
        kelpieui::v1::GetTopologyResponse topologySnapshot_;
        QString           topologyHighlightNodeUuid_;
        QString           topologyHighlightParentUuid_;
        QString           topologyHighlightChildUuid_;
        QTimer*           streamRefreshDebounce_ = nullptr;
        QTimer*           dialRefreshCooldown_ = nullptr;
        QTimer*           topologyViewDebounce_ = nullptr;
        bool              streamDiagnosticsRefreshInFlight_{false};
        bool              streamDiagnosticsRefreshPending_{false};
        std::shared_ptr<StockmanNamespace::AppContext> context_ = nullptr;

        void setupUi();
        void buildOverviewWorkspaceTab();
        void buildSessionsStateTab();
        void buildTopologyStateTab();
        void buildSessionListStateTab();
        void buildPivotingStateTab();
        void buildListenersStateTab();
        void buildInfrastructureStateTab();
        void buildTransportStateTab();
        void buildResilienceStateTab();
        void buildStrategyStateTab();
        void buildAuditTrailStateTab();
        void buildHostIntelStateTab();
        void buildRecoveryStateTab();
        void buildTaskingStateTab();
        void buildStreamsWorkspaceTab();
        void buildConsoleWorkspaceTab();
        void buildChatWorkspaceTab();
        void buildLootWorkspaceTab();
        void buildShellWorkspaceTab();
        void wireStateActions();
        void wireWorkspaceActions();
        void wireTopologyInteractions();
        void populateNodes(const kelpieui::v1::Snapshot& snapshot);
        void populateStreams(const kelpieui::v1::Snapshot& snapshot);
        void updateSelectedNode();
        void updateNodeMemo();
        void sendNodeCommand();
        void refreshProxies();
        void refreshListeners();
        void closeSelectedStream();
        void refreshTopology();
        void refreshSessionList();
        void refreshSupplemental();
        void applySupplementalFilter();
        void refreshChat();
        void sendChatMessage();
        void appendChatMessage(const kelpieui::v1::ChatMessage& message);
        void createPivotListener();
        void editPivotListener();
        void deletePivotListener();
        void createControllerListener();
        void editControllerListener();
        void deleteControllerListener();
        void markCurrentSession(kelpieui::v1::SessionMarkAction action);
        void repairCurrentSession();
        void reconnectCurrentSession();
        void terminateCurrentSession();
        void queryNodeStatus();
        void shutdownCurrentNode();
        void refreshLoot();
        void appendLootItem(const kelpieui::v1::LootItem& item);
        void submitLootFromFile();
        void downloadSelectedLoot();
        void refreshRepairs();
        void refreshAudit();
        void appendAuditEntry(const kelpieui::v1::AuditLogEntry& entry);
        void refreshNetworks();
        void useSelectedNetwork();
        void resetNetwork();
        void setNodeNetwork();
        void pruneOffline();
        void refreshSleep();
        void updateSleep();
        void refreshDials();
        void startDial();
        void cancelDial();
        void refreshSsh();
        void startSshSession();
        void startSshTunnel();
        void refreshMetrics();
        void applyRoutingStrategy();
        void refreshStreamDiagnostics();
        void refreshDtn();
        void refreshDtnPolicy();
        void applyDtnPolicy();
        void enqueueDtn();
        void startForwardProxy();
        void startBackwardProxy();
        void stopSelectedProxy();
        void populateProxies(const std::vector<kelpieui::v1::ProxyInfo>& proxies);
        void appendSupplementalEvent(const kelpieui::v1::SupplementalEvent& event);
        void updateProxyRow(const kelpieui::v1::ProxyEvent& event);
        void setNodeScopedActionsEnabled(bool enabled);
        void refreshNodeScopedData();
        void scheduleDialRefresh();
        void scheduleStreamRefresh();
        void refreshStatePage(QWidget* page);
        void refreshWorkspacePage(QWidget* page);
        void streamPing();
        void toastInfo(const QString& message, int timeoutMs = 3500);
        void toastWarn(const QString& message, int timeoutMs = 5000);
        void toastError(const QString& message, int timeoutMs = 8000);
        void scheduleTopologyViewRefresh();
        void refreshTopologyView();
        void setTopologyHighlightNode(const QString& uuid);
        void setTopologyHighlightEdge(const QString& parentUuid, const QString& childUuid);
        void applyTopologyHighlights();
        void fitTopologyGraph();
        void renderTopologyGraph(const kelpieui::v1::GetTopologyResponse& topo);
        void populateTopologyEdges(const kelpieui::v1::GetTopologyResponse& topo);
        void locateTopologyNode();
        void selectNodeByUuid(const QString& uuid);
        void startShell();
        void stopShell();
        void sendShellInput();
        void handleShellData(const QByteArray& data);
        void handleShellClosed(const QString& reason);
        void setShellStatus(const QString& status);
        void browseDownloadPath();
        void startDownloadFile();
        void browseUploadPath();
        void startUploadFile();
        void finishDownload(bool success, const QString& errorMessage);
        void finishUpload(bool success, const QString& errorMessage);
        void stopDownload(bool waitForJoin = false);
        void stopUpload(bool waitForJoin = false);
        void startSocksBridge();
        void stopSocksBridge();
        void onNewSocksConnection();
        void registerSocksBridge(std::unique_ptr<SocksBridge> bridge);
        void removeSocksBridge(QTcpSocket* socket);
        void stopSocksServer();
        QString selectedLootId() const;
        QString currentPivotListenerId() const;
        QString currentControllerListenerId() const;
        StockmanNamespace::KelpieController* controller() const;
        StockmanNamespace::KelpieState* state() const;
        void refreshDiagnosticsForNode(const QString& targetUuid);

    };
}

#endif // STOCKMAN_KELPIE_PANEL_HPP
