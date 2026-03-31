// Headers for UserInterface
#include <Stockman/Stockman.hpp>
#include <UserInterface/StockmanUI.hpp>
#include <UserInterface/KelpiePanel.hpp>

#include <Util/Base.hpp>
#include <Util/ThemeManager.hpp>
#include <Util/UiConstants.hpp>
#include <spdlog/spdlog.h>

#include <Stockman/KelpieController.hpp>
#include <Stockman/KelpieState.hpp>
#include <Stockman/AppContext.hpp>
#include "proto/kelpieui/v1/kelpieui.pb.h"

#include <QDateTime>
#include <QDockWidget>
#include <QDesktopServices>
#include <QProcess>
#include <QStatusBar>
#include <QThread>
#include <QTimer>
#include <Util/Message.hpp>

using namespace StockmanNamespace::StockmanSpace;

namespace
{
    constexpr std::size_t kUiEventDrainBatchSize = 32;

    QString displayNode(const kelpieui::v1::NodeInfo& node)
    {
        const QString uuid = QString::fromStdString(node.uuid());
        const QString alias = QString::fromStdString(node.alias());
        if ( alias.isEmpty() )
        {
            return uuid;
        }
        return QStringLiteral("%1 (%2)").arg(alias, uuid);
    }

    QString describeNodeKind(kelpieui::v1::NodeEvent_Kind kind)
    {
        switch ( kind )
        {
            case kelpieui::v1::NodeEvent_Kind_ADDED:
                return QObject::tr("added");
            case kelpieui::v1::NodeEvent_Kind_UPDATED:
                return QObject::tr("updated");
            case kelpieui::v1::NodeEvent_Kind_REMOVED:
                return QObject::tr("removed");
            default:
                return QObject::tr("changed");
        }
    }

    QString describeListenerKind(kelpieui::v1::PivotListenerEvent_Kind kind)
    {
        switch ( kind )
        {
            case kelpieui::v1::PivotListenerEvent_Kind_PIVOT_LISTENER_ADDED:
                return QObject::tr("added");
            case kelpieui::v1::PivotListenerEvent_Kind_PIVOT_LISTENER_UPDATED:
                return QObject::tr("updated");
            case kelpieui::v1::PivotListenerEvent_Kind_PIVOT_LISTENER_REMOVED:
                return QObject::tr("removed");
            default:
                return QObject::tr("changed");
        }
    }

    QString describeSessionKind(kelpieui::v1::SessionEvent_Kind kind)
    {
        switch ( kind )
        {
            case kelpieui::v1::SessionEvent_Kind_SESSION_EVENT_ADDED:
                return QObject::tr("added");
            case kelpieui::v1::SessionEvent_Kind_SESSION_EVENT_UPDATED:
                return QObject::tr("updated");
            case kelpieui::v1::SessionEvent_Kind_SESSION_EVENT_REMOVED:
                return QObject::tr("removed");
            case kelpieui::v1::SessionEvent_Kind_SESSION_EVENT_MARKED:
                return QObject::tr("marked");
            case kelpieui::v1::SessionEvent_Kind_SESSION_EVENT_REPAIR_STARTED:
                return QObject::tr("repair started");
            case kelpieui::v1::SessionEvent_Kind_SESSION_EVENT_REPAIR_COMPLETED:
                return QObject::tr("repair completed");
            case kelpieui::v1::SessionEvent_Kind_SESSION_EVENT_TERMINATED:
                return QObject::tr("terminated");
            default:
                return QObject::tr("updated");
        }
    }

    QString describeStreamKind(kelpieui::v1::StreamEvent_Kind kind)
    {
        switch ( kind )
        {
            case kelpieui::v1::StreamEvent_Kind_STREAM_OPENED:
                return QObject::tr("opened");
            case kelpieui::v1::StreamEvent_Kind_STREAM_UPDATED:
                return QObject::tr("updated");
            case kelpieui::v1::StreamEvent_Kind_STREAM_CLOSED:
                return QObject::tr("closed");
            default:
                return QObject::tr("changed");
        }
    }

    QString describeProxyKind(kelpieui::v1::ProxyEvent_Kind kind)
    {
        switch ( kind )
        {
            case kelpieui::v1::ProxyEvent_Kind_PROXY_EVENT_STARTED:
                return QObject::tr("started");
            case kelpieui::v1::ProxyEvent_Kind_PROXY_EVENT_STOPPED:
                return QObject::tr("stopped");
            case kelpieui::v1::ProxyEvent_Kind_PROXY_EVENT_FAILED:
                return QObject::tr("failed");
            default:
                return QObject::tr("updated");
        }
    }

    QString describeDialKind(kelpieui::v1::DialEvent_Kind kind)
    {
        switch ( kind )
        {
            case kelpieui::v1::DialEvent_Kind_DIAL_EVENT_ENQUEUED:
                return QObject::tr("enqueued");
            case kelpieui::v1::DialEvent_Kind_DIAL_EVENT_RUNNING:
                return QObject::tr("running");
            case kelpieui::v1::DialEvent_Kind_DIAL_EVENT_COMPLETED:
                return QObject::tr("completed");
            case kelpieui::v1::DialEvent_Kind_DIAL_EVENT_FAILED:
                return QObject::tr("failed");
            case kelpieui::v1::DialEvent_Kind_DIAL_EVENT_CANCELED:
                return QObject::tr("canceled");
            default:
                return QObject::tr("updated");
        }
    }

    QString describeKelpieEvent(const kelpieui::v1::UiEvent& event)
    {
        if ( event.has_listener_event() )
        {
            const auto& payload = event.listener_event();
            const auto& listener = payload.listener();
            const QString id = QString::fromStdString(listener.listener_id());
            return QObject::tr("Pivot listener %1 %2")
                .arg(id.isEmpty() ? QObject::tr("(unnamed)") : id,
                     describeListenerKind(payload.kind()));
        }
        if ( event.has_session_event() )
        {
            const auto& payload = event.session_event();
            const QString target = QString::fromStdString(payload.session().target_uuid());
            QString message = QObject::tr("Session %1 %2")
                                  .arg(target.isEmpty() ? QObject::tr("(unknown)") : target,
                                       describeSessionKind(payload.kind()));
            if ( !payload.reason().empty() )
            {
                message.append(QObject::tr(" (%1)").arg(QString::fromStdString(payload.reason())));
            }
            return message;
        }
        if ( event.has_node_event() )
        {
            const auto& payload = event.node_event();
            return QObject::tr("Node %1 %2").arg(displayNode(payload.node()),
                                                 describeNodeKind(payload.kind()));
        }
        if ( event.has_stream_event() )
        {
            const auto& payload = event.stream_event();
            return QObject::tr("Stream %1 %2 (%3)")
                .arg(QString::number(payload.stream().stream_id()),
                     describeStreamKind(payload.kind()),
                     QString::fromStdString(payload.stream().kind()));
        }
        if ( event.has_proxy_event() )
        {
            const auto& payload = event.proxy_event();
            const auto& proxy = payload.proxy();
            QString message = QObject::tr("Proxy %1 %2")
                                  .arg(QString::fromStdString(proxy.proxy_id()),
                                       describeProxyKind(payload.kind()));
            if ( !payload.reason().empty() )
            {
                message.append(QObject::tr(" (%1)").arg(QString::fromStdString(payload.reason())));
            }
            return message;
        }
        if ( event.has_sleep_event() )
        {
            const auto& payload = event.sleep_event();
            QString detail;
            if ( payload.has_sleep_seconds() )
            {
                detail = QObject::tr("%1s").arg(payload.sleep_seconds());
            }
            if ( payload.has_work_seconds() )
            {
                if ( !detail.isEmpty() )
                {
                    detail.append("/");
                }
                detail.append(QObject::tr("%1s work").arg(payload.work_seconds()));
            }
            if ( detail.isEmpty() )
            {
                detail = QObject::tr("updated");
            }
            return QObject::tr("Sleep profile %1 %2")
                .arg(QString::fromStdString(payload.target_uuid()), detail);
        }
        if ( event.has_supplemental_event() )
        {
            const auto& payload = event.supplemental_event();
            QString target = QString::fromStdString(payload.target_uuid());
            if ( target.isEmpty() )
            {
                target = QString::fromStdString(payload.source_uuid());
            }
            return QObject::tr("Supplemental %1 %2 (%3)")
                .arg(QString::fromStdString(payload.kind()),
                     QString::fromStdString(payload.action()),
                     target);
        }
        if ( event.has_dial_event() )
        {
            const auto& payload = event.dial_event();
            const auto& status = payload.status();
            QString message = QObject::tr("Dial %1 %2")
                                  .arg(QString::fromStdString(status.dial_id()),
                                       describeDialKind(payload.kind()));
            if ( !status.reason().empty() )
            {
                message.append(QObject::tr(" (%1)").arg(QString::fromStdString(status.reason())));
            }
            return message;
        }
        if ( event.has_loot_event() )
        {
            const auto& payload = event.loot_event();
            const auto& item = payload.item();
            QString category;
            switch ( item.category() )
            {
                case kelpieui::v1::LOOT_CATEGORY_FILE:
                    category = QObject::tr("file");
                    break;
                case kelpieui::v1::LOOT_CATEGORY_SCREENSHOT:
                    category = QObject::tr("screenshot");
                    break;
                case kelpieui::v1::LOOT_CATEGORY_TICKET:
                    category = QObject::tr("ticket");
                    break;
                default:
                    category = QObject::tr("loot");
                    break;
            }
            const QString target = QString::fromStdString(item.target_uuid());
            const QString name = QString::fromStdString(item.name());
            return QObject::tr("Loot %1 (%2) from %3")
                .arg(name.isEmpty() ? QObject::tr("(unnamed)") : name,
                     category,
                     target.isEmpty() ? QObject::tr("(unknown)") : target);
        }
        if ( event.has_chat_event() )
        {
            const auto& msg = event.chat_event().message();
            return QObject::tr("Chat %1: %2").arg(QString::fromStdString(msg.username()),
                                                  QString::fromStdString(msg.message()));
        }
        if ( event.has_audit_event() )
        {
            const auto& entry = event.audit_event().entry();
            return QObject::tr("Audit %1 %2 (%3)")
                .arg(QString::fromStdString(entry.username()),
                     QString::fromStdString(entry.method()),
                     QString::fromStdString(entry.status()));
        }
        return {};
    }

}

void StockmanNamespace::UserInterface::StockmanUi::setupUi(QMainWindow *Stockman)
{
    StockmanWindow = Stockman;
    initWindow(Stockman);
    createActions();
    createMenuBar();
    createCentralPanels();
    StockmanNamespace::Util::NormalizeInteractiveControls(StockmanWindow);
    initUiEventQueue();
    applyThemeChrome();

    ConnectEvents();
    retranslateUi( StockmanWindow );
    QMetaObject::connectSlotsByName( StockmanWindow );

    auto* controller = context_ ? context_->kelpieController.get() : nullptr;
    if ( controller )
    {
        BindKelpieController();
    }
}

void StockmanNamespace::UserInterface::StockmanUi::initWindow(QMainWindow* Stockman)
{
    if ( StockmanWindow->objectName().isEmpty() ) {
        StockmanWindow->setObjectName( QString::fromUtf8( "StockmanWindow" ) );
    }

    StockmanWindow->resize( 1399, 821 );
    StockmanWindow->setMinimumSize(
        StockmanNamespace::Util::UiScalePx(1120),
        StockmanNamespace::Util::UiScalePx(700));

    centralwidget = new QWidget( StockmanWindow );
    centralwidget->setObjectName( QString::fromUtf8( "centralwidget" ) );
    gridLayout_3 = new QGridLayout( centralwidget );
    gridLayout_3->setObjectName( QString::fromUtf8( "gridLayout_3" ) );
    gridLayout_3->setContentsMargins( 0, 0, 0, 0 );
}

void StockmanNamespace::UserInterface::StockmanUi::createActions()
{
    actionNew_Client = new QAction( StockmanWindow );
    actionNew_Client->setObjectName( QString::fromUtf8( "NewClient" ) );

    actionChat = new QAction( StockmanWindow );
    actionChat->setObjectName( QString::fromUtf8( "actionChat" ) );

    actionDisconnect = new QAction( StockmanWindow );
    actionDisconnect->setObjectName( QString::fromUtf8( "actionDisconnect" ) );

    actionExit = new QAction( StockmanWindow );
    actionExit->setObjectName( QString::fromUtf8( "actionExit" ) );

    actionKelpie = new QAction( StockmanWindow );
    actionKelpie->setObjectName( QString::fromUtf8( "actionKelpie" ) );

    actionAbout = new QAction( StockmanWindow );
    actionAbout->setObjectName( QString::fromUtf8( "actionAbout" ) );

    actionOpen_Help_Documentation = new QAction( StockmanWindow );
    actionOpen_Help_Documentation->setObjectName( QString::fromUtf8( "actionOpen_Help_Documentation" ) );

    actionOpen_API_Reference = new QAction( StockmanWindow );
    actionOpen_API_Reference->setObjectName( QString::fromUtf8( "actionOpen_API_Reference" ) );

    actionGithub_Repository = new QAction( StockmanWindow );
    actionGithub_Repository->setObjectName( QString::fromUtf8( "actionGithub_Repository" ) );

    actionPivotListeners = new QAction( StockmanWindow );
    actionPivotListeners->setObjectName( QString::fromUtf8( "actionPivotListeners" ) );

    actionControllerListeners = new QAction(StockmanWindow);
    actionControllerListeners->setObjectName(QString::fromUtf8("actionControllerListeners"));

    actionSessionsTable = new QAction( StockmanWindow );
    actionSessionsTable->setObjectName( QString::fromUtf8( "actionSessionsTable" ) );

    actionSessionsGraph = new QAction( StockmanWindow );
    actionSessionsGraph->setObjectName( QString::fromUtf8( "actionSessionsGraph" ) );

    actionLogs = new QAction( StockmanWindow );
    actionLogs->setObjectName( QString::fromUtf8( "actionLogs" ) );

    actionLoot = new QAction(StockmanWindow);
    actionLoot->setObjectName(QString::fromUtf8("actionLoot"));
}

	void StockmanNamespace::UserInterface::StockmanUi::createCentralPanels()
	{
	    KelpieTabWidget = new QTabWidget( centralwidget );
	    KelpieTabWidget->setObjectName( QString::fromUtf8( "KelpieTabWidget" ) );
	    KelpieTabWidget->setTabBarAutoHide( true );
	    KelpieTabWidget->setTabsClosable( false );

    KelpieWidget = new StockmanNamespace::UserInterface::KelpiePanel( context_, centralwidget );
    KelpieWidget->setObjectName( QString::fromUtf8( "KelpiePanelWidget" ) );
    KelpieTabIndex = KelpieTabWidget->addTab( KelpieWidget, tr("Kelpie") );
    KelpieTabWidget->setCurrentIndex( KelpieTabIndex );

    gridLayout_3->addWidget( KelpieTabWidget, 0, 0, 1, 1 );

    // Error log dock：集中展示最近错误，默认可折叠
    auto* errorDock = new QDockWidget(tr("Errors"), this);
    errorDock->setObjectName(QStringLiteral("ErrorDock"));
	    errorDock->setAllowedAreas(Qt::BottomDockWidgetArea | Qt::RightDockWidgetArea);
	    errorLog_ = new QTextEdit(errorDock);
	    errorLog_->setReadOnly(true);
	    errorLog_->document()->setMaximumBlockCount(2000);
	    errorDock->setWidget(errorLog_);
	    this->addDockWidget(Qt::BottomDockWidgetArea, errorDock);
	    menuView->addAction(errorDock->toggleViewAction());
	}

void StockmanNamespace::UserInterface::StockmanUi::createMenuBar()
{
    menubar = new QMenuBar( this->StockmanWindow );
    menubar->setObjectName( QString::fromUtf8( "menubar" ) );
    menubar->setGeometry( QRect( 0, 0, 1143, 20 ) );

    menuStockman   = new QMenu( menubar );
    menuView    = new QMenu( menubar );
    MenuSession = new QMenu( menubar );
    menuHelp    = new QMenu( menubar );

    menuStockman->setObjectName( QString::fromUtf8( "menuStockman" ) );
    menuView->setObjectName( QString::fromUtf8( "menuView" ) );

    StockmanWindow->setMenuBar( menubar );

    menuHelp->setObjectName( QString::fromUtf8( "menuHelp" ) );

    menubar->addAction( menuStockman->menuAction() );
    menubar->addAction( menuView->menuAction() );
    menubar->addAction( menuHelp->menuAction() );

    menuStockman->addAction( actionNew_Client );
    menuStockman->addSeparator();
    menuStockman->addAction( actionDisconnect );
    menuStockman->addAction( actionExit );

    MenuSession->addAction( actionSessionsTable );
    MenuSession->addAction( actionSessionsGraph );

    menuView->addAction( actionPivotListeners );
    menuView->addAction( actionControllerListeners );
    menuView->addSeparator();
    menuView->addAction( MenuSession->menuAction() );
    menuView->addSeparator();
    menuView->addAction( actionChat );
    menuView->addSeparator();
    menuView->addAction( actionLogs );
    menuView->addAction( actionKelpie );
    menuView->addAction( actionLoot );

    menuHelp->addAction( actionAbout );
    menuHelp->addSeparator();
    menuHelp->addAction( actionOpen_Help_Documentation );
    menuHelp->addAction( actionOpen_API_Reference );
    menuHelp->addSeparator();
    menuHelp->addAction( actionGithub_Repository );
}

	void StockmanNamespace::UserInterface::StockmanUi::applyThemeChrome()
	{
	    auto& theme = StockmanNamespace::Util::ThemeManager::Instance();
	    theme.ApplyStockmanChrome(StockmanWindow, menubar, KelpieTabWidget);
	}

	void StockmanNamespace::UserInterface::StockmanUi::BindKelpieController()
	{
	    auto* controller = context_ ? context_->kelpieController.get() : nullptr;
	    auto* state = context_ ? context_->kelpieState.get() : nullptr;
	    if ( !controller )
    {
        return;
    }
    QObject::connect(
        controller,
        &StockmanNamespace::KelpieController::LogMessageReceived,
        this,
        &StockmanNamespace::UserInterface::StockmanUi::HandleKelpieLog,
        Qt::UniqueConnection );
    QObject::connect(
        controller,
        &StockmanNamespace::KelpieController::CommandOutputReceived,
        this,
        &StockmanNamespace::UserInterface::StockmanUi::HandleKelpieCommand,
        Qt::UniqueConnection );
    if ( state )
    {
        QObject::connect(
            state,
            &StockmanNamespace::KelpieState::SnapshotUpdated,
            this,
            &StockmanNamespace::UserInterface::StockmanUi::HandleKelpieSnapshot,
            Qt::UniqueConnection );
        QObject::connect(
            state,
            &StockmanNamespace::KelpieState::UiEventReceived,
            this,
            &StockmanNamespace::UserInterface::StockmanUi::HandleKelpieUiEvent,
            Qt::UniqueConnection );
    }

    if ( KelpieTabIndex >= 0 )
    {
        KelpieTabWidget->setCurrentIndex( KelpieTabIndex );
    }
    kelpieui::v1::Snapshot initialSnapshot;
    if ( state && state->HasSnapshot() )
    {
        initialSnapshot = state->Snapshot();
    }
    else if ( controller )
    {
        initialSnapshot = controller->LatestSnapshot();
    }
    HandleKelpieSnapshot(initialSnapshot);
}

void StockmanNamespace::UserInterface::StockmanUi::HandleKelpieSnapshot(const kelpieui::v1::Snapshot& snapshot)
{
    if ( !KelpieWidget )
    {
        return;
    }
    KelpieWidget->UpdateSnapshot( snapshot );
}

void StockmanNamespace::UserInterface::StockmanUi::HandleKelpieLog(const QString& message)
{
    if ( KelpieWidget )
    {
        KelpieWidget->AppendLog( message );
    }
}

void StockmanNamespace::UserInterface::StockmanUi::HandleKelpieCommand(const QString& message)
{
    if ( KelpieWidget )
    {
        KelpieWidget->AppendLog( message );
    }
    AppendErrorLog(message);
}

void StockmanNamespace::UserInterface::StockmanUi::HandleKelpieUiEvent(const kelpieui::v1::UiEvent& evt)
{
    // bounded queue to avoid OOM during storms; drop oldest if over limit
    constexpr std::size_t kUiEventQueueLimit = StockmanNamespace::UiConstants::kUiEventQueueMax;
    if ( uiEventQueue_.size() >= kUiEventQueueLimit )
    {
        uiEventQueue_.pop_front();
    }
    uiEventQueue_.push_back(evt);
    if ( uiEventDebounce_ && !uiEventDebounce_->isActive() )
    {
        uiEventDebounce_->start();
    }
}

void StockmanNamespace::UserInterface::StockmanUi::drainUiEventQueue()
{
    std::size_t processed = 0;
    while ( !uiEventQueue_.empty() && processed < kUiEventDrainBatchSize )
    {
        const auto evt = uiEventQueue_.front();
        uiEventQueue_.pop_front();
        handleAuditEvent(evt);
        appendKelpieLog(evt);
        ++processed;
    }
    if ( !uiEventQueue_.empty() && uiEventDebounce_ != nullptr )
    {
        // Yield to the event loop between batches so bursty streams do not pin the UI thread.
        uiEventDebounce_->start(0);
    }
}

void StockmanNamespace::UserInterface::StockmanUi::handleAuditEvent(const kelpieui::v1::UiEvent& evt)
{
    if ( evt.has_audit_event() )
    {
        const auto& entry = evt.audit_event().entry();
        spdlog::info("Audit: {} {} ({})",
                     entry.username(),
                     entry.method(),
                     entry.status());
    }
}

void StockmanNamespace::UserInterface::StockmanUi::appendKelpieLog(const kelpieui::v1::UiEvent& evt)
{
    if ( !KelpieWidget )
    {
        return;
    }
    KelpieWidget->ProcessEvent(evt);
    const QString description = describeKelpieEvent(evt);
    if ( !description.isEmpty() )
    {
        KelpieWidget->AppendLog(description);
    }
}

bool StockmanNamespace::UserInterface::StockmanUi::IsKelpieGrpcActive() const
{
    auto* controller = context_ ? context_->kelpieController.get() : nullptr;
    return controller && controller->Connected();
}

void StockmanNamespace::UserInterface::StockmanUi::ShowKelpiePanel(const std::function<void(KelpiePanel*)>& focusCallback)
{
    if ( KelpieTabIndex < 0 || KelpieWidget == nullptr )
    {
        return;
    }
    KelpieTabWidget->setCurrentIndex( KelpieTabIndex );
    if ( focusCallback )
    {
        focusCallback( KelpieWidget );
    }
}

bool StockmanNamespace::UserInterface::StockmanUi::focusKelpiePanelIfGrpc(const std::function<void(KelpiePanel*)>& focusCallback)
{
    if ( !IsKelpieGrpcActive() )
    {
        return false;
    }
	    ShowKelpiePanel(focusCallback);
	    return true;
	}

	void StockmanNamespace::UserInterface::StockmanUi::withKelpiePanelOrWarn(
	    const QString& warnMessage, const std::function<void(KelpiePanel*)>& focusCallback)
	{
	    if ( focusKelpiePanelIfGrpc(focusCallback) )
	    {
	        return;
	    }
	    StockmanNamespace::Ui::ShowWarn(tr("Kelpie"), warnMessage, this);
	}

	void StockmanNamespace::UserInterface::StockmanUi::handleViewChat()
	{
	    withKelpiePanelOrWarn(tr("Kelpie gRPC not connected; connect first to use operator chat."),
	                          [](KelpiePanel* panel) {
	                              if ( panel ) { panel->FocusChat(); }
	                          });
	}

	void StockmanNamespace::UserInterface::StockmanUi::handleViewLoot()
	{
	    withKelpiePanelOrWarn(tr("Kelpie gRPC not connected; connect first to view loot."),
	                          [](KelpiePanel* panel) {
	                              if ( panel ) { panel->FocusLoot(); }
	                          });
	}

	void StockmanNamespace::UserInterface::StockmanUi::handleViewSessionsTable()
	{
	    withKelpiePanelOrWarn(tr("Kelpie gRPC not connected; connect first to view target sessions."),
	                          [](KelpiePanel* panel) {
	                              if ( panel ) { panel->FocusNodes(); }
	                          });
	}

	void StockmanNamespace::UserInterface::StockmanUi::handleViewPivotListeners()
	{
	    withKelpiePanelOrWarn(tr("Kelpie gRPC not connected; connect first to view pivot listeners."),
	                          [](KelpiePanel* panel) {
	                              if ( panel ) { panel->FocusPivotListeners(); }
	                          });
	}

	void StockmanNamespace::UserInterface::StockmanUi::handleViewControllerListeners()
	{
	    withKelpiePanelOrWarn(tr("Kelpie gRPC not connected; connect first to view Kelpie listeners."),
	                          [](KelpiePanel* panel) {
	                              if ( panel ) { panel->FocusControllerListeners(); }
	                          });
	}

	void StockmanNamespace::UserInterface::StockmanUi::handleViewKelpie()
	{
	    withKelpiePanelOrWarn(tr("Kelpie gRPC not connected; connect first to view teamserver info."), nullptr);
	}

	void StockmanNamespace::UserInterface::StockmanUi::handleViewSessionsGraph()
	{
	    withKelpiePanelOrWarn(tr("Kelpie gRPC not connected; connect first to view traffic streams."),
	                          [](KelpiePanel* panel) {
	                              if ( panel ) { panel->FocusStreams(); }
	                          });
	}

	void StockmanNamespace::UserInterface::StockmanUi::handleViewLogs()
	{
	    withKelpiePanelOrWarn(tr("Kelpie gRPC not connected; connect first to view Ops Console."),
	                          [](KelpiePanel* panel) {
	                              if ( panel ) { panel->FocusLog(); }
	                          });
	}

void StockmanNamespace::UserInterface::StockmanUi::retranslateUi(QMainWindow* Stockman ) const
{
    Stockman->setWindowTitle( "Stockman" );

    actionNew_Client->setText( "New Client" );
    actionChat->setText( "Kelpie Chat" );
    actionDisconnect->setText( "Disconnect" );
    actionExit->setText( "Exit" );
    actionKelpie->setText( "Kelpie" );
    actionAbout->setText( "About" );
    actionOpen_Help_Documentation->setText( "Open Documentation" );
    actionOpen_API_Reference->setText( "Open API Reference" );
    actionGithub_Repository->setText( "Shepherd Repository" );
    actionPivotListeners->setText( "Pivot Listeners" );
    actionControllerListeners->setText("Kelpie Listeners");
    actionSessionsTable->setText( "Targets" );
    actionSessionsGraph->setText( "Traffic" );
    actionLogs->setText( "Ops Console" );
    actionLoot->setText( "Loot" );
    menuStockman->setTitle( "Stockman" );
    menuView->setTitle( "View" );
    menuHelp->setTitle( "Help" );
    MenuSession->setTitle( "Workspace" );

    StockmanWindow->setFocus();
    StockmanWindow->showMaximized();

}

void StockmanNamespace::UserInterface::StockmanUi::ConnectEvents()
{
    connectStockmanActions();
    connectViewActions();
    connectHelpActions();
}

void StockmanNamespace::UserInterface::StockmanUi::initUiEventQueue()
{
    uiEventDebounce_ = new QTimer(this);
    uiEventDebounce_->setSingleShot(true);
    uiEventDebounce_->setInterval(200);
    connect(uiEventDebounce_, &QTimer::timeout, this, &StockmanNamespace::UserInterface::StockmanUi::drainUiEventQueue);
}

void StockmanNamespace::UserInterface::StockmanUi::connectStockmanActions()
{
    QMainWindow::connect( actionNew_Client, &QAction::triggered, this, []() {
        QProcess::startDetached( QCoreApplication::applicationFilePath(), QStringList{""} );
    } );

    QMainWindow::connect( actionDisconnect, &QAction::triggered, this, [this]() {
        auto* controller = context_ ? context_->kelpieController.get() : nullptr;
        if ( controller ) {
            controller->Disconnect();
            StockmanNamespace::Ui::ShowInfo(tr("Disconnected"), QObject::tr("Disconnected Kelpie gRPC session"), this);
            return;
        }
        StockmanNamespace::Ui::ShowInfo(tr("Disconnected"), QObject::tr("No active Kelpie session to disconnect."), this);
    } );

    QMainWindow::connect( actionExit, &QAction::triggered, this, []() {
        Stockman::Exit();
    } );
}

void StockmanNamespace::UserInterface::StockmanUi::connectViewActions()
{
    QMainWindow::connect(
        actionChat,
        &QAction::triggered,
        this,
        &StockmanNamespace::UserInterface::StockmanUi::handleViewChat);
    QMainWindow::connect(
        actionLoot,
        &QAction::triggered,
        this,
        &StockmanNamespace::UserInterface::StockmanUi::handleViewLoot);
    QMainWindow::connect(
        actionSessionsTable,
        &QAction::triggered,
        this,
        &StockmanNamespace::UserInterface::StockmanUi::handleViewSessionsTable);
    QMainWindow::connect(
        actionPivotListeners,
        &QAction::triggered,
        this,
        &StockmanNamespace::UserInterface::StockmanUi::handleViewPivotListeners);
    QMainWindow::connect(
        actionControllerListeners,
        &QAction::triggered,
        this,
        &StockmanNamespace::UserInterface::StockmanUi::handleViewControllerListeners);
    QMainWindow::connect(
        actionKelpie,
        &QAction::triggered,
        this,
        &StockmanNamespace::UserInterface::StockmanUi::handleViewKelpie);
    QMainWindow::connect(
        actionSessionsGraph,
        &QAction::triggered,
        this,
        &StockmanNamespace::UserInterface::StockmanUi::handleViewSessionsGraph);
    QMainWindow::connect(
        actionLogs,
        &QAction::triggered,
        this,
        &StockmanNamespace::UserInterface::StockmanUi::handleViewLogs);
}

	void StockmanNamespace::UserInterface::StockmanUi::connectHelpActions()
	{
    QMainWindow::connect( actionAbout, &QAction::triggered, this, [&]() {
        if ( !AboutDialog ) {
            AboutDialog = std::make_unique<About>( new QDialog(this->StockmanWindow) );
            AboutDialog->setupUi();
        }

        AboutDialog->AboutDialog->exec();
    } );

	    QMainWindow::connect( actionGithub_Repository, &QAction::triggered, this, []() {
	        QDesktopServices::openUrl( QUrl( StockmanNamespace::UiConstants::kRepositoryUrl ) );
	    } );

	    QMainWindow::connect( actionOpen_API_Reference, &QAction::triggered, this, []() {
	        QDesktopServices::openUrl( QUrl( StockmanNamespace::UiConstants::kApiReferenceUrl ) );
	    } );

	    QMainWindow::connect( actionOpen_Help_Documentation, &QAction::triggered, this, []() {
	        QDesktopServices::openUrl( QUrl( StockmanNamespace::UiConstants::kDocsUrl ) );
	    } );
	}

void StockmanNamespace::UserInterface::StockmanUi::RefreshTheme()
{
    applyThemeChrome();
    StockmanNamespace::Util::NormalizeInteractiveControls(StockmanWindow);
    // KelpiePanel 内部组件在各自页面刷新时会同步主题。
}
void StockmanNamespace::UserInterface::StockmanUi::AppendErrorLog(const QString& message) const
{
    if ( !errorLog_ )
    {
        return;
    }
    const QString stamped = QStringLiteral("[%1] %2")
                                .arg(QDateTime::currentDateTime().toString(Qt::ISODateWithMs),
                                     message);
    errorLog_->append(stamped);
}

static void toastStatusBar(StockmanNamespace::UserInterface::StockmanUi* ui,
                           const QString& prefix,
                           const QString& message,
                           int timeoutMs)
{
    if ( ui == nullptr )
    {
        return;
    }
    if ( QThread::currentThread() != ui->thread() )
    {
        QMetaObject::invokeMethod(
            ui,
            [ui, prefix, message, timeoutMs]() { toastStatusBar(ui, prefix, message, timeoutMs); },
            Qt::QueuedConnection);
        return;
    }
    if ( auto* sb = ui->statusBar() )
    {
        const QString full = prefix.isEmpty() ? message : QStringLiteral("%1%2").arg(prefix, message);
        sb->showMessage(full, timeoutMs);
    }
}

void StockmanNamespace::UserInterface::StockmanUi::ToastInfo(const QString& message, int timeoutMs) const
{
    toastStatusBar(const_cast<StockmanUi*>(this), QString(), message, timeoutMs);
}

void StockmanNamespace::UserInterface::StockmanUi::ToastWarn(const QString& message, int timeoutMs) const
{
    toastStatusBar(const_cast<StockmanUi*>(this), QStringLiteral("WARN: "), message, timeoutMs);
}

void StockmanNamespace::UserInterface::StockmanUi::ToastError(const QString& message, int timeoutMs) const
{
    auto* self = const_cast<StockmanUi*>(this);
    toastStatusBar(self, QStringLiteral("ERROR: "), message, timeoutMs);
    if ( QThread::currentThread() != self->thread() )
    {
        QMetaObject::invokeMethod(self, [self, message]() { self->AppendErrorLog(message); }, Qt::QueuedConnection);
        return;
    }
    AppendErrorLog(message);
}
