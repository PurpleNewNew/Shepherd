#ifndef STOCKMAN_STOCKMANUI_HPP
#define STOCKMAN_STOCKMANUI_HPP

#include <QAction>
#include <QGridLayout>
#include <QMainWindow>
#include <QMenu>
#include <QMenuBar>
#include <QTabWidget>
#include <QTextEdit>
#include <QTimer>
#include <deque>
#include <functional>
#include <memory>

#include <UserInterface/Dialogs/About.hpp>
#include <UserInterface/Dialogs/Connect.hpp>
#include <Stockman/AppContext.hpp>
#include "proto/kelpieui/v1/kelpieui.pb.h"

namespace StockmanNamespace {
namespace UserInterface {
    class KelpiePanel;
}
}

class StockmanNamespace::UserInterface::StockmanUi : public QMainWindow
{
public:
    QWidget*               centralwidget                 = {};
    QAction*               actionNew_Client              = {};
    QAction*               actionChat                    = {};
    QAction*               actionDisconnect              = {};
    QAction*               actionExit                    = {};
    QAction*               actionKelpie              = {};
    QAction*               actionAbout                   = {};
    QAction*               actionOpen_Help_Documentation = {};
    QAction*               actionOpen_API_Reference      = {};
    QAction*               actionGithub_Repository       = {};
    QAction*               actionPivotListeners          = {};
    QAction*               actionControllerListeners     = {};
    QAction*               actionSessionsTable           = {};
    QAction*               actionSessionsGraph           = {};
    QAction*               actionLogs                    = {};
    QAction*               actionLoot                    = {};
    QGridLayout*           gridLayout                    = {};
    QGridLayout*           gridLayout_3                  = {};
    QTabWidget*            KelpieTabWidget           = {};
    QMenuBar*              menubar                       = {};
    QMenu*                 menuStockman                     = {};
    QMenu*                 menuView                      = {};
    QMenu*                 menuHelp                      = {};
    QMenu*                 MenuSession                   = {};
    std::unique_ptr<About> AboutDialog;
    QMainWindow*           StockmanWindow                   = {};
    std::shared_ptr<StockmanNamespace::AppContext> context_               = nullptr;
    KelpiePanel*           KelpieWidget                  = {};
    int                    KelpieTabIndex                = -1;

public:
        void setupUi( QMainWindow *Stockman );
        void retranslateUi( QMainWindow *Stockman ) const;
        void setContext( std::shared_ptr<StockmanNamespace::AppContext> ctx ) { context_ = std::move(ctx); }
    void RefreshTheme();
    void ConnectEvents();
    // 统一错误输出窗口：对外公开，便于其他组件集中写入
    void AppendErrorLog(const QString& message) const;
    // 统一 status/toast：默认展示在 status bar，错误同时写入 error dock
    void ToastInfo(const QString& message, int timeoutMs = 3500) const;
    void ToastWarn(const QString& message, int timeoutMs = 5000) const;
    void ToastError(const QString& message, int timeoutMs = 8000) const;

public slots:
    void HandleKelpieSnapshot(const kelpieui::v1::Snapshot& snapshot);
    void HandleKelpieLog(const QString& message);
    void HandleKelpieCommand(const QString& message);
    void HandleKelpieUiEvent(const kelpieui::v1::UiEvent& event);
    void BindKelpieController();
    bool IsKelpieGrpcActive() const;
    void ShowKelpiePanel(const std::function<void(KelpiePanel*)>& focusCallback);

private:
    QTimer*                uiEventDebounce_ = {};
    std::deque<kelpieui::v1::UiEvent> uiEventQueue_;

    // 统一错误输出窗口
    QTextEdit*             errorLog_ = nullptr;

    // setupUi helpers to降低维护成本
    void initWindow(QMainWindow* Stockman);
    void createActions();
    void createCentralPanels();
    void createMenuBar();
    void applyThemeChrome();

    // event wiring拆分
    void connectStockmanActions();
    void connectViewActions();
    void connectHelpActions();
    void initUiEventQueue();
    void drainUiEventQueue();

    // View actions 拆分，统一分流到 KelpiePanel
    void handleViewChat();
    void handleViewLoot();
    void handleViewSessionsTable();
    void handleViewPivotListeners();
    void handleViewControllerListeners();
    void handleViewKelpie();
    void handleViewSessionsGraph();
    void handleViewLogs();
    bool focusKelpiePanelIfGrpc(const std::function<void(KelpiePanel*)>& focusCallback);
    void withKelpiePanelOrWarn(const QString& warnMessage, const std::function<void(KelpiePanel*)>& focusCallback);
    void handleAuditEvent(const kelpieui::v1::UiEvent& evt);
    void appendKelpieLog(const kelpieui::v1::UiEvent& evt);
};

#endif
