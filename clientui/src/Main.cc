#include <QApplication>
#include <QGuiApplication>
#include <QIcon>
#include <QMainWindow>
#include <QTimer>
#include <QStyleHints>

#include <Stockman/Stockman.hpp>
#include <Stockman/AppContext.hpp>
#include <Util/Base.hpp>

auto main(
    int    argc,
    char** argv
) -> int {
    // Ensure high-DPI scaling uses precise rounding on Qt6.
    QGuiApplication::setHighDpiScaleFactorRoundingPolicy(
        Qt::HighDpiScaleFactorRoundingPolicy::PassThrough);

    auto StockmanApp = QApplication( argc, argv );
    auto Status   = 0;

    QGuiApplication::setWindowIcon( QIcon( ":/Stockman.ico" ) );
    StockmanNamespace::Util::ApplyGlobalDraculaTheme();

    auto appCtx = std::make_shared<StockmanNamespace::AppContext>();
    auto stockman = std::make_unique<StockmanNamespace::StockmanSpace::Stockman>(
        appCtx, new QMainWindow );
    appCtx->app = stockman.get();
    stockman->Init( argc, argv );

    // 如果启动阶段用户在连接对话框中选择关闭，则不进入主事件循环，直接优雅退出。
    if (appCtx->gateGUI)
    {
        spdlog::info("Stockman GUI startup cancelled; exiting.");
        return 0;
    }

    StockmanNamespace::Util::RegisterThemeAutoRefresh(
        &StockmanApp,
        [stockman_ptr = stockman.get()]() {
            if (stockman_ptr) {
                StockmanNamespace::Util::ApplyGlobalDraculaTheme();
                stockman_ptr->RefreshTheme();
            }
        });

    QObject::connect(&StockmanApp, &QGuiApplication::lastWindowClosed, &StockmanApp, &QCoreApplication::quit);

    Status = QApplication::exec();

    spdlog::info( "Stockman Application status: {}", Status );

    return Status;
}
