#include <Stockman/Stockman.hpp>
#include <Stockman/CmdLine.hpp>

#include <toml++/toml.hpp>

#include <memory>

#include <QCoreApplication>
#include <QApplication>
#include <QFont>
#include <QDir>
#include <QFileInfo>
#include <QTimer>
#include <QStandardPaths>
#include <Util/Base.hpp>
#include <Stockman/KelpieController.hpp>
#include <Stockman/KelpieState.hpp>

StockmanSpace::Stockman::Stockman( QMainWindow* w )
    : Stockman(std::make_shared<StockmanNamespace::AppContext>(), w)
{
}

StockmanSpace::Stockman::Stockman( std::shared_ptr<StockmanNamespace::AppContext> context, QMainWindow* w )
{
    this->context_ = context;
    if ( this->context_ )
    {
        this->context_->app = this;
        this->StockmanAppUI.setContext( this->context_ );
    }
    w->setVisible( false );
    mainWindow_.reset(w);

    spdlog::set_pattern( "[%T] [%^%l%$] %v" );
    spdlog::info(
        "Stockman Framework [Version: {}] [CodeName: {}]",
        StockmanNamespace::Version,
        StockmanNamespace::CodeName
    );

    // 数据库固定放在用户数据目录；不可写时直接报错并停止启动。
    const QString appDataDir = QStandardPaths::writableLocation(QStandardPaths::AppDataLocation);
    const QString dbPath = appDataDir + "/client.db";
    QDir().mkpath(QFileInfo(dbPath).absolutePath());
    QFile testFile(dbPath + ".tmp");
    if ( !testFile.open(QIODevice::WriteOnly) )
    {
        spdlog::error("AppData db path not writable: {}", appDataDir.toStdString());
        if (context_) {
            context_->gateGUI = true;
        }
        return;
    }
    testFile.close();
    QFile::remove(testFile.fileName());
    this->dbManager = std::make_shared<StockmanSpace::DBManager>( dbPath, DBManager::CreateSqlFile );
    if ( this->context_ )
    {
        this->context_->dbManager = this->dbManager;
        this->context_->ui = &this->StockmanAppUI;
    }

}

void StockmanSpace::Stockman::Init( int argc, char** argv )
{
    auto List      = std::vector<Util::ConnectionInfo>();
    StockmanNamespace::UserInterface::Dialogs::Connect connectDialog;
    auto Arguments = cmdline::parser();
    auto Path      = std::string();

    Arguments.add( "debug",  '\0', "debug mode" );
    Arguments.add( "config", '\0', "toml config path" );
    Arguments.parse_check( argc, argv );


    if ( Arguments.exist( "debug" ) ) {
        spdlog::set_level( spdlog::level::debug );
        spdlog::debug( "Debug mode enabled" );
    }

    std::string fontFamily = "Cantarell";
    int fontSize = 11;

    // Config is optional: it currently controls only UI niceties (fonts, scripts list).
    QString foundConfig;
    QStringList candidates;
    if ( Arguments.exist( "config" ) )
    {
        const auto cli = QString::fromStdString(Arguments.get<std::string>("config"));
        if ( !cli.trimmed().isEmpty() )
        {
            candidates << cli;
        }
    }
    const QString exeDir = QCoreApplication::applicationDirPath();
    candidates << (exeDir + "/config.toml");

    for (const auto& c : candidates)
    {
        if (!c.isEmpty() && QFile::exists(c))
        {
            foundConfig = c;
            break;
        }
    }

    if ( foundConfig.isEmpty() )
    {
        spdlog::warn("config.toml not found; using defaults (font {} {})", fontFamily, fontSize);
    }
    else
    {
        try
        {
            auto config = toml::parse_file(foundConfig.toStdString());
            const auto& tbl = config;
            if ( const auto* fontTbl = tbl["font"].as_table() )
            {
                if ( auto fam = (*fontTbl)["family"].value<std::string>() )
                {
                    fontFamily = *fam;
                }
                if ( auto sz = (*fontTbl)["size"].value<int>() )
                {
                    fontSize = *sz;
                }
            }
            spdlog::info("loaded config file: {}", foundConfig.toStdString());
        }
        catch ( const std::exception& e )
        {
            spdlog::warn("failed to parse config file {}: {} (using defaults)", foundConfig.toStdString(), e.what());
        }
    }

    QApplication::setFont( QFont( fontFamily.c_str(), fontSize ) );
    QTimer::singleShot( 10, [&]() {
        QApplication::setFont( QFont( fontFamily.c_str(), fontSize ) );
    } );

    if (auto* win = window()) {
        win->setVisible(false);
    }

    connectDialog.KelpieList = dbManager->listKelpies();
    connectDialog.passDB( this->dbManager );
    connectDialog.setContext( this->context_ );
    auto dialog = std::make_unique<QDialog>();
    connectDialog.setupUi( dialog.get() );

    if ( this->context_ )
    {
        this->context_->kelpieConfig = connectDialog.StartDialog( false );
    }
    PendingKelpieBinding = connectDialog.LastConnectionWasGrpc();

    // 启动阶段：连接对话框连接成功后，直接显示主窗口。
    // 否则会出现“连接成功但没有任何窗口显示”的假死体验。
    if ( this->context_ && !this->context_->gateGUI && connectDialog.tryConnect )
    {
        this->Start();
    }
}

void StockmanSpace::Stockman::Start()
{
    this->ClientInitConnect = false;
    if ( this->StockmanAppUI.centralwidget == nullptr )
    {
        this->StockmanAppUI.setupUi( window() );
    }
    if ( PendingKelpieBinding )
    {
        this->StockmanAppUI.BindKelpieController();
        PendingKelpieBinding = false;
    }
    if (auto* win = window()) {
        win->setVisible(true);
        win->setCentralWidget( this->StockmanAppUI.centralwidget );
        win->show();
    }
}

void StockmanSpace::Stockman::RefreshTheme()
{
    StockmanNamespace::Util::ApplyGlobalDraculaTheme();
    this->StockmanAppUI.RefreshTheme();
}

void StockmanSpace::Stockman::Exit()
{
    spdlog::critical( "Exit Program" );
    if (auto* app = QCoreApplication::instance()) {
        app->quit();
    } else {
        QCoreApplication::quit();
    }
}

StockmanSpace::Stockman::~Stockman()
{
    if ( this->context_ )
    {
        if (this->context_->kelpieController) {
            this->context_->kelpieController->Disconnect();
        }
        this->context_->ClearRuntime(false);
        this->context_->kelpieController.reset();
        this->context_->kelpieState.reset();
        this->context_->dbManager.reset();
        this->context_->ui = nullptr;
        this->context_->app = nullptr;
    }
}
