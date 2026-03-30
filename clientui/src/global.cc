#include <QCoreApplication>
#include <QFileDialog>
#include <QDir>
#include <QJsonObject>
#include <QJsonDocument>
#include <UserInterface/StockmanUI.hpp>
#include <Stockman/AppContext.hpp>
#include <random>
#include <memory>

#include <Stockman/Grpc/Client.hpp>
#include <Stockman/KelpieState.hpp>
#include <Stockman/KelpieController.hpp>
#include <Util/Base.hpp>
#include <Util/Message.hpp>

#include <QFileDialog>

using namespace StockmanNamespace;

std::string StockmanNamespace::Version  = "0.7";
std::string StockmanNamespace::CodeName = "Bites The Dust";

AppContext::AppContext()
{
    kelpieState = std::make_shared<StockmanNamespace::KelpieState>();
    uiRegistry = std::make_unique<UiResourceRegistry>();
}

AppContext::~AppContext() = default;

void AppContext::ClearRuntime(bool disconnectKelpie)
{
    std::lock_guard<std::mutex> guard(runtimeMutex);
    kelpieRuntime.RegisteredListeners.clear();
    kelpieRuntime.Sessions.clear();
    kelpieRuntime.RegisteredCommands.clear();
    kelpieRuntime.RegisteredModules.clear();
    kelpieRuntime.AddedCommands.clear();
    kelpieRuntime.IpAddresses.clear();
    if (uiRegistry) {
        uiRegistry->CleanupAll();
    }
    if (disconnectKelpie && kelpieController) {
        kelpieController->Disconnect();
    }
}

std::string StockmanNamespace::Util::gen_random( const int len )
{
    auto str = std::string( "0123456789ABCDEF" );
    auto rd  = std::random_device();
    auto gen = std::mt19937( rd() );

    std::shuffle( str.begin(), str.end(), gen );

    return str.substr( 0, len );
}

void Util::SessionItem::Export()
{
    auto FileDialog = QFileDialog();
    auto Filename   = QUrl();

    StockmanNamespace::Util::ApplyFileDialogStyle( FileDialog );
    FileDialog.setAcceptMode( QFileDialog::AcceptSave );
    FileDialog.setDirectory( QDir::homePath() );
    FileDialog.selectFile( "Session_data_" + Name + ".json" );

    if ( FileDialog.exec() == QFileDialog::Accepted )
    {
        Filename = FileDialog.selectedUrls().value( 0 ).toLocalFile();

        if ( ! Filename.toString().isNull() )
        {
            auto file       = QFile( Filename.toString() );

            if ( file.open( QIODevice::ReadWrite ) ) {
                auto SessionData = QJsonObject();

                SessionData.insert( "AgentID",          QJsonValue::fromVariant( Name ) );
                SessionData.insert( "ExternalIP",       QJsonValue::fromVariant( External ) );
                SessionData.insert( "InternalIP",       QJsonValue::fromVariant( Internal ) );
                SessionData.insert( "Listener",         QJsonValue::fromVariant( Listener ) );
                SessionData.insert( "User",             QJsonValue::fromVariant( User ) );
                SessionData.insert( "Computer",         QJsonValue::fromVariant( Computer ) );
                SessionData.insert( "Domain",           QJsonValue::fromVariant( Domain ) );
                SessionData.insert( "OS",               QJsonValue::fromVariant( OS ) );
                SessionData.insert( "OSBuild",          QJsonValue::fromVariant( OSBuild ) );
                SessionData.insert( "OSArch",           QJsonValue::fromVariant( OSArch ) );
                SessionData.insert( "ProcessName",      QJsonValue::fromVariant( Process ) );
                SessionData.insert( "ProcessID",        QJsonValue::fromVariant( PID ) );
                SessionData.insert( "ProcessArch",      QJsonValue::fromVariant( Arch ) );
                SessionData.insert( "ProcessElevated",  QJsonValue::fromVariant( Elevated ) );
                SessionData.insert( "PivotParent",      QJsonValue::fromVariant( PivotParent ) );
                SessionData.insert( "First Callback",   QJsonValue::fromVariant( First ) );
                SessionData.insert( "Last Callback",    QJsonValue::fromVariant( Last ) );

                file.write( QJsonDocument( SessionData ).toJson( QJsonDocument::Indented ) );
            }
            else {
                auto path = Filename.toString().toStdString();
                spdlog::error("Couldn't write to file {}", path );
            }

            file.close();

            StockmanNamespace::Ui::ShowInfo(
                QObject::tr("Session Exported"),
                QObject::tr("Path: %1").arg(Filename.toString()));
        }
    }
}
