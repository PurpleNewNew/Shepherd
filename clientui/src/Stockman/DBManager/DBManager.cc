#include <Stockman/DBManager/DBManager.hpp>
#include <QFileInfo>
#include <QSet>

using namespace StockmanNamespace::StockmanSpace;

int DBManager::OpenSqlFile = 1;
int DBManager::CreateSqlFile = 2;

DBManager::DBManager( const QString& FilePath, int OpenFlag )
{
    auto exists = false;
    if ( QFileInfo::exists( FilePath ) ) {
        exists = true;
    }

    this->DB = QSqlDatabase::addDatabase( "QSQLITE" );
    this->DB.setDatabaseName( FilePath );

    if ( this->DB.open() ) {
        if ( OpenFlag == DBManager::CreateSqlFile && ! exists ) {
            if ( this->createNewDatabase() ) {
                spdlog::info( "Successful created database" );
            } else {
                spdlog::error( "Failed to create a new database" );
            }
        }
        ensureKelpieSchema();
    } else {
        spdlog::error( "[DB] Failed to open database" );
    }
}

bool DBManager::createNewDatabase()
{
    auto query = QSqlQuery();
    auto error = std::string();

    /* check if the db file is opened */
    if ( ! DB.isOpen() ) {
        return false;
    }

    query.prepare(
        "CREATE TABLE \"KelpieControllers\" ( "
        "\"ID\" INTEGER PRIMARY KEY, "
        "\"ProfileName\" TEXT, "
        "\"Host\" TEXT, "
        "\"Port\" INTEGER, "
        "\"Token\" TEXT, "
        "\"ClientName\" TEXT, "
        "\"TlsCa\" TEXT "
        ");"
    );
    if ( ! query.exec() ) {
        error = query.lastError().text().toStdString();
        spdlog::error( "[DB] Couldn't create Kelpie controller table: {}", error );
    }

    return true;
}

bool DBManager::ensureKelpieSchema()
{
    if ( !DB.isOpen() ) {
        return false;
    }

    // 使用 IF NOT EXISTS 避免已有数据时重复报错或丢表
    QSqlQuery create(
        "CREATE TABLE IF NOT EXISTS KelpieControllers ("
        "ID INTEGER PRIMARY KEY,"
        "ProfileName TEXT,"
        "Host TEXT,"
        "Port INTEGER,"
        "Token TEXT,"
        "ClientName TEXT,"
        "TlsCa TEXT"
        ")"
    );
    if ( !create.exec() ) {
        spdlog::error("[DB] Failed to ensure KelpieControllers table: {}", create.lastError().text().toStdString());
        return false;
    }

    return true;
}
