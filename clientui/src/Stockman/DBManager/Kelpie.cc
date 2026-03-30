#include <Stockman/DBManager/DBManager.hpp>
#include <QSqlError>
#include <QVariant>
#include <vector>

using namespace StockmanNamespace;

bool StockmanSpace::DBManager::addKelpieInfo( const Util::ConnectionInfo& connection )
{
    auto query   = QSqlQuery();
    auto success = true;
    auto error   = std::string();

    query.prepare( "insert into KelpieControllers (ProfileName, Host, Port, Token, ClientName, TlsCa) values(:ProfileName, :Host, :Port, :Token, :ClientName, :TlsCa)" );

    query.bindValue( ":ProfileName", connection.Name );
    query.bindValue( ":Host",        connection.Host );
    query.bindValue( ":Port",        connection.Port );
    query.bindValue( ":Token",       connection.Token );
    query.bindValue( ":ClientName",  connection.ClientName );
    query.bindValue( ":TlsCa",       connection.TlsCa );

    /* print error */
    success = query.exec();
    if ( ! success ) {
        error = query.lastError().text().toStdString();
        spdlog::error( "[DB] Failed to add Kelpie controller info: {}", error );
    }

    return success;
}

bool StockmanSpace::DBManager::checkKelpieExists( const QString& ProfileName )
{
    auto query   = QSqlQuery();
    auto success = false;
    auto error   = std::string();

    query.prepare( "select * from KelpieControllers" );

    if ( ! query.exec() ) {
        error = query.lastError().text().toStdString();
        spdlog::error( "[DB] Failed to query Kelpie controller existence: {}", error );
        return success;
    }

    while ( query.next() ) {
        if ( query.value( "ProfileName" ) == ProfileName ) {
            success = true;
            break;
        }
    }

    return success;
}

bool StockmanSpace::DBManager::removeKelpieInfo( const QString& ProfileName )
{
    auto query = QSqlQuery();
    auto error = std::string();
    auto name  = std::string();

    query.prepare( "delete from KelpieControllers where ProfileName = :ProfileName" );
    query.bindValue( ":ProfileName", ProfileName );

    if ( ! query.exec() ) {
        error = query.lastError().text().toStdString();
        name  = ProfileName.toStdString();

        spdlog::error( "[DB] Failed to delete Kelpie controller [{}] info: {}", name, error );
        return false;
    }

    return true;
}

std::vector<Util::ConnectionInfo> StockmanSpace::DBManager::listKelpies()
{
    auto query          = QSqlQuery();
    auto KelpieList = std::vector<Util::ConnectionInfo>();
    auto error          = std::string();

    query.prepare( "select * from KelpieControllers" );

    if ( ! query.exec() ) {
        error = query.lastError().text().toStdString();

        spdlog::error( "[DB] Error while querying Kelpie controllers: {}", error );
        return KelpieList;
    }

    /* iterating over the queried list */
    while ( query.next() ) {
        KelpieList.push_back( {
            .Name     = query.value( "ProfileName" ).toString(),
            .Host     = query.value( "Host" ).toString(),
            .Port     = query.value( "Port" ).toString(),
            .Token    = query.value( "Token" ).toString(),
            .ClientName = query.value( "ClientName" ).toString(),
            .TlsCa    = query.value( "TlsCa" ).toString(),
        } );
    }

    return KelpieList;
}

bool StockmanSpace::DBManager::removeAllKelpies()
{
    auto query = QSqlQuery();
    auto error = std::string();

    query.prepare( "delete from KelpieControllers" );

    if ( ! query.exec() ) {
        error = query.lastError().text().toStdString();

        spdlog::error( "[DB] Error while deleting Kelpie controllers: {}", error );

        return false;
    }

    return true;
}
