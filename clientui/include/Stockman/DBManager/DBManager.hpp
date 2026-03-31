#ifndef STOCKMAN_DBMANAGER_HPP
#define STOCKMAN_DBMANAGER_HPP

#include <QString>
#include <string>
#include <vector>
#include <Stockman/KelpieTypes.hpp>

#include <QSqlDatabase>
#include <QSqlQuery>
#include <QSqlError>

namespace StockmanNamespace::StockmanSpace
{
class DBManager
{
private:
    QSqlDatabase DB;

    bool createNewDatabase();
    bool ensureKelpieSchema();

public:
    static std::string DBFilePath;

    static int OpenSqlFile;
    static int CreateSqlFile;

    DBManager(const QString& FilePath, int OpenFlag = OpenSqlFile);

    bool addKelpieInfo( const Util::ConnectionInfo& );
    bool checkKelpieExists( const QString& ProfileName );
    bool removeKelpieInfo( const QString& ProfileName );
    bool removeAllKelpies();
    std::vector<Util::ConnectionInfo> listKelpies();

};
} // namespace StockmanNamespace::StockmanSpace

#endif
