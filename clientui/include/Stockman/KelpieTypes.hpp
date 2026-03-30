#ifndef STOCKMAN_KELPIE_TYPES_HPP
#define STOCKMAN_KELPIE_TYPES_HPP

#include <QDateTime>
#include <QString>
#include <QStringList>

#include <vector>

#include <External.h>  // json alias
#include <Stockman/Types.hpp>

namespace StockmanNamespace::Util
{
    struct SessionItem
    {
        QString   KelpieID;
        QString   Name;
        QString   External;
        QString   Internal;
        QString   Listener;
        QString   User;
        QString   Computer;
        QString   Domain;
        QString   OS;
        QString   OSBuild;
        QString   OSArch;
        QString   Process;
        QString   PID;
        QString   Arch;
        QString   First;
        QString   Last;
        QDateTime LastUTC;
        QString   Elevated;
        QString   PivotParent;
        QString   Marked;
        QString   Health;
        StockmanNamespace::u32     SleepDelay;
        StockmanNamespace::u32     SleepJitter;
        StockmanNamespace::u64     KillDate;
        StockmanNamespace::u32     WorkingHours;

        void Export();
    };

    struct KelpieConfig
    {
        QString Name;
        QString Host;
        QString Port;
        QString Token;
        QString ClientName;
        QString TlsCa;
    };

    struct KelpieRuntime
    {
        std::vector<json>                            RegisteredListeners;
        std::vector<SessionItem>                     Sessions;
        std::vector<StockmanNamespace::RegisteredCommand> RegisteredCommands;
        std::vector<StockmanNamespace::RegisteredModule>  RegisteredModules;

        QStringList AddedCommands;
        QStringList IpAddresses;
    };

    using ConnectionInfo = KelpieConfig;
}

#endif // STOCKMAN_KELPIE_TYPES_HPP
