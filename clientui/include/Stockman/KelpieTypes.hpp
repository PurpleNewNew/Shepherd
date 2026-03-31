#ifndef STOCKMAN_KELPIE_TYPES_HPP
#define STOCKMAN_KELPIE_TYPES_HPP

#include <QString>

namespace StockmanNamespace::Util
{
    struct KelpieConfig
    {
        QString Name;
        QString Host;
        QString Port;
        QString Token;
        QString ClientName;
        QString TlsCa;
    };

    using ConnectionInfo = KelpieConfig;
}

#endif // STOCKMAN_KELPIE_TYPES_HPP
