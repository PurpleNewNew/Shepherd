#ifndef STOCKMAN_FORWARD_HPP
#define STOCKMAN_FORWARD_HPP

#include <string>

namespace StockmanNamespace
{
    extern std::string Version;
    extern std::string CodeName;

    class UiResourceRegistry;

    namespace Grpc { class Client; }

    class KelpieController;
    class KelpieState;

    namespace UserInterface
    {
        class StockmanUi;
        namespace Dialogs {
            class Connect;
        }
    }

    namespace StockmanSpace {
        class DBManager;
        class Stockman;
    }
}

#endif // STOCKMAN_FORWARD_HPP
