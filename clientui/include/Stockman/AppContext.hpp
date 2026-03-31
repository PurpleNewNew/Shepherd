#ifndef STOCKMAN_APP_CONTEXT_HPP
#define STOCKMAN_APP_CONTEXT_HPP

#include <memory>

#include <Stockman/Forward.hpp>
#include <Stockman/UiRegistry.hpp>
#include <Stockman/KelpieTypes.hpp>

namespace StockmanNamespace
{
    struct AppContext
    {
        AppContext();
        ~AppContext();

        StockmanNamespace::Util::KelpieConfig            kelpieConfig;
        std::unique_ptr<StockmanNamespace::Grpc::Client> grpcClient;
        std::shared_ptr<StockmanNamespace::KelpieController> kelpieController;
        std::shared_ptr<StockmanNamespace::KelpieState>  kelpieState;
        StockmanNamespace::UserInterface::StockmanUi*    ui = nullptr;
        StockmanNamespace::StockmanSpace::Stockman*      app = nullptr;
        std::shared_ptr<StockmanNamespace::StockmanSpace::DBManager> dbManager;
        std::unique_ptr<UiResourceRegistry>              uiRegistry;
        bool                                             debugMode = false;
        bool                                             gateGUI = false;

        // Clear cached UI-side runtime state; does not touch gRPC connections unless explicitly asked。
        void ClearRuntime(bool disconnectKelpie = false);
    };
}

inline StockmanNamespace::UiResourceRegistry* UiRegistry(StockmanNamespace::AppContext* ctx) {
    if (ctx == nullptr) {
        return nullptr;
    }
    if (!ctx->uiRegistry) {
        ctx->uiRegistry = std::make_unique<StockmanNamespace::UiResourceRegistry>();
    }
    return ctx->uiRegistry.get();
}

#endif // STOCKMAN_APP_CONTEXT_HPP
