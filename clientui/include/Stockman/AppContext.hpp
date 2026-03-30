#ifndef STOCKMAN_APP_CONTEXT_HPP
#define STOCKMAN_APP_CONTEXT_HPP

#include <memory>
#include <mutex>

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
        StockmanNamespace::Util::KelpieRuntime           kelpieRuntime;
        std::unique_ptr<StockmanNamespace::Grpc::Client> grpcClient;
        std::shared_ptr<StockmanNamespace::KelpieController> kelpieController;
        std::shared_ptr<StockmanNamespace::KelpieState>  kelpieState;
        StockmanNamespace::UserInterface::StockmanUi*    ui = nullptr;
        StockmanNamespace::StockmanSpace::Stockman*      app = nullptr;
        std::shared_ptr<StockmanNamespace::StockmanSpace::DBManager> dbManager;
        std::unique_ptr<UiResourceRegistry>              uiRegistry;
        std::mutex                                       runtimeMutex; // guards kelpie runtime registries
        bool                                             debugMode = false;
        bool                                             gateGUI = false;

        // Clear cached Kelpie runtime state; does not touch gRPC connections unless explicitly asked。
        void ClearRuntime(bool disconnectKelpie = false);
    };
}

namespace StockmanNamespace {
// RAII guard for accessing runtime registries with a mutex held.
struct RuntimeAccess {
    RuntimeAccess() = default;
    explicit RuntimeAccess(StockmanNamespace::AppContext* ctx) {
        if (ctx != nullptr) {
            guard = std::unique_lock<std::mutex>(ctx->runtimeMutex);
            runtime = &ctx->kelpieRuntime;
        }
    }

    explicit operator bool() const { return runtime != nullptr; }
    StockmanNamespace::Util::KelpieRuntime* operator->() { return runtime; }
    StockmanNamespace::Util::KelpieRuntime& operator*() { return *runtime; }

private:
    std::unique_lock<std::mutex> guard;
    StockmanNamespace::Util::KelpieRuntime* runtime = nullptr;
};
} // namespace StockmanNamespace

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
