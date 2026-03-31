#include <Stockman/AppContext.hpp>
#include <Stockman/KelpieController.hpp>
#include <Stockman/KelpieState.hpp>

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
    if ( uiRegistry )
    {
        uiRegistry->CleanupAll();
    }
    if ( disconnectKelpie && kelpieController )
    {
        kelpieController->Disconnect();
    }
}
