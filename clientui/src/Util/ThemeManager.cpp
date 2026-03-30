#include <Util/ThemeManager.hpp>
#include <Util/Base.hpp>

using StockmanNamespace::Util::ThemeManager;
using StockmanNamespace::Util::ThemeResources;
using namespace StockmanNamespace::UiConstants;

ThemeManager& ThemeManager::Instance()
{
    static ThemeManager instance;
    return instance;
}

void ThemeManager::ApplyStockmanChrome(QMainWindow* window,
                                       QMenuBar* menuBar,
                                       QTabWidget* tabs) const
{
    const auto style = StockmanNamespace::Util::IsDarkMode()
        ? resources_.darkStyle
        : resources_.lightStyle;
    ApplyWidget(window, style);
    ApplyWidget(menuBar, style);
    ApplyWidget(tabs, style);
}

void ThemeManager::ApplyWidget(QWidget* widget, const QString& resource) const
{
    StockmanNamespace::Util::ApplyStyle(widget, resource);
}
