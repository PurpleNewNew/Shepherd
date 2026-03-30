#ifndef STOCKMAN_UTIL_THEMEMANAGER_HPP
#define STOCKMAN_UTIL_THEMEMANAGER_HPP

#include <QString>
#include <QMainWindow>
#include <QMenuBar>
#include <QTabWidget>

#include <Util/UiConstants.hpp>

namespace StockmanNamespace::Util {

struct ThemeResources
{
    QString darkStyle = QString::fromLatin1(StockmanNamespace::UiConstants::kStyleDark);
    QString lightStyle = QString::fromLatin1(StockmanNamespace::UiConstants::kStyleLight);
};

class ThemeManager
{
public:
    static ThemeManager& Instance();

    void ApplyStockmanChrome(QMainWindow* window,
                             QMenuBar* menuBar,
                             QTabWidget* tabs) const;
    void ApplyWidget(QWidget* widget, const QString& resource) const;
    const ThemeResources& Resources() const { return resources_; }

private:
    ThemeManager() = default;
    ThemeResources resources_;
};

} // namespace StockmanNamespace::Util

#endif // STOCKMAN_UTIL_THEMEMANAGER_HPP
