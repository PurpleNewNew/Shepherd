#include <Util/Base.hpp>
#include <QGuiApplication>
#include <QApplication>
#include <QStyleHints>
#include <QStyleFactory>
#include <QStyle>
#include <QTimer>
#include <QEvent>
#include <QComboBox>
#include <QDateTimeEdit>
#include <QDoubleSpinBox>
#include <QFontMetrics>
#include <QLineEdit>
#include <QProcessEnvironment>
#include <QProcessEnvironment>
#include <QPushButton>
#include <QSettings>
#include <QSpinBox>
#include <QProcess>
#include <QToolButton>
#include <memory>
#include <algorithm>
#include <cmath>
#include <Util/Message.hpp>

auto FileRead( const QString& FilePath ) -> QByteArray
{
    auto path = FilePath.toStdString();

    if ( FilePath[ 0 ] != ':' )
    {
        if ( ! QFile::exists( FilePath ) ) {
            spdlog::error( "Failed to find file: {}", path );
            return QByteArray();
        }
    }

    QFile File( FilePath );
    if (!File.open( QIODevice::ReadOnly )) {
        spdlog::error( "Failed to open file: {}", path );
        return QByteArray();
    }

    QByteArray Content = File.readAll();
    File.close();
    return Content;
}

auto MessageBox( QString Title, QString Text, QMessageBox::Icon Icon ) -> void
{
    switch ( Icon )
    {
        case QMessageBox::Critical:
            StockmanNamespace::Ui::ShowError(Title, Text);
            break;
        case QMessageBox::Warning:
            StockmanNamespace::Ui::ShowWarn(Title, Text);
            break;
        default:
            StockmanNamespace::Ui::ShowInfo(Title, Text);
            break;
    }
}

auto WinVersionIcon( QString OSVersion, bool High ) -> QIcon
{
    if ( OSVersion.startsWith( "Windows 10" ) || OSVersion.startsWith( "Windows Server 2019" )|| OSVersion.startsWith( "Windows 2019 Server" ) )
    {
        spdlog::debug( "OSVersion is Windows 10" );

        if ( High )
            return QIcon( ":/images/win10-8-high" );
        else
            return QIcon( ":/images/win10-8" );
    }
    else if ( OSVersion.startsWith( "Windows XP" )  )
    {
        spdlog::debug( "OSVersion is Windows XP" );

        if ( High )
            return QIcon( ":/images/winxp-high" );
        else
            return QIcon( ":/images/winxp" );
    }
    if ( OSVersion.startsWith( "Windows 8" ) || OSVersion.startsWith( "Windows Server 2012" ) )
    {
        spdlog::debug( "OSVersion is Windows 8" );

        if ( High )
            return QIcon( ":/images/win10-8-high" );
        else
            return QIcon( ":/images/win10-8" );
    }
    if ( OSVersion.startsWith( "Windows 11" )  )
    {
        spdlog::debug( "OSVersion is Windows 11" );

        if ( High )
            return QIcon( ":/images/win11-high" );
        else
            return QIcon( ":/images/win11" );
    }
    if ( OSVersion.startsWith( "Windows 7" ) || OSVersion.startsWith( "Windows Vista" ) )
    {
        spdlog::debug( "OSVersion is Windows 7 or Vista" );

        if ( High )
            return QIcon( ":/images/win7-vista-high" );
        else
            return QIcon( ":/images/win7-vista" );
    }
    if ( OSVersion.startsWith( "MacOS" )  )
    {
        spdlog::debug( "OSVersion is MacOS" );

        if ( High )
            return QIcon( ":/images/macos-high" );
        else
            return QIcon( ":/images/macos" );
    }
    if ( OSVersion.startsWith( "Linux" )  )
    {
        spdlog::debug( "OSVersion is Linux" );

        if ( High )
            return QIcon( ":/images/linux-high" );
        else
            return QIcon( ":/images/linux" );
    }
    else
    {
        if ( High )
            return QIcon( ":/images/unknown-high" );
        else
            return QIcon( ":/images/unknown" );
    }
}

auto WinVersionImage( QString OSVersion, bool High ) -> QImage
{
    if ( OSVersion.startsWith( "Windows 10" )  )
    {
        spdlog::debug( "OSVersion is Windows 10" );

        if ( High )
            return QImage( ":/images/win10-8-high" );
        else
            return QImage( ":/images/win10-8" );
    }
    else if ( OSVersion.startsWith( "Windows XP" )  )
    {
        spdlog::debug( "OSVersion is Windows XP" );

        if ( High )
            return QImage( ":/images/winxp-high" );
        else
            return QImage( ":/images/winxp" );
    }
    if ( OSVersion.startsWith( "Windows 8" )  )
    {
        spdlog::debug( "OSVersion is Windows 8" );

        if ( High )
            return QImage( ":/images/win10-8-high" );
        else
            return QImage( ":/images/win10-8" );
    }
    if ( OSVersion.startsWith( "Windows 11" )  )
    {
        spdlog::debug( "OSVersion is Windows 11" );

        if ( High )
            return QImage( ":/images/win11-high" );
        else
            return QImage( ":/images/win11" );
    }
    if ( OSVersion.startsWith( "Windows 7" ) || OSVersion.startsWith( "Windows Vista" ) )
    {
        spdlog::debug( "OSVersion is Windows 7 or Vista" );

        if ( High )
            return QImage( ":/images/win7-vista-high" );
        else
            return QImage( ":/images/win7-vista" );
    }
    if ( OSVersion.startsWith( "MacOS" )  )
    {
        spdlog::debug( "OSVersion is MacOS" );

        if ( High )
            return QImage( ":/images/macos-high" );
        else
            return QImage( ":/images/macos" );
    }
    if ( OSVersion.startsWith( "Linux" )  )
    {
        spdlog::debug( "OSVersion is Linux" );

        if ( High )
            return QImage( ":/images/linux-high" );
        else
            return QImage( ":/images/linux" );
    }
    else
    {
        if ( High )
            return QImage( ":/images/unknown-high" );
        else
            return QImage( ":/images/unknown" );
    }
}

auto CurrentDateTime(
    void
) -> QString {
    return ( QDateTime::currentDateTime().toString( "dd/MM/yyyy" ) + " " + CurrentTime() );
}

auto CurrentTime(
    void
) -> QString {
    return ( QTime::currentTime().toString( "hh:mm:ss" ) );
}

auto GrayScale(
    QImage image
) -> QImage {
    auto im = image.convertToFormat( QImage::Format_ARGB32 );

    for ( int y = 0; y < im.height(); ++y ) {
        auto scanLine = ( QRgb* ) im.scanLine( y );

        for ( int x = 0; x < im.width(); ++x ) {
            auto pixel = *scanLine;
            auto ci    = uint( qGray( pixel ) );

            *scanLine = qRgba( ci, ci, ci, qAlpha( pixel ) / 3 );
            ++scanLine;
        }
    }

    return im;
}

// --- Theme helpers ---

namespace
{
class ThemeWatcher : public QObject
{
public:
    ThemeWatcher(std::function<void()> fn, QObject* parent)
        : QObject(parent), refreshFn_(std::move(fn)), lastDark_(StockmanNamespace::Util::IsDarkMode()) {}

protected:
    bool eventFilter(QObject* object, QEvent* event) override
    {
        switch (event->type())
        {
            case QEvent::ApplicationPaletteChange:
            case QEvent::ThemeChange:
                scheduleRefreshIfNeeded();
                break;
            default:
                break;
        }
        return QObject::eventFilter(object, event);
    }

private:
    void scheduleRefreshIfNeeded()
    {
        if (!refreshFn_) { return; }
        // Avoid re-entrancy and style recursion:
        // applying the global theme triggers palette/style events synchronously.
        if (pending_) { return; }
        pending_ = true;
        QTimer::singleShot(0, this, [this]() {
            // Keep pending_ true while invoking refreshFn_ so events emitted during
            // the refresh won't recursively schedule again.
            const bool nowDark = StockmanNamespace::Util::IsDarkMode();
            if (nowDark != lastDark_)
            {
                lastDark_ = nowDark;
                if (refreshFn_) { refreshFn_(); }
            }
            pending_ = false;
        });
    }

    std::function<void()> refreshFn_;
    bool pending_ = false;
    bool lastDark_ = false;
};
} // namespace

bool StockmanNamespace::Util::IsDarkMode()
{
    // 1) 环境变量优先：STOCKMAN_THEME=dark/light 或 FORCE_* 直接覆盖。
    const auto themeEnv = qEnvironmentVariable("STOCKMAN_THEME").trimmed().toLower();
    if (themeEnv == "dark") { return true; }
    if (themeEnv == "light") { return false; }
    if (qEnvironmentVariableIsSet("STOCKMAN_FORCE_DARK")) { return true; }
    if (qEnvironmentVariableIsSet("STOCKMAN_FORCE_LIGHT")) { return false; }

#if defined(Q_OS_WIN)
    QSettings settings(
        R"(HKEY_CURRENT_USER\Software\Microsoft\Windows\CurrentVersion\Themes\Personalize)",
        QSettings::NativeFormat);
    if (settings.contains("AppsUseLightTheme"))
    {
        return settings.value("AppsUseLightTheme").toInt() == 0;
    }
#elif defined(Q_OS_MACOS)
    QSettings settings("Apple Global Domain", "AppleInterfaceStyle");
    const auto style = settings.value("AppleInterfaceStyle").toString();
    if (!style.isEmpty())
    {
        return style.compare("Dark", Qt::CaseInsensitive) == 0;
    }
#elif defined(Q_OS_LINUX)
    {
        QProcess proc;
        proc.start("gsettings", {"get", "org.gnome.desktop.interface", "color-scheme"});
        if (proc.waitForFinished(80))
        {
            const auto out = QString::fromUtf8(proc.readAllStandardOutput()).trimmed().toLower();
            if (out.contains("dark")) { return true; }
            if (out.contains("default") || out.contains("light")) { return false; }
        }
    }
    {
        QProcess proc;
        proc.start("gsettings", {"get", "org.gnome.desktop.interface", "gtk-theme"});
        if (proc.waitForFinished(80))
        {
            const auto out = QString::fromUtf8(proc.readAllStandardOutput()).trimmed().toLower();
            if (out.contains("dark")) { return true; }
        }
    }
#endif

    // 4) Fallback: 根据当前调色板亮度判断。
    const auto windowColor = QGuiApplication::palette().color(QPalette::Window);
    const auto textColor   = QGuiApplication::palette().color(QPalette::WindowText);
    return windowColor.lightness() < textColor.lightness();
}

int StockmanNamespace::Util::UiScalePx(int basePx)
{
    if (basePx <= 0)
    {
        return basePx;
    }

    auto* app = qobject_cast<QApplication*>(QCoreApplication::instance());
    if (!app)
    {
        return basePx;
    }

    const QFontMetrics metrics(app->font());
    if (metrics.height() <= 0)
    {
        return basePx;
    }

    const double scale = std::clamp(static_cast<double>(metrics.height()) / 16.0, 0.90, 1.45);
    return std::max(1, static_cast<int>(std::lround(static_cast<double>(basePx) * scale)));
}

void StockmanNamespace::Util::NormalizeInteractiveControls(QWidget* root)
{
    if (!root)
    {
        return;
    }

    const QFontMetrics metrics(root->font());
    const int controlHeight = std::max(UiScalePx(28), metrics.height() + UiScalePx(8));
    const int buttonMinWidth = std::max(UiScalePx(96), metrics.horizontalAdvance(QStringLiteral("Connect")) + UiScalePx(20));

    for (auto* btn : root->findChildren<QPushButton*>())
    {
        if (!btn || btn->property("stockmanFixedSize").toBool())
        {
            continue;
        }
        btn->setFixedHeight(controlHeight);
        if (btn->minimumWidth() < buttonMinWidth)
        {
            btn->setMinimumWidth(buttonMinWidth);
        }
    }

    for (auto* btn : root->findChildren<QToolButton*>())
    {
        if (!btn || btn->property("stockmanFixedSize").toBool())
        {
            continue;
        }
        const bool iconOnly = btn->text().trimmed().isEmpty();
        btn->setFixedHeight(controlHeight);
        if (iconOnly)
        {
            btn->setFixedWidth(controlHeight);
        }
        else if (btn->minimumWidth() < buttonMinWidth)
        {
            btn->setMinimumWidth(buttonMinWidth);
        }
    }

    for (auto* edit : root->findChildren<QLineEdit*>())
    {
        if (edit)
        {
            edit->setFixedHeight(controlHeight);
        }
    }

    for (auto* combo : root->findChildren<QComboBox*>())
    {
        if (combo)
        {
            combo->setFixedHeight(controlHeight);
        }
    }

    for (auto* spin : root->findChildren<QSpinBox*>())
    {
        if (spin)
        {
            spin->setFixedHeight(controlHeight);
        }
    }

    for (auto* spin : root->findChildren<QDoubleSpinBox*>())
    {
        if (spin)
        {
            spin->setFixedHeight(controlHeight);
        }
    }

    for (auto* edit : root->findChildren<QDateTimeEdit*>())
    {
        if (edit)
        {
            edit->setFixedHeight(controlHeight);
        }
    }
}

void StockmanNamespace::Util::ApplyStyle(QWidget* w, const QString& resourcePath)
{
    if (!w || resourcePath.isEmpty()) { return; }
    w->setStyleSheet( FileRead(resourcePath) );
}

void StockmanNamespace::Util::ApplyMessageBoxStyle(QMessageBox& box)
{
    if (IsDarkMode()) {
        box.setStyleSheet( FileRead( ":/stylesheets/MessageBox" ) );
    } else {
        box.setStyleSheet(QString());
    }
}

void StockmanNamespace::Util::ApplyFileDialogStyle(QFileDialog& dlg)
{
    if (IsDarkMode()) {
        dlg.setStyleSheet( FileRead( ":/stylesheets/Dialogs/FileDialog" ) );
    } else {
        dlg.setStyleSheet(QString());
    }
}

void StockmanNamespace::Util::ApplyStyleRecursive(QWidget* root, const QString& resourcePath)
{
    if (!root) return;
    ApplyStyle(root, resourcePath);
    for (auto* child : root->findChildren<QWidget*>()) {
        ApplyStyle(child, resourcePath);
    }
}

void StockmanNamespace::Util::RegisterThemeAutoRefresh(QObject* owner, std::function<void()> refreshFn)
{
    if (!owner || !refreshFn) return;
    if (auto* guiApp = qobject_cast<QGuiApplication*>(QCoreApplication::instance())) {
        auto callback = std::make_shared<std::function<void()>>(std::move(refreshFn));

        auto watcher = new ThemeWatcher(*callback, owner);
        guiApp->installEventFilter(watcher);

        // 轮询检测系统主题变化，兼容部分平台不发 palette 事件的情况
        auto* timer = new QTimer(owner);
        timer->setInterval(800);
        auto lastDark = std::make_shared<bool>(StockmanNamespace::Util::IsDarkMode());
        QObject::connect(timer, &QTimer::timeout, owner, [lastDark, callback]() {
            const bool nowDark = StockmanNamespace::Util::IsDarkMode();
            if (nowDark != *lastDark) {
                *lastDark = nowDark;
                if (callback && *callback) {
                    (*callback)();
                }
            }
        });
        timer->start();
    }
}

void StockmanNamespace::Util::ApplyGlobalDraculaTheme()
{
    auto* app = qobject_cast<QApplication*>(QCoreApplication::instance());
    if (!app) { return; }
    // Setting style/palette emits palette/theme events synchronously. If the caller
    // is itself running from a theme watcher, we must avoid re-entrancy.
    static thread_local bool applying = false;
    if (applying) { return; }
    applying = true;
    const auto guard = std::unique_ptr<void, void(*)(void*)>(nullptr, [](void*) { applying = false; });
    static bool styleInitialized = false;
    static QString originalStyleName;
    static QPalette originalPalette;
    static QString originalStyleSheet;

    if (!styleInitialized)
    {
        originalStyleName  = app->style() ? app->style()->objectName() : QString();
        originalPalette    = app->palette();
        originalStyleSheet = app->styleSheet();
        styleInitialized = true;
    }

    const bool useDark = StockmanNamespace::Util::IsDarkMode();
    // 为了视觉一致，两个主题都用 Fusion 基础样式，避免不同平台几何差异
    app->setStyle(QStyleFactory::create("Fusion"));

    if (useDark)
    {
        // Dracula 调色板
        QPalette palette;
        palette.setColor(QPalette::Window, QColor("#282a36"));
        palette.setColor(QPalette::WindowText, QColor("#f8f8f2"));
        palette.setColor(QPalette::Base, QColor("#1e1f29"));
        palette.setColor(QPalette::AlternateBase, QColor("#44475a"));
        palette.setColor(QPalette::ToolTipBase, QColor("#44475a"));
        palette.setColor(QPalette::ToolTipText, QColor("#f8f8f2"));
        palette.setColor(QPalette::Text, QColor("#f8f8f2"));
        palette.setColor(QPalette::Button, QColor("#44475a"));
        palette.setColor(QPalette::ButtonText, QColor("#f8f8f2"));
        palette.setColor(QPalette::BrightText, QColor("#ff5555"));
        palette.setColor(QPalette::Highlight, QColor("#6272a4"));
        palette.setColor(QPalette::HighlightedText, QColor("#f8f8f2"));
        palette.setColor(QPalette::Link, QColor("#8be9fd"));
        palette.setColor(QPalette::LinkVisited, QColor("#bd93f9"));
        app->setPalette(palette);

        // 组合主要 QSS 资源，避免各控件掉样式
        app->setStyleSheet(FileRead(":/stylesheets/Dark"));
    }
    else
    {
        // 亮色调色板 + 亮色样式
        QPalette palette;
        palette.setColor(QPalette::Window, QColor("#f4f5f8"));
        palette.setColor(QPalette::WindowText, QColor("#1f2330"));
        palette.setColor(QPalette::Base, QColor("#ffffff"));
        palette.setColor(QPalette::AlternateBase, QColor("#eef1f7"));
        palette.setColor(QPalette::ToolTipBase, QColor("#ffffff"));
        palette.setColor(QPalette::ToolTipText, QColor("#1f2330"));
        palette.setColor(QPalette::Text, QColor("#1f2330"));
        palette.setColor(QPalette::Button, QColor("#e8ecf4"));
        palette.setColor(QPalette::ButtonText, QColor("#1f2330"));
        palette.setColor(QPalette::BrightText, QColor("#d00000"));
        palette.setColor(QPalette::Highlight, QColor("#c7d2fe"));
        palette.setColor(QPalette::HighlightedText, QColor("#1f2330"));
        palette.setColor(QPalette::Link, QColor("#3b82f6"));
        palette.setColor(QPalette::LinkVisited, QColor("#6366f1"));
        app->setPalette(palette);

        app->setStyleSheet(FileRead(":/stylesheets/Light"));
    }
}
