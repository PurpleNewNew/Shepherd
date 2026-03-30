#ifndef STOCKMAN_BASE_HPP
#define STOCKMAN_BASE_HPP

#include <spdlog/spdlog.h>

#include <QString>
#include <QFile>
#include <QIcon>
#include <QMessageBox>
#include <QDateTime>
#include <QTime>
#include <QWidget>
#include <QFileDialog>
#include <QPalette>
#include <QStyleHints>

auto FileRead(
    const QString& FilePath
) -> QByteArray;

auto MessageBox(
    QString           Title,
    QString           Text,
    QMessageBox::Icon Icon
) -> void;

auto WinVersionIcon(
    QString OSVersion,
    bool    High
) -> QIcon;

auto WinVersionImage(
    QString OSVersion,
    bool    High
) -> QImage;

auto GrayScale(
    QImage image
) -> QImage;

auto CurrentDateTime(
    
) -> QString;

auto CurrentTime(
        
) -> QString;

namespace StockmanNamespace::Util
{
// 读取系统配色方案，true 表示暗色模式
bool IsDarkMode();

// 按当前字体/缩放计算像素值（用于高分屏下控件尺寸统一）。
int UiScalePx(int basePx);

// 统一页面中常见交互控件（按钮/输入框等）的最小尺寸。
void NormalizeInteractiveControls(QWidget* root);

void ApplyStyle(QWidget* w, const QString& resourcePath);
void ApplyMessageBoxStyle(QMessageBox& box);
void ApplyFileDialogStyle(QFileDialog& dlg);

// 递归应用样式，便于在主题切换时刷新子控件。
void ApplyStyleRecursive(QWidget* root, const QString& resourcePath);

// 注册随系统色板变化自动刷新主题；owner 销毁后自动断开。
void RegisterThemeAutoRefresh(QObject* owner, std::function<void()> refreshFn);

// 全局 Dracula 主题：设置 Fusion 风格、调色板与样式表；可重复调用以应对系统主题切换。
void ApplyGlobalDraculaTheme();
} // namespace StockmanNamespace::Util

#endif
