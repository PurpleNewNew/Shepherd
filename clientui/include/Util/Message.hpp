#ifndef STOCKMAN_UTIL_MESSAGE_HPP
#define STOCKMAN_UTIL_MESSAGE_HPP

#include <QMessageBox>
#include <QString>

#include <Util/Base.hpp>

namespace StockmanNamespace::Ui
{
// Lightweight wrapper to standardize styled message boxes.
inline void ShowMessage(QMessageBox::Icon icon,
                        const QString& title,
                        const QString& text,
                        QWidget* parent = nullptr)
{
    QMessageBox box(parent);
    box.setIcon(icon);
    box.setWindowTitle(title);
    box.setText(text);
    StockmanNamespace::Util::ApplyMessageBoxStyle(box);
    box.exec();
}

inline void ShowInfo(const QString& title, const QString& text, QWidget* parent = nullptr)
{
    ShowMessage(QMessageBox::Information, title, text, parent);
}

inline void ShowWarn(const QString& title, const QString& text, QWidget* parent = nullptr)
{
    ShowMessage(QMessageBox::Warning, title, text, parent);
}

inline void ShowError(const QString& title, const QString& text, QWidget* parent = nullptr)
{
    ShowMessage(QMessageBox::Critical, title, text, parent);
}

inline bool ConfirmYesNo(const QString& title,
                         const QString& text,
                         QWidget* parent = nullptr,
                         QMessageBox::StandardButton defaultButton = QMessageBox::No)
{
    QMessageBox box(parent);
    box.setIcon(QMessageBox::Question);
    box.setWindowTitle(title);
    box.setText(text);
    box.setStandardButtons(QMessageBox::Yes | QMessageBox::No);
    box.setDefaultButton(defaultButton);
    StockmanNamespace::Util::ApplyMessageBoxStyle(box);
    return box.exec() == QMessageBox::Yes;
}

inline bool ConfirmOkCancel(const QString& title,
                            const QString& text,
                            const QString& informative = {},
                            QWidget* parent = nullptr,
                            QMessageBox::StandardButton defaultButton = QMessageBox::Ok)
{
    QMessageBox box(parent);
    box.setIcon(QMessageBox::Question);
    box.setWindowTitle(title);
    box.setText(text);
    if (!informative.isEmpty()) {
        box.setInformativeText(informative);
    }
    box.setStandardButtons(QMessageBox::Ok | QMessageBox::Cancel);
    box.setDefaultButton(defaultButton);
    StockmanNamespace::Util::ApplyMessageBoxStyle(box);
    return box.exec() == QMessageBox::Ok;
}
} // namespace StockmanNamespace::Ui

#endif // STOCKMAN_UTIL_MESSAGE_HPP
