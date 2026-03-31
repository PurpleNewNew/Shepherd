#ifndef STOCKMAN_SHELL_PAGE_HPP
#define STOCKMAN_SHELL_PAGE_HPP

#include <QCheckBox>
#include <QLabel>
#include <QLineEdit>
#include <QPlainTextEdit>
#include <QPushButton>
#include <QWidget>

#include <memory>

namespace StockmanNamespace
{
    class ProxyStreamHandle;
}

namespace StockmanNamespace::UserInterface
{
    class ShellPage : public QWidget
    {
    public:
        explicit ShellPage(QWidget* parent = nullptr);

        QPlainTextEdit* view = nullptr;
        QLineEdit* input = nullptr;
        QPushButton* openButton = nullptr;
        QPushButton* closeButton = nullptr;
        QLabel* statusLabel = nullptr;
        QLabel* opsStatusLabel = nullptr;
        QString pendingTarget;
        QString pendingLine;
        std::shared_ptr<StockmanNamespace::ProxyStreamHandle> stream;
        QLineEdit* downloadRemotePathInput = nullptr;
        QLineEdit* downloadLocalPathInput = nullptr;
        QPushButton* browseDownloadButton = nullptr;
        QPushButton* startDownloadButton = nullptr;
        QLabel* downloadStatusLabel = nullptr;
        QLineEdit* uploadLocalPathInput = nullptr;
        QLineEdit* uploadRemotePathInput = nullptr;
        QCheckBox* uploadOverwriteCheck = nullptr;
        QPushButton* browseUploadButton = nullptr;
        QPushButton* startUploadButton = nullptr;
        QLabel* uploadStatusLabel = nullptr;
        QLineEdit* socksPortInput = nullptr;
        QCheckBox* socksAuthCheck = nullptr;
        QLineEdit* socksUserInput = nullptr;
        QLineEdit* socksPasswordInput = nullptr;
        QPushButton* startSocksButton = nullptr;
        QPushButton* stopSocksButton = nullptr;
        QLabel* socksStatusLabel = nullptr;
    };
}

#endif // STOCKMAN_SHELL_PAGE_HPP
