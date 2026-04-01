#ifndef STOCKMAN_SHELL_PAGE_HPP
#define STOCKMAN_SHELL_PAGE_HPP

#include <QCheckBox>
#include <QTableWidget>
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
        QLineEdit* browsePathInput = nullptr;
        QPushButton* browseGoButton = nullptr;
        QPushButton* browseUpButton = nullptr;
        QPushButton* browseRefreshButton = nullptr;
        QTableWidget* browseTable = nullptr;
        QLabel* browseStatusLabel = nullptr;
        QPushButton* downloadButton = nullptr;
        QLabel* downloadStatusLabel = nullptr;
        QString remoteRootPath;
        QString remoteResolvedPath;
        QString remoteParentPath;
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
