#ifndef STOCKMAN_TASKING_PAGE_HPP
#define STOCKMAN_TASKING_PAGE_HPP

#include <QComboBox>
#include <QLineEdit>
#include <QPushButton>
#include <QTableWidget>
#include <QWidget>

#include <memory>
#include <vector>

namespace StockmanNamespace
{
    class ProxyStreamHandle;
}

namespace StockmanNamespace::UserInterface
{
    class TaskingPage : public QWidget
    {
    public:
        explicit TaskingPage(QWidget* parent = nullptr);

        QPushButton* refreshSleepButton = nullptr;
        QPushButton* updateSleepButton = nullptr;
        QLineEdit* sleepSecondsInput = nullptr;
        QLineEdit* workSecondsInput = nullptr;
        QLineEdit* jitterInput = nullptr;
        QLineEdit* dialAddressInput = nullptr;
        QLineEdit* dialReasonInput = nullptr;
        QPushButton* startDialButton = nullptr;
        QPushButton* cancelDialButton = nullptr;
        QTableWidget* dialTable = nullptr;
        QLineEdit* sshServerInput = nullptr;
        QLineEdit* sshUserInput = nullptr;
        QLineEdit* sshPassInput = nullptr;
        QComboBox* sshAuthCombo = nullptr;
        QPushButton* startSshSessionButton = nullptr;
        QPushButton* startSshTunnelButton = nullptr;
        QLineEdit* sshTunnelPortInput = nullptr;
        QTableWidget* sshTable = nullptr;
        std::vector<std::shared_ptr<StockmanNamespace::ProxyStreamHandle>> sshStreams;
    };
}

#endif // STOCKMAN_TASKING_PAGE_HPP
