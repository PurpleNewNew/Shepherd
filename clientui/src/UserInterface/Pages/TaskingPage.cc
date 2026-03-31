#include <UserInterface/Pages/TaskingPage.hpp>

#include <QHeaderView>
#include <QHBoxLayout>
#include <QLabel>
#include <QTabWidget>
#include <QVBoxLayout>

#include "proto/kelpieui/v1/kelpieui.pb.h"

namespace StockmanNamespace::UserInterface
{
    TaskingPage::TaskingPage(QWidget* parent)
        : QWidget(parent)
    {
        auto* opsLayout = new QVBoxLayout(this);
        opsLayout->setContentsMargins(0, 0, 0, 0);

        auto* taskingTabs = new QTabWidget(this);
        taskingTabs->setDocumentMode(true);

        auto* sleepPage = new QWidget(this);
        auto* sleepPageLayout = new QVBoxLayout(sleepPage);
        sleepPageLayout->setContentsMargins(0, 0, 0, 0);
        auto* sleepLayout = new QHBoxLayout();
        sleepSecondsInput = new QLineEdit(sleepPage);
        sleepSecondsInput->setPlaceholderText(tr("Sleep seconds"));
        workSecondsInput = new QLineEdit(sleepPage);
        workSecondsInput->setPlaceholderText(tr("Work seconds"));
        jitterInput = new QLineEdit(sleepPage);
        jitterInput->setPlaceholderText(tr("Jitter %"));
        refreshSleepButton = new QPushButton(tr("Refresh Sleep"), sleepPage);
        updateSleepButton = new QPushButton(tr("Update Sleep"), sleepPage);
        sleepLayout->addWidget(new QLabel(tr("Sleep:"), sleepPage));
        sleepLayout->addWidget(sleepSecondsInput);
        sleepLayout->addWidget(new QLabel(tr("Work:"), sleepPage));
        sleepLayout->addWidget(workSecondsInput);
        sleepLayout->addWidget(new QLabel(tr("Jitter:"), sleepPage));
        sleepLayout->addWidget(jitterInput);
        sleepLayout->addWidget(updateSleepButton);
        sleepLayout->addWidget(refreshSleepButton);
        sleepPageLayout->addLayout(sleepLayout);
        sleepPageLayout->addStretch();
        taskingTabs->addTab(sleepPage, tr("Sleep"));

        auto* dialPage = new QWidget(this);
        auto* dialPageLayout = new QVBoxLayout(dialPage);
        dialPageLayout->setContentsMargins(0, 0, 0, 0);
        auto* dialLayout = new QHBoxLayout();
        dialAddressInput = new QLineEdit(dialPage);
        dialAddressInput->setPlaceholderText(tr("Dial address (host:port)"));
        dialReasonInput = new QLineEdit(dialPage);
        dialReasonInput->setPlaceholderText(tr("Reason (optional)"));
        startDialButton = new QPushButton(tr("Start Dial"), dialPage);
        cancelDialButton = new QPushButton(tr("Cancel Dial"), dialPage);
        dialLayout->addWidget(dialAddressInput);
        dialLayout->addWidget(dialReasonInput);
        dialLayout->addWidget(startDialButton);
        dialLayout->addWidget(cancelDialButton);
        dialPageLayout->addLayout(dialLayout);
        dialTable = new QTableWidget(dialPage);
        dialTable->setColumnCount(5);
        dialTable->setHorizontalHeaderLabels({tr("Dial ID"), tr("Target"), tr("Address"), tr("State"), tr("Reason/Error")});
        dialTable->horizontalHeader()->setStretchLastSection(true);
        dialTable->setEditTriggers(QAbstractItemView::NoEditTriggers);
        dialPageLayout->addWidget(dialTable, 1);
        taskingTabs->addTab(dialPage, tr("Dial"));

        auto* sshPage = new QWidget(this);
        auto* sshPageLayout = new QVBoxLayout(sshPage);
        sshPageLayout->setContentsMargins(0, 0, 0, 0);
        sshTable = new QTableWidget(sshPage);
        sshTable->setColumnCount(4);
        sshTable->setHorizontalHeaderLabels({tr("Type"), tr("Target"), tr("Server/Port"), tr("Status")});
        sshTable->horizontalHeader()->setStretchLastSection(true);
        sshTable->setEditTriggers(QAbstractItemView::NoEditTriggers);
        sshPageLayout->addWidget(sshTable, 1);

        auto* sshLayout = new QHBoxLayout();
        sshServerInput = new QLineEdit(sshPage);
        sshServerInput->setPlaceholderText(tr("SSH server (host:port)"));
        sshAuthCombo = new QComboBox(sshPage);
        sshAuthCombo->addItem(tr("Password"), kelpieui::v1::SSH_TUNNEL_AUTH_METHOD_PASSWORD);
        sshAuthCombo->addItem(tr("Cert"), kelpieui::v1::SSH_TUNNEL_AUTH_METHOD_CERT);
        sshUserInput = new QLineEdit(sshPage);
        sshUserInput->setPlaceholderText(tr("Username"));
        sshPassInput = new QLineEdit(sshPage);
        sshPassInput->setPlaceholderText(tr("Password / PrivateKey"));
        sshPassInput->setEchoMode(QLineEdit::Password);
        sshTunnelPortInput = new QLineEdit(sshPage);
        sshTunnelPortInput->setPlaceholderText(tr("Agent port (for tunnel)"));
        startSshSessionButton = new QPushButton(tr("Start SSH Session"), sshPage);
        startSshTunnelButton = new QPushButton(tr("Start SSH Tunnel"), sshPage);
        sshLayout->addWidget(sshServerInput);
        sshLayout->addWidget(sshAuthCombo);
        sshLayout->addWidget(sshUserInput);
        sshLayout->addWidget(sshPassInput);
        sshLayout->addWidget(sshTunnelPortInput);
        sshLayout->addWidget(startSshSessionButton);
        sshLayout->addWidget(startSshTunnelButton);
        sshPageLayout->addLayout(sshLayout);
        taskingTabs->addTab(sshPage, tr("SSH"));

        opsLayout->addWidget(taskingTabs, 1);
    }
}
