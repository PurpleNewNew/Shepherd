#include <UserInterface/KelpiePanel.hpp>
#include <UserInterface/Pages/ChatPage.hpp>
#include <UserInterface/Pages/LootPage.hpp>
#include <UserInterface/Pages/ShellPage.hpp>

#include <QHeaderView>

namespace StockmanNamespace::UserInterface
{
    void KelpiePanel::buildStreamsWorkspaceTab()
    {
        streams_.page = new QWidget(this);
        auto* streamsLayout = new QVBoxLayout(streams_.page);
        streamsLayout->setContentsMargins(0, 0, 0, 0);

        streams_.table = new QTableWidget(streams_.page);
        streams_.table->setColumnCount(6);
        streams_.table->setHorizontalHeaderLabels({tr("ID"), tr("Target"), tr("Kind"), tr("Pending"), tr("Inflight"), tr("Window")});
        streams_.table->horizontalHeader()->setStretchLastSection(true);
        streams_.table->setEditTriggers(QAbstractItemView::NoEditTriggers);
        streamsLayout->addWidget(streams_.table, 1);

        auto* streamActionLayout = new QHBoxLayout();
        streamActionLayout->setContentsMargins(0, 0, 0, 0);
        streams_.closeReasonInput = new QLineEdit(streams_.page);
        streams_.closeReasonInput->setPlaceholderText(tr("Close reason (optional)"));
        streams_.closeButton = new QPushButton(tr("Close Selected"), streams_.page);
        streams_.closeButton->setEnabled(false);
        streamActionLayout->addWidget(streams_.closeReasonInput, 1);
        streamActionLayout->addWidget(streams_.closeButton);
        streamsLayout->addLayout(streamActionLayout);

        workspaceTabs_->addTab(streams_.page, tr("Traffic"));
    }

    void KelpiePanel::buildConsoleWorkspaceTab()
    {
        workspace_.consolePage = new QWidget(this);
        auto* consoleLayout = new QVBoxLayout(workspace_.consolePage);
        consoleLayout->setContentsMargins(0, 0, 0, 0);

        workspace_.logView = new QPlainTextEdit(workspace_.consolePage);
        workspace_.logView->setReadOnly(true);
        workspace_.logView->document()->setMaximumBlockCount(5000);
        consoleLayout->addWidget(workspace_.logView, 1);

        workspace_.refreshProxiesButton = new QPushButton(tr("Refresh Pivoting"), workspace_.consolePage);
        auto* adminButtonLayout = new QHBoxLayout();
        adminButtonLayout->addWidget(workspace_.refreshProxiesButton);
        adminButtonLayout->addStretch();
        consoleLayout->addLayout(adminButtonLayout);

        workspaceTabs_->addTab(workspace_.consolePage, tr("Ops Console"));
    }

    void KelpiePanel::buildChatWorkspaceTab()
    {
        chatPage_ = new ChatPage(this);
        workspaceTabs_->addTab(chatPage_, tr("Collab"));
    }

    void KelpiePanel::buildLootWorkspaceTab()
    {
        lootPage_ = new LootPage(this);
        workspaceTabs_->addTab(lootPage_, tr("Loot"));
    }

    void KelpiePanel::buildShellWorkspaceTab()
    {
        shellPage_ = new ShellPage(this);
        workspaceTabs_->addTab(shellPage_, tr("Beacon Shell"));
    }

}
