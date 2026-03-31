#include <UserInterface/KelpiePanel.hpp>

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
        workspace_.chatPage = new QWidget(this);
        auto* chatLayout = new QVBoxLayout(workspace_.chatPage);
        chatLayout->setContentsMargins(0, 0, 0, 0);

        workspace_.chatTable = new QTableWidget(workspace_.chatPage);
        workspace_.chatTable->setColumnCount(4);
        workspace_.chatTable->setHorizontalHeaderLabels({tr("Time"), tr("User"), tr("Role"), tr("Message")});
        workspace_.chatTable->horizontalHeader()->setStretchLastSection(true);
        workspace_.chatTable->setEditTriggers(QAbstractItemView::NoEditTriggers);
        chatLayout->addWidget(workspace_.chatTable, 1);

        auto* chatControlLayout = new QHBoxLayout();
        workspace_.chatInput = new QLineEdit(workspace_.chatPage);
        workspace_.chatInput->setPlaceholderText(tr("Message to operators"));
        workspace_.sendChatButton = new QPushButton(tr("Send"), workspace_.chatPage);
        workspace_.refreshChatButton = new QPushButton(tr("Refresh"), workspace_.chatPage);
        chatControlLayout->addWidget(workspace_.chatInput, 1);
        chatControlLayout->addWidget(workspace_.sendChatButton);
        chatControlLayout->addWidget(workspace_.refreshChatButton);
        chatLayout->addLayout(chatControlLayout);

        workspaceTabs_->addTab(workspace_.chatPage, tr("Collab"));
    }

    void KelpiePanel::buildLootWorkspaceTab()
    {
        workspace_.lootPage = new QWidget(this);
        auto* lootLayout = new QVBoxLayout(workspace_.lootPage);
        lootLayout->setContentsMargins(0, 0, 0, 0);

        auto* lootControlLayout = new QHBoxLayout();
        workspace_.lootTargetInput = new QLineEdit(workspace_.lootPage);
        workspace_.lootTargetInput->setPlaceholderText(tr("Target UUID (optional)"));
        workspace_.lootTagInput = new QLineEdit(workspace_.lootPage);
        workspace_.lootTagInput->setPlaceholderText(tr("Tags (comma separated)"));
        workspace_.lootCategoryBox = new QComboBox(workspace_.lootPage);
        workspace_.lootCategoryBox->addItem(tr("All"), kelpieui::v1::LOOT_CATEGORY_UNSPECIFIED);
        workspace_.lootCategoryBox->addItem(tr("File"), kelpieui::v1::LOOT_CATEGORY_FILE);
        workspace_.lootCategoryBox->addItem(tr("Screenshot"), kelpieui::v1::LOOT_CATEGORY_SCREENSHOT);
        workspace_.lootCategoryBox->addItem(tr("Ticket"), kelpieui::v1::LOOT_CATEGORY_TICKET);
        workspace_.lootLimitSpin = new QSpinBox(workspace_.lootPage);
        workspace_.lootLimitSpin->setRange(1, 500);
        workspace_.lootLimitSpin->setValue(100);
        workspace_.refreshLootButton = new QPushButton(tr("Refresh"), workspace_.lootPage);
        workspace_.submitLootButton = new QPushButton(tr("Submit File"), workspace_.lootPage);
        workspace_.downloadLootButton = new QPushButton(tr("Download Selected"), workspace_.lootPage);
        lootControlLayout->addWidget(new QLabel(tr("Target:"), workspace_.lootPage));
        lootControlLayout->addWidget(workspace_.lootTargetInput);
        lootControlLayout->addWidget(new QLabel(tr("Tags:"), workspace_.lootPage));
        lootControlLayout->addWidget(workspace_.lootTagInput);
        lootControlLayout->addWidget(new QLabel(tr("Category:"), workspace_.lootPage));
        lootControlLayout->addWidget(workspace_.lootCategoryBox);
        lootControlLayout->addWidget(new QLabel(tr("Limit:"), workspace_.lootPage));
        lootControlLayout->addWidget(workspace_.lootLimitSpin);
        lootControlLayout->addWidget(workspace_.refreshLootButton);
        lootControlLayout->addWidget(workspace_.submitLootButton);
        lootControlLayout->addWidget(workspace_.downloadLootButton);
        lootLayout->addLayout(lootControlLayout);

        workspace_.lootTable = new QTableWidget(workspace_.lootPage);
        workspace_.lootTable->setColumnCount(6);
        workspace_.lootTable->setHorizontalHeaderLabels({tr("Created"), tr("Name"), tr("Category"), tr("Target"), tr("Size"), tr("Tags")});
        workspace_.lootTable->horizontalHeader()->setStretchLastSection(true);
        workspace_.lootTable->setEditTriggers(QAbstractItemView::NoEditTriggers);
        lootLayout->addWidget(workspace_.lootTable, 1);

        workspaceTabs_->addTab(workspace_.lootPage, tr("Loot"));
    }

    void KelpiePanel::buildShellWorkspaceTab()
    {
        shell_.page = new QWidget(this);
        auto* shellLayout = new QVBoxLayout(shell_.page);
        shellLayout->setContentsMargins(0, 0, 0, 0);

        shell_.statusLabel = new QLabel(tr("Shell: disconnected"), shell_.page);
        shell_.openButton = new QPushButton(tr("Open Shell"), shell_.page);
        shell_.closeButton = new QPushButton(tr("Close Shell"), shell_.page);
        shell_.closeButton->setEnabled(false);
        shell_.view = new QPlainTextEdit(shell_.page);
        shell_.view->setReadOnly(true);
        shell_.view->document()->setMaximumBlockCount(5000);
        shell_.view->setMinimumHeight(120);
        shell_.input = new QLineEdit(shell_.page);
        shell_.input->setPlaceholderText(tr("Type shell command and press Enter"));
        shell_.input->setEnabled(false);

        auto* shellControlLayout = new QHBoxLayout();
        shellControlLayout->addWidget(shell_.statusLabel);
        shellControlLayout->addStretch();
        shellControlLayout->addWidget(shell_.openButton);
        shellControlLayout->addWidget(shell_.closeButton);
        shellLayout->addLayout(shellControlLayout);

        shell_.opsStatusLabel = new QLabel(shell_.page);
        shellLayout->addWidget(shell_.opsStatusLabel);
        shellLayout->addWidget(shell_.view, 1);
        shellLayout->addWidget(shell_.input);

        auto* downloadRow = new QHBoxLayout();
        shell_.downloadRemotePathInput = new QLineEdit(shell_.page);
        shell_.downloadRemotePathInput->setPlaceholderText(tr("Download remote path"));
        shell_.downloadLocalPathInput = new QLineEdit(shell_.page);
        shell_.downloadLocalPathInput->setPlaceholderText(tr("Download local path"));
        shell_.browseDownloadButton = new QPushButton(tr("Browse"), shell_.page);
        shell_.startDownloadButton = new QPushButton(tr("Start Download"), shell_.page);
        shell_.downloadStatusLabel = new QLabel(tr("Download: idle"), shell_.page);
        downloadRow->addWidget(new QLabel(tr("Download:"), shell_.page));
        downloadRow->addWidget(shell_.downloadRemotePathInput, 1);
        downloadRow->addWidget(shell_.downloadLocalPathInput, 1);
        downloadRow->addWidget(shell_.browseDownloadButton);
        downloadRow->addWidget(shell_.startDownloadButton);
        shellLayout->addLayout(downloadRow);
        shellLayout->addWidget(shell_.downloadStatusLabel);

        auto* uploadRow = new QHBoxLayout();
        shell_.uploadLocalPathInput = new QLineEdit(shell_.page);
        shell_.uploadLocalPathInput->setPlaceholderText(tr("Upload local file"));
        shell_.uploadRemotePathInput = new QLineEdit(shell_.page);
        shell_.uploadRemotePathInput->setPlaceholderText(tr("Upload remote path"));
        shell_.browseUploadButton = new QPushButton(tr("Browse"), shell_.page);
        shell_.uploadOverwriteCheck = new QCheckBox(tr("Overwrite"), shell_.page);
        shell_.startUploadButton = new QPushButton(tr("Start Upload"), shell_.page);
        shell_.uploadStatusLabel = new QLabel(tr("Upload: idle"), shell_.page);
        uploadRow->addWidget(new QLabel(tr("Upload:"), shell_.page));
        uploadRow->addWidget(shell_.uploadLocalPathInput, 1);
        uploadRow->addWidget(shell_.uploadRemotePathInput, 1);
        uploadRow->addWidget(shell_.browseUploadButton);
        uploadRow->addWidget(shell_.uploadOverwriteCheck);
        uploadRow->addWidget(shell_.startUploadButton);
        shellLayout->addLayout(uploadRow);
        shellLayout->addWidget(shell_.uploadStatusLabel);

        auto* socksRow = new QHBoxLayout();
        shell_.socksPortInput = new QLineEdit(shell_.page);
        shell_.socksPortInput->setPlaceholderText(tr("SOCKS local port"));
        shell_.socksPortInput->setText(QStringLiteral("1080"));
        shell_.socksAuthCheck = new QCheckBox(tr("Auth"), shell_.page);
        shell_.socksUserInput = new QLineEdit(shell_.page);
        shell_.socksUserInput->setPlaceholderText(tr("Username"));
        shell_.socksPasswordInput = new QLineEdit(shell_.page);
        shell_.socksPasswordInput->setPlaceholderText(tr("Password"));
        shell_.socksPasswordInput->setEchoMode(QLineEdit::Password);
        shell_.startSocksButton = new QPushButton(tr("Start SOCKS"), shell_.page);
        shell_.stopSocksButton = new QPushButton(tr("Stop SOCKS"), shell_.page);
        shell_.stopSocksButton->setEnabled(false);
        shell_.socksStatusLabel = new QLabel(tr("SOCKS: stopped"), shell_.page);
        socksRow->addWidget(new QLabel(tr("SOCKS:"), shell_.page));
        socksRow->addWidget(shell_.socksPortInput);
        socksRow->addWidget(shell_.socksAuthCheck);
        socksRow->addWidget(shell_.socksUserInput);
        socksRow->addWidget(shell_.socksPasswordInput);
        socksRow->addWidget(shell_.startSocksButton);
        socksRow->addWidget(shell_.stopSocksButton);
        socksRow->addWidget(shell_.socksStatusLabel, 1);
        shellLayout->addLayout(socksRow);

        workspaceTabs_->addTab(shell_.page, tr("Beacon Shell"));
    }

}
