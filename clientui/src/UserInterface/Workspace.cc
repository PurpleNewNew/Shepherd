#include <UserInterface/KelpiePanel.hpp>

#include <QHeaderView>

namespace StockmanNamespace::UserInterface
{
    void KelpiePanel::buildStreamsWorkspaceTab()
    {
        streamsPage_ = new QWidget(this);
        auto* streamsLayout = new QVBoxLayout(streamsPage_);
        streamsLayout->setContentsMargins(0, 0, 0, 0);

        streamsTable_ = new QTableWidget(streamsPage_);
        streamsTable_->setColumnCount(6);
        streamsTable_->setHorizontalHeaderLabels({tr("ID"), tr("Target"), tr("Kind"), tr("Pending"), tr("Inflight"), tr("Window")});
        streamsTable_->horizontalHeader()->setStretchLastSection(true);
        streamsTable_->setEditTriggers(QAbstractItemView::NoEditTriggers);
        streamsLayout->addWidget(streamsTable_, 1);

        auto* streamActionLayout = new QHBoxLayout();
        streamActionLayout->setContentsMargins(0, 0, 0, 0);
        closeStreamReasonInput_ = new QLineEdit(streamsPage_);
        closeStreamReasonInput_->setPlaceholderText(tr("Close reason (optional)"));
        closeStreamButton_ = new QPushButton(tr("Close Selected"), streamsPage_);
        closeStreamButton_->setEnabled(false);
        streamActionLayout->addWidget(closeStreamReasonInput_, 1);
        streamActionLayout->addWidget(closeStreamButton_);
        streamsLayout->addLayout(streamActionLayout);

        workspaceTabs_->addTab(streamsPage_, tr("Traffic"));
    }

    void KelpiePanel::buildConsoleWorkspaceTab()
    {
        consolePage_ = new QWidget(this);
        auto* consoleLayout = new QVBoxLayout(consolePage_);
        consoleLayout->setContentsMargins(0, 0, 0, 0);

        logView_ = new QPlainTextEdit(consolePage_);
        logView_->setReadOnly(true);
        logView_->document()->setMaximumBlockCount(5000);
        consoleLayout->addWidget(logView_, 1);

        refreshProxiesButton_ = new QPushButton(tr("Refresh Pivoting"), consolePage_);
        auto* adminButtonLayout = new QHBoxLayout();
        adminButtonLayout->addWidget(refreshProxiesButton_);
        adminButtonLayout->addStretch();
        consoleLayout->addLayout(adminButtonLayout);

        workspaceTabs_->addTab(consolePage_, tr("Ops Console"));
    }

    void KelpiePanel::buildChatWorkspaceTab()
    {
        chatPage_ = new QWidget(this);
        auto* chatLayout = new QVBoxLayout(chatPage_);
        chatLayout->setContentsMargins(0, 0, 0, 0);

        chatTable_ = new QTableWidget(chatPage_);
        chatTable_->setColumnCount(4);
        chatTable_->setHorizontalHeaderLabels({tr("Time"), tr("User"), tr("Role"), tr("Message")});
        chatTable_->horizontalHeader()->setStretchLastSection(true);
        chatTable_->setEditTriggers(QAbstractItemView::NoEditTriggers);
        chatLayout->addWidget(chatTable_, 1);

        auto* chatControlLayout = new QHBoxLayout();
        chatInput_ = new QLineEdit(chatPage_);
        chatInput_->setPlaceholderText(tr("Message to operators"));
        sendChatButton_ = new QPushButton(tr("Send"), chatPage_);
        refreshChatButton_ = new QPushButton(tr("Refresh"), chatPage_);
        chatControlLayout->addWidget(chatInput_, 1);
        chatControlLayout->addWidget(sendChatButton_);
        chatControlLayout->addWidget(refreshChatButton_);
        chatLayout->addLayout(chatControlLayout);

        workspaceTabs_->addTab(chatPage_, tr("Collab"));
    }

    void KelpiePanel::buildLootWorkspaceTab()
    {
        lootPage_ = new QWidget(this);
        auto* lootLayout = new QVBoxLayout(lootPage_);
        lootLayout->setContentsMargins(0, 0, 0, 0);

        auto* lootControlLayout = new QHBoxLayout();
        lootTargetInput_ = new QLineEdit(lootPage_);
        lootTargetInput_->setPlaceholderText(tr("Target UUID (optional)"));
        lootTagInput_ = new QLineEdit(lootPage_);
        lootTagInput_->setPlaceholderText(tr("Tags (comma separated)"));
        lootCategoryBox_ = new QComboBox(lootPage_);
        lootCategoryBox_->addItem(tr("All"), kelpieui::v1::LOOT_CATEGORY_UNSPECIFIED);
        lootCategoryBox_->addItem(tr("File"), kelpieui::v1::LOOT_CATEGORY_FILE);
        lootCategoryBox_->addItem(tr("Screenshot"), kelpieui::v1::LOOT_CATEGORY_SCREENSHOT);
        lootCategoryBox_->addItem(tr("Ticket"), kelpieui::v1::LOOT_CATEGORY_TICKET);
        lootLimitSpin_ = new QSpinBox(lootPage_);
        lootLimitSpin_->setRange(1, 500);
        lootLimitSpin_->setValue(100);
        refreshLootButton_ = new QPushButton(tr("Refresh"), lootPage_);
        submitLootButton_ = new QPushButton(tr("Submit File"), lootPage_);
        downloadLootButton_ = new QPushButton(tr("Download Selected"), lootPage_);
        lootControlLayout->addWidget(new QLabel(tr("Target:"), lootPage_));
        lootControlLayout->addWidget(lootTargetInput_);
        lootControlLayout->addWidget(new QLabel(tr("Tags:"), lootPage_));
        lootControlLayout->addWidget(lootTagInput_);
        lootControlLayout->addWidget(new QLabel(tr("Category:"), lootPage_));
        lootControlLayout->addWidget(lootCategoryBox_);
        lootControlLayout->addWidget(new QLabel(tr("Limit:"), lootPage_));
        lootControlLayout->addWidget(lootLimitSpin_);
        lootControlLayout->addWidget(refreshLootButton_);
        lootControlLayout->addWidget(submitLootButton_);
        lootControlLayout->addWidget(downloadLootButton_);
        lootLayout->addLayout(lootControlLayout);

        lootTable_ = new QTableWidget(lootPage_);
        lootTable_->setColumnCount(6);
        lootTable_->setHorizontalHeaderLabels({tr("Created"), tr("Name"), tr("Category"), tr("Target"), tr("Size"), tr("Tags")});
        lootTable_->horizontalHeader()->setStretchLastSection(true);
        lootTable_->setEditTriggers(QAbstractItemView::NoEditTriggers);
        lootLayout->addWidget(lootTable_, 1);

        workspaceTabs_->addTab(lootPage_, tr("Loot"));
    }

    void KelpiePanel::buildShellWorkspaceTab()
    {
        shellPage_ = new QWidget(this);
        auto* shellLayout = new QVBoxLayout(shellPage_);
        shellLayout->setContentsMargins(0, 0, 0, 0);

        shellStatusLabel_ = new QLabel(tr("Shell: disconnected"), shellPage_);
        openShellButton_ = new QPushButton(tr("Open Shell"), shellPage_);
        closeShellButton_ = new QPushButton(tr("Close Shell"), shellPage_);
        closeShellButton_->setEnabled(false);
        shellView_ = new QPlainTextEdit(shellPage_);
        shellView_->setReadOnly(true);
        shellView_->document()->setMaximumBlockCount(5000);
        shellView_->setMinimumHeight(120);
        shellInput_ = new QLineEdit(shellPage_);
        shellInput_->setPlaceholderText(tr("Type shell command and press Enter"));
        shellInput_->setEnabled(false);

        auto* shellControlLayout = new QHBoxLayout();
        shellControlLayout->addWidget(shellStatusLabel_);
        shellControlLayout->addStretch();
        shellControlLayout->addWidget(openShellButton_);
        shellControlLayout->addWidget(closeShellButton_);
        shellLayout->addLayout(shellControlLayout);

        opsStatusLabel_ = new QLabel(shellPage_);
        shellLayout->addWidget(opsStatusLabel_);
        shellLayout->addWidget(shellView_, 1);
        shellLayout->addWidget(shellInput_);

        auto* downloadRow = new QHBoxLayout();
        downloadRemotePathInput_ = new QLineEdit(shellPage_);
        downloadRemotePathInput_->setPlaceholderText(tr("Download remote path"));
        downloadLocalPathInput_ = new QLineEdit(shellPage_);
        downloadLocalPathInput_->setPlaceholderText(tr("Download local path"));
        browseDownloadButton_ = new QPushButton(tr("Browse"), shellPage_);
        startDownloadButton_ = new QPushButton(tr("Start Download"), shellPage_);
        downloadStatusLabel_ = new QLabel(tr("Download: idle"), shellPage_);
        downloadRow->addWidget(new QLabel(tr("Download:"), shellPage_));
        downloadRow->addWidget(downloadRemotePathInput_, 1);
        downloadRow->addWidget(downloadLocalPathInput_, 1);
        downloadRow->addWidget(browseDownloadButton_);
        downloadRow->addWidget(startDownloadButton_);
        shellLayout->addLayout(downloadRow);
        shellLayout->addWidget(downloadStatusLabel_);

        auto* uploadRow = new QHBoxLayout();
        uploadLocalPathInput_ = new QLineEdit(shellPage_);
        uploadLocalPathInput_->setPlaceholderText(tr("Upload local file"));
        uploadRemotePathInput_ = new QLineEdit(shellPage_);
        uploadRemotePathInput_->setPlaceholderText(tr("Upload remote path"));
        browseUploadButton_ = new QPushButton(tr("Browse"), shellPage_);
        uploadOverwriteCheck_ = new QCheckBox(tr("Overwrite"), shellPage_);
        startUploadButton_ = new QPushButton(tr("Start Upload"), shellPage_);
        uploadStatusLabel_ = new QLabel(tr("Upload: idle"), shellPage_);
        uploadRow->addWidget(new QLabel(tr("Upload:"), shellPage_));
        uploadRow->addWidget(uploadLocalPathInput_, 1);
        uploadRow->addWidget(uploadRemotePathInput_, 1);
        uploadRow->addWidget(browseUploadButton_);
        uploadRow->addWidget(uploadOverwriteCheck_);
        uploadRow->addWidget(startUploadButton_);
        shellLayout->addLayout(uploadRow);
        shellLayout->addWidget(uploadStatusLabel_);

        auto* socksRow = new QHBoxLayout();
        socksPortInput_ = new QLineEdit(shellPage_);
        socksPortInput_->setPlaceholderText(tr("SOCKS local port"));
        socksPortInput_->setText(QStringLiteral("1080"));
        socksAuthCheck_ = new QCheckBox(tr("Auth"), shellPage_);
        socksUserInput_ = new QLineEdit(shellPage_);
        socksUserInput_->setPlaceholderText(tr("Username"));
        socksPasswordInput_ = new QLineEdit(shellPage_);
        socksPasswordInput_->setPlaceholderText(tr("Password"));
        socksPasswordInput_->setEchoMode(QLineEdit::Password);
        startSocksButton_ = new QPushButton(tr("Start SOCKS"), shellPage_);
        stopSocksButton_ = new QPushButton(tr("Stop SOCKS"), shellPage_);
        stopSocksButton_->setEnabled(false);
        socksStatusLabel_ = new QLabel(tr("SOCKS: stopped"), shellPage_);
        socksRow->addWidget(new QLabel(tr("SOCKS:"), shellPage_));
        socksRow->addWidget(socksPortInput_);
        socksRow->addWidget(socksAuthCheck_);
        socksRow->addWidget(socksUserInput_);
        socksRow->addWidget(socksPasswordInput_);
        socksRow->addWidget(startSocksButton_);
        socksRow->addWidget(stopSocksButton_);
        socksRow->addWidget(socksStatusLabel_, 1);
        shellLayout->addLayout(socksRow);

        workspaceTabs_->addTab(shellPage_, tr("Beacon Shell"));
    }

}
