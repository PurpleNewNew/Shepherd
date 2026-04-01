#include <UserInterface/Pages/ShellPage.hpp>

#include <QAbstractItemView>
#include <QHeaderView>
#include <QHBoxLayout>
#include <QLabel>
#include <QSplitter>
#include <QVBoxLayout>

namespace StockmanNamespace::UserInterface
{
    ShellPage::ShellPage(QWidget* parent)
        : QWidget(parent)
    {
        auto* rootLayout = new QVBoxLayout(this);
        rootLayout->setContentsMargins(0, 0, 0, 0);

        auto* split = new QSplitter(Qt::Vertical, this);
        split->setChildrenCollapsible(false);
        rootLayout->addWidget(split, 1);

        auto* shellPane = new QWidget(split);
        auto* shellLayout = new QVBoxLayout(shellPane);
        shellLayout->setContentsMargins(0, 0, 0, 0);

        statusLabel = new QLabel(tr("Shell: disconnected"), shellPane);
        openButton = new QPushButton(tr("Open Shell"), shellPane);
        closeButton = new QPushButton(tr("Close Shell"), shellPane);
        closeButton->setEnabled(false);
        view = new QPlainTextEdit(shellPane);
        view->setReadOnly(true);
        view->document()->setMaximumBlockCount(5000);
        view->setMinimumHeight(120);
        input = new QLineEdit(shellPane);
        input->setPlaceholderText(tr("Type shell command and press Enter"));
        input->setEnabled(false);

        auto* shellControlLayout = new QHBoxLayout();
        shellControlLayout->addWidget(statusLabel);
        shellControlLayout->addStretch();
        shellControlLayout->addWidget(openButton);
        shellControlLayout->addWidget(closeButton);
        shellLayout->addLayout(shellControlLayout);

        opsStatusLabel = new QLabel(shellPane);
        shellLayout->addWidget(opsStatusLabel);
        shellLayout->addWidget(view, 1);
        shellLayout->addWidget(input);

        auto* filePane = new QWidget(split);
        auto* fileLayout = new QVBoxLayout(filePane);
        fileLayout->setContentsMargins(0, 0, 0, 0);

        auto* browseRow = new QHBoxLayout();
        browsePathInput = new QLineEdit(filePane);
        browsePathInput->setPlaceholderText(tr("Remote directory or drive (blank = top level)"));
        browseGoButton = new QPushButton(tr("Go"), filePane);
        browseUpButton = new QPushButton(tr("Up"), filePane);
        browseRefreshButton = new QPushButton(tr("Refresh"), filePane);
        downloadButton = new QPushButton(tr("Download"), filePane);
        browseStatusLabel = new QLabel(tr("Files: idle"), filePane);
        downloadStatusLabel = new QLabel(tr("Download: idle"), filePane);
        browseRow->addWidget(new QLabel(tr("Files:"), filePane));
        browseRow->addWidget(browsePathInput, 1);
        browseRow->addWidget(browseGoButton);
        browseRow->addWidget(browseUpButton);
        browseRow->addWidget(browseRefreshButton);
        browseRow->addWidget(downloadButton);
        fileLayout->addLayout(browseRow);
        fileLayout->addWidget(browseStatusLabel);

        browseTable = new QTableWidget(filePane);
        browseTable->setColumnCount(4);
        browseTable->setHorizontalHeaderLabels({tr("Name"), tr("Type"), tr("Size"), tr("Modified")});
        browseTable->setEditTriggers(QAbstractItemView::NoEditTriggers);
        browseTable->setSelectionBehavior(QAbstractItemView::SelectRows);
        browseTable->setSelectionMode(QAbstractItemView::SingleSelection);
        browseTable->setAlternatingRowColors(true);
        browseTable->verticalHeader()->setVisible(false);
        browseTable->horizontalHeader()->setStretchLastSection(true);
        browseTable->horizontalHeader()->setSectionResizeMode(0, QHeaderView::Stretch);
        fileLayout->addWidget(browseTable, 1);
        fileLayout->addWidget(downloadStatusLabel);

        auto* uploadRow = new QHBoxLayout();
        uploadLocalPathInput = new QLineEdit(filePane);
        uploadLocalPathInput->setPlaceholderText(tr("Upload local file"));
        uploadRemotePathInput = new QLineEdit(filePane);
        uploadRemotePathInput->setPlaceholderText(tr("Upload remote path"));
        browseUploadButton = new QPushButton(tr("Browse"), filePane);
        uploadOverwriteCheck = new QCheckBox(tr("Overwrite"), filePane);
        startUploadButton = new QPushButton(tr("Start Upload"), filePane);
        uploadStatusLabel = new QLabel(tr("Upload: idle"), filePane);
        uploadRow->addWidget(new QLabel(tr("Upload:"), filePane));
        uploadRow->addWidget(uploadLocalPathInput, 1);
        uploadRow->addWidget(uploadRemotePathInput, 1);
        uploadRow->addWidget(browseUploadButton);
        uploadRow->addWidget(uploadOverwriteCheck);
        uploadRow->addWidget(startUploadButton);
        fileLayout->addLayout(uploadRow);
        fileLayout->addWidget(uploadStatusLabel);

        auto* socksRow = new QHBoxLayout();
        socksPortInput = new QLineEdit(filePane);
        socksPortInput->setPlaceholderText(tr("SOCKS local port"));
        socksPortInput->setText(QStringLiteral("1080"));
        socksAuthCheck = new QCheckBox(tr("Auth"), filePane);
        socksUserInput = new QLineEdit(filePane);
        socksUserInput->setPlaceholderText(tr("Username"));
        socksPasswordInput = new QLineEdit(filePane);
        socksPasswordInput->setPlaceholderText(tr("Password"));
        socksPasswordInput->setEchoMode(QLineEdit::Password);
        startSocksButton = new QPushButton(tr("Start SOCKS"), filePane);
        stopSocksButton = new QPushButton(tr("Stop SOCKS"), filePane);
        stopSocksButton->setEnabled(false);
        socksStatusLabel = new QLabel(tr("SOCKS: stopped"), filePane);
        socksRow->addWidget(new QLabel(tr("SOCKS:"), filePane));
        socksRow->addWidget(socksPortInput);
        socksRow->addWidget(socksAuthCheck);
        socksRow->addWidget(socksUserInput);
        socksRow->addWidget(socksPasswordInput);
        socksRow->addWidget(startSocksButton);
        socksRow->addWidget(stopSocksButton);
        socksRow->addWidget(socksStatusLabel, 1);
        fileLayout->addLayout(socksRow);

        split->setStretchFactor(0, 2);
        split->setStretchFactor(1, 3);
    }
}
