#include <UserInterface/Pages/ShellPage.hpp>

#include <QHBoxLayout>
#include <QLabel>
#include <QVBoxLayout>

namespace StockmanNamespace::UserInterface
{
    ShellPage::ShellPage(QWidget* parent)
        : QWidget(parent)
    {
        auto* shellLayout = new QVBoxLayout(this);
        shellLayout->setContentsMargins(0, 0, 0, 0);

        statusLabel = new QLabel(tr("Shell: disconnected"), this);
        openButton = new QPushButton(tr("Open Shell"), this);
        closeButton = new QPushButton(tr("Close Shell"), this);
        closeButton->setEnabled(false);
        view = new QPlainTextEdit(this);
        view->setReadOnly(true);
        view->document()->setMaximumBlockCount(5000);
        view->setMinimumHeight(120);
        input = new QLineEdit(this);
        input->setPlaceholderText(tr("Type shell command and press Enter"));
        input->setEnabled(false);

        auto* shellControlLayout = new QHBoxLayout();
        shellControlLayout->addWidget(statusLabel);
        shellControlLayout->addStretch();
        shellControlLayout->addWidget(openButton);
        shellControlLayout->addWidget(closeButton);
        shellLayout->addLayout(shellControlLayout);

        opsStatusLabel = new QLabel(this);
        shellLayout->addWidget(opsStatusLabel);
        shellLayout->addWidget(view, 1);
        shellLayout->addWidget(input);

        auto* downloadRow = new QHBoxLayout();
        downloadRemotePathInput = new QLineEdit(this);
        downloadRemotePathInput->setPlaceholderText(tr("Download remote path"));
        downloadLocalPathInput = new QLineEdit(this);
        downloadLocalPathInput->setPlaceholderText(tr("Download local path"));
        browseDownloadButton = new QPushButton(tr("Browse"), this);
        startDownloadButton = new QPushButton(tr("Start Download"), this);
        downloadStatusLabel = new QLabel(tr("Download: idle"), this);
        downloadRow->addWidget(new QLabel(tr("Download:"), this));
        downloadRow->addWidget(downloadRemotePathInput, 1);
        downloadRow->addWidget(downloadLocalPathInput, 1);
        downloadRow->addWidget(browseDownloadButton);
        downloadRow->addWidget(startDownloadButton);
        shellLayout->addLayout(downloadRow);
        shellLayout->addWidget(downloadStatusLabel);

        auto* uploadRow = new QHBoxLayout();
        uploadLocalPathInput = new QLineEdit(this);
        uploadLocalPathInput->setPlaceholderText(tr("Upload local file"));
        uploadRemotePathInput = new QLineEdit(this);
        uploadRemotePathInput->setPlaceholderText(tr("Upload remote path"));
        browseUploadButton = new QPushButton(tr("Browse"), this);
        uploadOverwriteCheck = new QCheckBox(tr("Overwrite"), this);
        startUploadButton = new QPushButton(tr("Start Upload"), this);
        uploadStatusLabel = new QLabel(tr("Upload: idle"), this);
        uploadRow->addWidget(new QLabel(tr("Upload:"), this));
        uploadRow->addWidget(uploadLocalPathInput, 1);
        uploadRow->addWidget(uploadRemotePathInput, 1);
        uploadRow->addWidget(browseUploadButton);
        uploadRow->addWidget(uploadOverwriteCheck);
        uploadRow->addWidget(startUploadButton);
        shellLayout->addLayout(uploadRow);
        shellLayout->addWidget(uploadStatusLabel);

        auto* socksRow = new QHBoxLayout();
        socksPortInput = new QLineEdit(this);
        socksPortInput->setPlaceholderText(tr("SOCKS local port"));
        socksPortInput->setText(QStringLiteral("1080"));
        socksAuthCheck = new QCheckBox(tr("Auth"), this);
        socksUserInput = new QLineEdit(this);
        socksUserInput->setPlaceholderText(tr("Username"));
        socksPasswordInput = new QLineEdit(this);
        socksPasswordInput->setPlaceholderText(tr("Password"));
        socksPasswordInput->setEchoMode(QLineEdit::Password);
        startSocksButton = new QPushButton(tr("Start SOCKS"), this);
        stopSocksButton = new QPushButton(tr("Stop SOCKS"), this);
        stopSocksButton->setEnabled(false);
        socksStatusLabel = new QLabel(tr("SOCKS: stopped"), this);
        socksRow->addWidget(new QLabel(tr("SOCKS:"), this));
        socksRow->addWidget(socksPortInput);
        socksRow->addWidget(socksAuthCheck);
        socksRow->addWidget(socksUserInput);
        socksRow->addWidget(socksPasswordInput);
        socksRow->addWidget(startSocksButton);
        socksRow->addWidget(stopSocksButton);
        socksRow->addWidget(socksStatusLabel, 1);
        shellLayout->addLayout(socksRow);
    }
}
