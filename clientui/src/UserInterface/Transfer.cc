#include <UserInterface/KelpiePanel.hpp>
#include <UserInterface/Pages/ShellPage.hpp>

#include "Internal.hpp"

#include <QFileDialog>
#include <QFileInfo>
#include <QMetaObject>
#include <QTableWidgetItem>
#include <thread>
#include <utility>

namespace StockmanNamespace::UserInterface
{
    namespace
    {
        constexpr int kRemotePathRole = Qt::UserRole;
        constexpr int kRemoteIsDirRole = Qt::UserRole + 1;

        QString remoteEntryTypeText(const kelpieui::v1::RemoteFileEntry& entry)
        {
            if ( entry.is_drive() )
            {
                return QObject::tr("Drive");
            }
            if ( entry.is_dir() && entry.is_symlink() )
            {
                return QObject::tr("Dir Link");
            }
            if ( entry.is_dir() )
            {
                return QObject::tr("Directory");
            }
            if ( entry.is_symlink() )
            {
                return QObject::tr("Symlink");
            }
            return QObject::tr("File");
        }

        QString joinRemotePath(const QString& dirPath, const QString& name)
        {
            const QString cleanDir = dirPath.trimmed();
            const QString cleanName = name.trimmed();
            if ( cleanDir.isEmpty() )
            {
                return cleanName;
            }
            if ( cleanName.isEmpty() )
            {
                return cleanDir;
            }
            const QChar sep = cleanDir.contains('\\') ? QChar('\\') : QChar('/');
            if ( cleanDir.endsWith('/') || cleanDir.endsWith('\\') )
            {
                return cleanDir + cleanName;
            }
            return cleanDir + sep + cleanName;
        }
    }

    void KelpiePanel::refreshRemoteFiles(const QString& path)
    {
        auto* ctrl = controller();
        if ( ctrl == nullptr )
        {
            return;
        }
        if ( currentNodeUuid_.isEmpty() )
        {
            return;
        }
        if ( shellPage_ == nullptr )
        {
            return;
        }
        const QString requestedPath = !path.trimmed().isEmpty()
                                          ? path.trimmed()
                                          : ((shellPage_->browsePathInput != nullptr) ? shellPage_->browsePathInput->text().trimmed() : QString());
        const QString targetUuid = currentNodeUuid_;
        const uint64_t epoch = ctrl->ConnectionEpoch();
        const uint64_t generation = remoteBrowseGeneration_.fetch_add(1, std::memory_order_relaxed) + 1;

        setWidgetsEnabled({shellPage_->browsePathInput, shellPage_->browseGoButton, shellPage_->browseUpButton, shellPage_->browseRefreshButton, shellPage_->browseTable, shellPage_->downloadButton}, false);
        if ( shellPage_->browseStatusLabel != nullptr )
        {
            shellPage_->browseStatusLabel->setText(tr("Files: loading..."));
        }

        struct Result {
            uint64_t epoch{0};
            uint64_t generation{0};
            QString targetUuid;
            bool ok{false};
            QString error;
            kelpieui::v1::ListRemoteFilesResponse listing;
        };

        runAsyncGuarded<Result>(
            this,
            [ctrl, epoch, generation, targetUuid, requestedPath]() {
                Result res;
                res.epoch = epoch;
                res.generation = generation;
                res.targetUuid = targetUuid;
                QString error;
                kelpieui::v1::ListRemoteFilesResponse listing;
                res.ok = ctrl->ListRemoteFiles(targetUuid, requestedPath, &listing, error);
                res.error = error;
                res.listing = listing;
                return res;
            },
            [this]() {
                if ( shellPage_ == nullptr )
                {
                    return;
                }
                const bool enabled = !currentNodeUuid_.isEmpty();
                if ( shellPage_->browsePathInput != nullptr ) { shellPage_->browsePathInput->setEnabled(enabled);
}
                if ( shellPage_->browseGoButton != nullptr ) { shellPage_->browseGoButton->setEnabled(enabled);
}
                if ( shellPage_->browseRefreshButton != nullptr ) { shellPage_->browseRefreshButton->setEnabled(enabled);
}
                if ( shellPage_->browseTable != nullptr ) { shellPage_->browseTable->setEnabled(enabled);
}
                updateRemoteFileSelection();
            },
            [this](const Result& res) {
                auto* ctrl = controller();
                return (ctrl != nullptr) &&
                       (ctrl->ConnectionEpoch() == res.epoch) &&
                       (res.targetUuid == currentNodeUuid_) &&
                       (res.generation == remoteBrowseGeneration_.load(std::memory_order_relaxed));
            },
            [this](const Result& res) {
                if ( shellPage_ != nullptr && shellPage_->browseStatusLabel != nullptr )
                {
                    shellPage_->browseStatusLabel->setText(tr("Files: failed (%1)").arg(res.error));
                }
                toastError(tr("Remote file browse failed: %1").arg(res.error));
            },
            [this](const Result& res) {
                if ( shellPage_ == nullptr || shellPage_->browseTable == nullptr )
                {
                    return;
                }
                shellPage_->remoteRootPath = QString::fromStdString(res.listing.root_path());
                shellPage_->remoteResolvedPath = QString::fromStdString(res.listing.resolved_path());
                shellPage_->remoteParentPath = QString::fromStdString(res.listing.parent_path());
                remoteBrowsePathByNode_.insert(res.targetUuid, shellPage_->remoteResolvedPath);
                if ( shellPage_->browsePathInput != nullptr )
                {
                    shellPage_->browsePathInput->setText(shellPage_->remoteResolvedPath);
                }
                const QString displayPath = QString::fromStdString(res.listing.display_path()).trimmed();
                const QString statusPath = !displayPath.isEmpty()
                                               ? displayPath
                                               : (shellPage_->remoteResolvedPath.trimmed().isEmpty() ? tr("Top Level") : shellPage_->remoteResolvedPath);
                if ( shellPage_->browseStatusLabel != nullptr )
                {
                    shellPage_->browseStatusLabel->setText(
                        tr("Files: %1 (%2 items)")
                            .arg(statusPath,
                                 QString::number(res.listing.entries_size())));
                }
                shellPage_->browseTable->setRowCount(0);
                for ( int i = 0; i < res.listing.entries_size(); ++i )
                {
                    const auto& entry = res.listing.entries(i);
                    const int row = shellPage_->browseTable->rowCount();
                    shellPage_->browseTable->insertRow(row);

                    auto* nameItem = new QTableWidgetItem(QString::fromStdString(entry.name()));
                    nameItem->setData(kRemotePathRole, QString::fromStdString(entry.path()));
                    nameItem->setData(kRemoteIsDirRole, entry.is_dir());
                    if ( entry.is_dir() || entry.is_drive() )
                    {
                        QFont font = nameItem->font();
                        font.setBold(true);
                        nameItem->setFont(font);
                    }
                    shellPage_->browseTable->setItem(row, 0, nameItem);
                    shellPage_->browseTable->setItem(row, 1, new QTableWidgetItem(remoteEntryTypeText(entry)));
                    shellPage_->browseTable->setItem(row, 2, new QTableWidgetItem(entry.is_dir() ? QStringLiteral("-") : QString::number(entry.size())));
                    shellPage_->browseTable->setItem(row, 3, new QTableWidgetItem(QString::fromStdString(entry.modified_at())));
                }
                if ( shellPage_->browseTable->rowCount() > 0 )
                {
                    shellPage_->browseTable->setCurrentCell(0, 0);
                }
                if ( shellPage_->browseUpButton != nullptr )
                {
                    shellPage_->browseUpButton->setEnabled(res.listing.can_go_up());
                }
                updateRemoteFileSelection();
            });
    }

    void KelpiePanel::browseRemoteFilesUp()
    {
        if ( shellPage_ == nullptr )
        {
            return;
        }
        const QString parent = shellPage_->remoteParentPath.trimmed();
        if ( parent.isEmpty() )
        {
            return;
        }
        refreshRemoteFiles(parent);
    }

    void KelpiePanel::openSelectedRemoteEntry()
    {
        if ( shellPage_ == nullptr || shellPage_->browseTable == nullptr )
        {
            return;
        }
        const int row = shellPage_->browseTable->currentRow();
        if ( row < 0 )
        {
            return;
        }
        auto* item = shellPage_->browseTable->item(row, 0);
        if ( item == nullptr )
        {
            return;
        }
        const QString path = item->data(kRemotePathRole).toString().trimmed();
        if ( path.isEmpty() )
        {
            return;
        }
        if ( item->data(kRemoteIsDirRole).toBool() )
        {
            refreshRemoteFiles(path);
            return;
        }
        collectRemoteFileToLootPath(path);
    }

    void KelpiePanel::updateRemoteFileSelection()
    {
        if ( shellPage_ == nullptr )
        {
            return;
        }
        const bool enabled = !selectedRemoteFilePath().isEmpty();
        if ( shellPage_->downloadButton != nullptr )
        {
            shellPage_->downloadButton->setEnabled(enabled);
        }
        if ( shellPage_->browseUpButton != nullptr && !shellPage_->remoteParentPath.trimmed().isEmpty() )
        {
            shellPage_->browseUpButton->setEnabled(shellPage_->browseTable != nullptr && shellPage_->browseTable->isEnabled());
        }
    }

    QString KelpiePanel::selectedRemoteFilePath() const
    {
        if ( shellPage_ == nullptr || shellPage_->browseTable == nullptr )
        {
            return {};
        }
        const int row = shellPage_->browseTable->currentRow();
        if ( row < 0 )
        {
            return {};
        }
        auto* item = shellPage_->browseTable->item(row, 0);
        if ( item == nullptr || item->data(kRemoteIsDirRole).toBool() )
        {
            return {};
        }
        return item->data(kRemotePathRole).toString().trimmed();
    }

    void KelpiePanel::collectRemoteFileToLoot()
    {
        const QString remotePath = selectedRemoteFilePath();
        if ( remotePath.isEmpty() )
        {
            toastWarn(tr("Select a file first"));
            return;
        }
        collectRemoteFileToLootPath(remotePath);
    }

    void KelpiePanel::collectRemoteFileToLootPath(const QString& remotePath)
    {
        auto* ctrl = controller();
        if ( ctrl == nullptr )
        {
            toastWarn(tr("gRPC client not connected"));
            return;
        }
        if ( currentNodeUuid_.isEmpty() )
        {
            toastWarn(tr("Select a node first"));
            return;
        }
        const QString trimmedPath = remotePath.trimmed();
        if ( trimmedPath.isEmpty() )
        {
            toastWarn(tr("Remote path is required"));
            return;
        }
        if ( shellPage_ == nullptr )
        {
            return;
        }
        setWidgetsEnabled({shellPage_->downloadButton, shellPage_->browseTable}, false);
        if ( shellPage_->downloadStatusLabel != nullptr )
        {
            shellPage_->downloadStatusLabel->setText(tr("Download: starting..."));
        }
        toastInfo(tr("Downloading %1 into Loot...").arg(QFileInfo(trimmedPath).fileName()));
        const uint64_t epoch = ctrl->ConnectionEpoch();
        const QString targetUuid = currentNodeUuid_;

        struct Result {
            uint64_t epoch{0};
            QString targetUuid;
            bool ok{false};
            QString error;
            kelpieui::v1::LootItem item;
        };

        runAsyncGuarded<Result>(
            this,
            [ctrl, epoch, targetUuid, trimmedPath]() {
                Result res;
                res.epoch = epoch;
                res.targetUuid = targetUuid;
                QString error;
                kelpieui::v1::LootItem item;
                res.ok = ctrl->CollectLootFile(targetUuid, trimmedPath, {}, &item, error);
                res.error = error;
                res.item = item;
                return res;
            },
            [this]() {
                if ( shellPage_ == nullptr )
                {
                    return;
                }
                if ( shellPage_->browseTable != nullptr ) { shellPage_->browseTable->setEnabled(!currentNodeUuid_.isEmpty());
}
                updateRemoteFileSelection();
            },
            [this](const Result& res) {
                auto* ctrl = controller();
                return (ctrl != nullptr) &&
                       (ctrl->ConnectionEpoch() == res.epoch) &&
                       (res.targetUuid == currentNodeUuid_);
            },
            [this](const Result& res) {
                if ( shellPage_ != nullptr && shellPage_->downloadStatusLabel != nullptr )
                {
                    shellPage_->downloadStatusLabel->setText(tr("Download: failed (%1)").arg(res.error));
                }
                toastError(tr("Download to Loot failed: %1").arg(res.error));
            },
            [this](const Result& res) {
                if ( shellPage_ != nullptr && shellPage_->downloadStatusLabel != nullptr )
                {
                    shellPage_->downloadStatusLabel->setText(
                        tr("Download: stored in Loot (%1)").arg(QString::fromStdString(res.item.name())));
                }
                toastInfo(tr("Downloaded to Loot: %1").arg(QString::fromStdString(res.item.name())));
                refreshLoot();
            });
    }

    void KelpiePanel::browseUploadPath()
    {
        const QString path = QFileDialog::getOpenFileName(this, tr("Select file to upload"));
        if ( path.isEmpty() )
        {
            return;
        }
        if ( shellPage_ != nullptr && shellPage_->uploadLocalPathInput != nullptr )
        {
            shellPage_->uploadLocalPathInput->setText(path);
        }
        if ( (shellPage_ != nullptr) && (shellPage_->uploadRemotePathInput != nullptr) && shellPage_->uploadRemotePathInput->text().trimmed().isEmpty() )
        {
            const QString baseName = QFileInfo(path).fileName();
            const QString defaultRemotePath =
                (shellPage_->remoteResolvedPath.trimmed().isEmpty())
                    ? baseName
                    : joinRemotePath(shellPage_->remoteResolvedPath, baseName);
            shellPage_->uploadRemotePathInput->setText(defaultRemotePath);
        }
    }

    void KelpiePanel::startUploadFile()
    {
        if ( uploadActive_.load() )
        {
            return;
        }
        if ( currentNodeUuid_.isEmpty() )
        {
            toastWarn(tr("Select a node first"));
            return;
        }
        QString localPath = (shellPage_ != nullptr && shellPage_->uploadLocalPathInput != nullptr) ? shellPage_->uploadLocalPathInput->text().trimmed() : QString();
        if ( localPath.isEmpty() )
        {
            browseUploadPath();
            localPath = (shellPage_ != nullptr && shellPage_->uploadLocalPathInput != nullptr) ? shellPage_->uploadLocalPathInput->text().trimmed() : QString();
        }
        if ( localPath.isEmpty() )
        {
            return;
        }
        const QString remotePath = (shellPage_ != nullptr && shellPage_->uploadRemotePathInput != nullptr) ? shellPage_->uploadRemotePathInput->text().trimmed() : QString();
        if ( remotePath.isEmpty() )
        {
            const QString defaultRemotePath =
                (shellPage_ != nullptr)
                    ? joinRemotePath(shellPage_->remoteResolvedPath, QFileInfo(localPath).fileName())
                    : QFileInfo(localPath).fileName();
            if ( shellPage_ != nullptr && shellPage_->uploadRemotePathInput != nullptr )
            {
                shellPage_->uploadRemotePathInput->setText(defaultRemotePath);
            }
            if ( defaultRemotePath.isEmpty() )
            {
                toastWarn(tr("Upload remote path is required"));
                return;
            }
        }
        stopUpload();

        uploadActive_.store(true);
        const uint64_t generation = uploadGeneration_.fetch_add(1, std::memory_order_relaxed) + 1;
        if ( shellPage_ != nullptr && shellPage_->startUploadButton != nullptr ) { shellPage_->startUploadButton->setEnabled(false);
}
        if ( shellPage_ != nullptr && shellPage_->uploadStatusLabel != nullptr ) { shellPage_->uploadStatusLabel->setText(tr("Upload: starting..."));
}
        toastInfo(tr("Upload started: %1").arg(QFileInfo(localPath).fileName()));

        auto ctx = context_;
        const QString targetUuid = currentNodeUuid_;
        StockmanNamespace::KelpieController::FileUploadSpec spec;
        spec.localPath = localPath;
        spec.remotePath = (shellPage_ != nullptr && shellPage_->uploadRemotePathInput != nullptr)
                              ? shellPage_->uploadRemotePathInput->text().trimmed()
                              : remotePath;
        spec.overwrite = (shellPage_ != nullptr && shellPage_->uploadOverwriteCheck != nullptr) ? shellPage_->uploadOverwriteCheck->isChecked() : false;

        uploadThread_ = std::jthread([this, ctx, targetUuid, spec, generation](std::stop_token stopToken) {
            QString error;
            bool ok = false;
            auto* ctrl = ctx ? ctx->kelpieController.get() : nullptr;
            if ( ctrl != nullptr )
            {
                ok = ctrl->UploadFileDataplane(
                    targetUuid,
                    spec,
                    error,
                    [this, generation](qint64 done, qint64 total) {
                        QMetaObject::invokeMethod(
                            this,
                            [this, generation, done, total]() {
                                if ( generation != uploadGeneration_.load(std::memory_order_relaxed) )
                                {
                                    return;
                                }
                                if ( shellPage_ != nullptr && shellPage_->uploadStatusLabel )
                                {
                                    shellPage_->uploadStatusLabel->setText(tr("Upload: %1 / %2").arg(done).arg(total));
                                }
                            },
                            Qt::QueuedConnection);
                    },
                    std::move(stopToken));
            }
            else
            {
                error = tr("Kelpie controller unavailable");
            }
            QMetaObject::invokeMethod(
                this,
                [this, generation, ok, error]() {
                    if ( generation != uploadGeneration_.load(std::memory_order_relaxed) )
                    {
                        return;
                    }
                    finishUpload(ok, error);
                },
                Qt::QueuedConnection);
        });
    }

    void KelpiePanel::finishUpload(bool success, const QString& errorMessage)
    {
        uploadActive_.store(false);
        if ( shellPage_ != nullptr && shellPage_->startUploadButton != nullptr )
        {
            shellPage_->startUploadButton->setEnabled(!currentNodeUuid_.isEmpty());
        }
        if ( success )
        {
            if ( shellPage_ != nullptr && shellPage_->uploadStatusLabel != nullptr ) { shellPage_->uploadStatusLabel->setText(tr("Upload: completed"));
}
            toastInfo(tr("Upload completed"));
        }
        else
        {
            if ( shellPage_ != nullptr && shellPage_->uploadStatusLabel != nullptr )
            {
                shellPage_->uploadStatusLabel->setText(tr("Upload: failed (%1)").arg(errorMessage));
            }
            if ( !errorMessage.isEmpty() )
            {
                toastError(tr("Upload failed: %1").arg(errorMessage));
            }
        }
    }

    void KelpiePanel::stopUpload(bool waitForJoin)
    {
        uploadGeneration_.fetch_add(1, std::memory_order_relaxed);
        if ( uploadThread_.joinable() )
        {
            uploadThread_.request_stop();
            std::jthread pending = std::move(uploadThread_);
            if ( waitForJoin )
            {
                if ( pending.joinable() )
                {
                    pending.join();
                }
            }
            else
            {
                std::thread([thread = std::move(pending)]() mutable {
                    if ( thread.joinable() )
                    {
                        thread.join();
                    }
                }).detach();
            }
        }
        uploadActive_.store(false);
    }
}
