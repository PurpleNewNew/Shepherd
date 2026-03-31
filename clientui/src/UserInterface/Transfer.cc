#include <UserInterface/KelpiePanel.hpp>
#include <UserInterface/Pages/ShellPage.hpp>

#include "Internal.hpp"

#include <QFileDialog>
#include <QFileInfo>
#include <QMetaObject>
#include <thread>
#include <utility>

namespace StockmanNamespace::UserInterface
{
    void KelpiePanel::browseDownloadPath()
    {
        const QString initial = (shellPage_ != nullptr && shellPage_->downloadLocalPathInput != nullptr) ? shellPage_->downloadLocalPathInput->text().trimmed() : QString();
        QString suggested = initial;
        if ( suggested.isEmpty() && (shellPage_ != nullptr) && (shellPage_->downloadRemotePathInput != nullptr) )
        {
            suggested = QFileInfo(shellPage_->downloadRemotePathInput->text().trimmed()).fileName();
        }
        const QString path = QFileDialog::getSaveFileName(this, tr("Select download destination"), suggested);
        if ( path.isEmpty() )
        {
            return;
        }
        if ( shellPage_ != nullptr && shellPage_->downloadLocalPathInput != nullptr )
        {
            shellPage_->downloadLocalPathInput->setText(path);
        }
    }

    void KelpiePanel::startDownloadFile()
    {
        if ( downloadActive_.load() )
        {
            return;
        }
        if ( currentNodeUuid_.isEmpty() )
        {
            toastWarn(tr("Select a node first"));
            return;
        }
        const QString remotePath = (shellPage_ != nullptr && shellPage_->downloadRemotePathInput != nullptr) ? shellPage_->downloadRemotePathInput->text().trimmed() : QString();
        if ( remotePath.isEmpty() )
        {
            toastWarn(tr("Download remote path is required"));
            return;
        }
        QString localPath = (shellPage_ != nullptr && shellPage_->downloadLocalPathInput != nullptr) ? shellPage_->downloadLocalPathInput->text().trimmed() : QString();
        if ( localPath.isEmpty() )
        {
            browseDownloadPath();
            localPath = (shellPage_ != nullptr && shellPage_->downloadLocalPathInput != nullptr) ? shellPage_->downloadLocalPathInput->text().trimmed() : QString();
        }
        if ( localPath.isEmpty() )
        {
            return;
        }
        stopDownload();

        downloadActive_.store(true);
        const uint64_t generation = downloadGeneration_.fetch_add(1, std::memory_order_relaxed) + 1;
        if ( shellPage_ != nullptr && shellPage_->startDownloadButton != nullptr ) { shellPage_->startDownloadButton->setEnabled(false);
}
        if ( shellPage_ != nullptr && shellPage_->downloadStatusLabel != nullptr ) { shellPage_->downloadStatusLabel->setText(tr("Download: starting..."));
}
        toastInfo(tr("Download started: %1").arg(QFileInfo(remotePath).fileName()));

        auto ctx = context_;
        const QString targetUuid = currentNodeUuid_;
        StockmanNamespace::KelpieController::FileDownloadSpec spec;
        spec.remotePath = remotePath;
        spec.localPath = localPath;
        spec.offset = 0;

        downloadThread_ = std::jthread([this, ctx, targetUuid, spec, generation](std::stop_token stopToken) {
            QString error;
            bool ok = false;
            auto* ctrl = ctx ? ctx->kelpieController.get() : nullptr;
            if ( ctrl != nullptr )
            {
                ok = ctrl->DownloadFileDataplane(
                    targetUuid,
                    spec,
                    error,
                    [this, generation](qint64 done, qint64 total) {
                        QMetaObject::invokeMethod(
                            this,
                            [this, generation, done, total]() {
                                if ( generation != downloadGeneration_.load(std::memory_order_relaxed) )
                                {
                                    return;
                                }
                                if ( shellPage_ != nullptr && shellPage_->downloadStatusLabel )
                                {
                                    shellPage_->downloadStatusLabel->setText(tr("Download: %1 / %2").arg(done).arg(total));
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
                    if ( generation != downloadGeneration_.load(std::memory_order_relaxed) )
                    {
                        return;
                    }
                    finishDownload(ok, error);
                },
                Qt::QueuedConnection);
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
            shellPage_->uploadRemotePathInput->setText(QFileInfo(path).fileName());
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
            toastWarn(tr("Upload remote path is required"));
            return;
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
        spec.remotePath = remotePath;
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

    void KelpiePanel::finishDownload(bool success, const QString& errorMessage)
    {
        downloadActive_.store(false);
        if ( shellPage_ != nullptr && shellPage_->startDownloadButton != nullptr )
        {
            shellPage_->startDownloadButton->setEnabled(!currentNodeUuid_.isEmpty());
        }
        if ( success )
        {
            if ( shellPage_ != nullptr && shellPage_->downloadStatusLabel != nullptr ) { shellPage_->downloadStatusLabel->setText(tr("Download: completed"));
}
            toastInfo(tr("Download completed"));
        }
        else
        {
            if ( shellPage_ != nullptr && shellPage_->downloadStatusLabel != nullptr )
            {
                shellPage_->downloadStatusLabel->setText(tr("Download: failed (%1)").arg(errorMessage));
            }
            if ( !errorMessage.isEmpty() )
            {
                toastError(tr("Download failed: %1").arg(errorMessage));
            }
        }
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

    void KelpiePanel::stopDownload(bool waitForJoin)
    {
        downloadGeneration_.fetch_add(1, std::memory_order_relaxed);
        if ( downloadThread_.joinable() )
        {
            downloadThread_.request_stop();
            std::jthread pending = std::move(downloadThread_);
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
        downloadActive_.store(false);
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
