#include <UserInterface/KelpiePanel.hpp>

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
        const QString initial = (downloadLocalPathInput_ != nullptr) ? downloadLocalPathInput_->text().trimmed() : QString();
        QString suggested = initial;
        if ( suggested.isEmpty() && (downloadRemotePathInput_ != nullptr) )
        {
            suggested = QFileInfo(downloadRemotePathInput_->text().trimmed()).fileName();
        }
        const QString path = QFileDialog::getSaveFileName(this, tr("Select download destination"), suggested);
        if ( path.isEmpty() )
        {
            return;
        }
        if ( downloadLocalPathInput_ != nullptr )
        {
            downloadLocalPathInput_->setText(path);
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
        const QString remotePath = (downloadRemotePathInput_ != nullptr) ? downloadRemotePathInput_->text().trimmed() : QString();
        if ( remotePath.isEmpty() )
        {
            toastWarn(tr("Download remote path is required"));
            return;
        }
        QString localPath = (downloadLocalPathInput_ != nullptr) ? downloadLocalPathInput_->text().trimmed() : QString();
        if ( localPath.isEmpty() )
        {
            browseDownloadPath();
            localPath = (downloadLocalPathInput_ != nullptr) ? downloadLocalPathInput_->text().trimmed() : QString();
        }
        if ( localPath.isEmpty() )
        {
            return;
        }
        stopDownload();

        downloadActive_.store(true);
        const uint64_t generation = downloadGeneration_.fetch_add(1, std::memory_order_relaxed) + 1;
        if ( startDownloadButton_ != nullptr ) { startDownloadButton_->setEnabled(false);
}
        if ( downloadStatusLabel_ != nullptr ) { downloadStatusLabel_->setText(tr("Download: starting..."));
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
                                if ( downloadStatusLabel_ )
                                {
                                    downloadStatusLabel_->setText(tr("Download: %1 / %2").arg(done).arg(total));
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
        if ( uploadLocalPathInput_ != nullptr )
        {
            uploadLocalPathInput_->setText(path);
        }
        if ( (uploadRemotePathInput_ != nullptr) && uploadRemotePathInput_->text().trimmed().isEmpty() )
        {
            uploadRemotePathInput_->setText(QFileInfo(path).fileName());
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
        QString localPath = (uploadLocalPathInput_ != nullptr) ? uploadLocalPathInput_->text().trimmed() : QString();
        if ( localPath.isEmpty() )
        {
            browseUploadPath();
            localPath = (uploadLocalPathInput_ != nullptr) ? uploadLocalPathInput_->text().trimmed() : QString();
        }
        if ( localPath.isEmpty() )
        {
            return;
        }
        const QString remotePath = (uploadRemotePathInput_ != nullptr) ? uploadRemotePathInput_->text().trimmed() : QString();
        if ( remotePath.isEmpty() )
        {
            toastWarn(tr("Upload remote path is required"));
            return;
        }
        stopUpload();

        uploadActive_.store(true);
        const uint64_t generation = uploadGeneration_.fetch_add(1, std::memory_order_relaxed) + 1;
        if ( startUploadButton_ != nullptr ) { startUploadButton_->setEnabled(false);
}
        if ( uploadStatusLabel_ != nullptr ) { uploadStatusLabel_->setText(tr("Upload: starting..."));
}
        toastInfo(tr("Upload started: %1").arg(QFileInfo(localPath).fileName()));

        auto ctx = context_;
        const QString targetUuid = currentNodeUuid_;
        StockmanNamespace::KelpieController::FileUploadSpec spec;
        spec.localPath = localPath;
        spec.remotePath = remotePath;
        spec.overwrite = (uploadOverwriteCheck_ != nullptr) ? uploadOverwriteCheck_->isChecked() : false;

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
                                if ( uploadStatusLabel_ )
                                {
                                    uploadStatusLabel_->setText(tr("Upload: %1 / %2").arg(done).arg(total));
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
        if ( startDownloadButton_ != nullptr )
        {
            startDownloadButton_->setEnabled(!currentNodeUuid_.isEmpty());
        }
        if ( success )
        {
            if ( downloadStatusLabel_ != nullptr ) { downloadStatusLabel_->setText(tr("Download: completed"));
}
            toastInfo(tr("Download completed"));
        }
        else
        {
            if ( downloadStatusLabel_ != nullptr )
            {
                downloadStatusLabel_->setText(tr("Download: failed (%1)").arg(errorMessage));
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
        if ( startUploadButton_ != nullptr )
        {
            startUploadButton_->setEnabled(!currentNodeUuid_.isEmpty());
        }
        if ( success )
        {
            if ( uploadStatusLabel_ != nullptr ) { uploadStatusLabel_->setText(tr("Upload: completed"));
}
            toastInfo(tr("Upload completed"));
        }
        else
        {
            if ( uploadStatusLabel_ != nullptr )
            {
                uploadStatusLabel_->setText(tr("Upload: failed (%1)").arg(errorMessage));
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
