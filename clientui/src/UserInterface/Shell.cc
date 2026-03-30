#include <UserInterface/KelpiePanel.hpp>

#include "Internal.hpp"

#include <QTextCursor>

namespace StockmanNamespace::UserInterface
{
    void KelpiePanel::setShellStatus(const QString& status)
    {
        shellStatusLabel_->setText(tr("Shell: %1").arg(status));
    }

    void KelpiePanel::startShell()
    {
        if ( currentNodeUuid_.isEmpty() )
        {
            toastWarn(tr("Select a node before opening a shell"));
            return;
        }
        auto* ctrl = controller();
        if ( ctrl == nullptr )
        {
            toastWarn(tr("Kelpie controller unavailable"));
            return;
        }
        const QString targetUuid = currentNodeUuid_;
        stopShell();
        setShellStatus(tr("connecting (%1)").arg(targetUuid));
        setWidgetsEnabled({openShellButton_, closeShellButton_, shellInput_}, false);
        toastInfo(tr("Opening shell for %1...").arg(targetUuid));

        const uint64_t epoch = ctrl->ConnectionEpoch();
        struct Result {
            uint64_t epoch{0};
            QString targetUuid;
            bool ok{false};
            QString error;
            std::shared_ptr<StockmanNamespace::ProxyStreamHandle> handle;
        };
        runAsync<Result>(
            this,
            [ctrl, epoch, targetUuid]() {
                Result res;
                res.epoch = epoch;
                res.targetUuid = targetUuid;
                QString error;
                auto handle = ctrl->StartShellStream(targetUuid, kelpieui::v1::SHELL_MODE_PTY, error);
                res.ok = (handle != nullptr);
                res.error = error;
                res.handle = std::move(handle);
                return res;
            },
            [this](const Result& res) {
                setWidgetsEnabled({openShellButton_}, true);
                auto* ctrl = controller();
                if ( ctrl == nullptr || ctrl->ConnectionEpoch() != res.epoch )
                {
                    if ( res.handle )
                    {
                        closeHandleAsync(res.handle);
                    }
                    setWidgetsEnabled({closeShellButton_, shellInput_}, false);
                    setShellStatus(tr("disconnected"));
                    return;
                }
                if ( res.targetUuid != currentNodeUuid_ )
                {
                    if ( res.handle )
                    {
                        closeHandleAsync(res.handle);
                    }
                    setWidgetsEnabled({closeShellButton_, shellInput_}, false);
                    setShellStatus(tr("disconnected"));
                    return;
                }
                if ( !res.ok || !res.handle )
                {
                    setWidgetsEnabled({closeShellButton_, shellInput_}, false);
                    setShellStatus(tr("disconnected"));
                    toastError(tr("Failed to open shell: %1").arg(res.error));
                    return;
                }

                shellStream_ = res.handle;
                connect(shellStream_.get(), &ProxyStreamHandle::DataReceived, this, &KelpiePanel::handleShellData);
                connect(shellStream_.get(), &ProxyStreamHandle::Closed, this, &KelpiePanel::handleShellClosed);

                if ( shellInput_ )
                {
                    shellInput_->setEnabled(true);
                    shellInput_->setFocus();
                }
                if ( closeShellButton_ ) { closeShellButton_->setEnabled(true);
}
                setShellStatus(tr("connected (%1)").arg(res.targetUuid));
                if ( shellView_ ) { shellView_->clear();
}

                if ( !pendingShellLine_.isEmpty() && pendingShellTarget_ == res.targetUuid )
                {
                    const QString queued = pendingShellLine_.trimmed();
                    const QByteArray bytes = pendingShellLine_.toUtf8();
                    pendingShellTarget_.clear();
                    pendingShellLine_.clear();
                    shellStream_->SendData(bytes);
                    AppendLog(tr("[%1] >> %2").arg(res.targetUuid, queued));
                }
            });
    }

    void KelpiePanel::stopShell()
    {
        if ( shellStream_ )
        {
            closeHandleAsync(shellStream_);
            shellStream_.reset();
        }
        shellInput_->setEnabled(false);
        closeShellButton_->setEnabled(false);
        setShellStatus(tr("disconnected"));
    }

    void KelpiePanel::sendShellInput()
    {
        if ( !shellStream_ )
        {
            return;
        }
        QString line = shellInput_->text();
        if ( line.isEmpty() )
        {
            return;
        }
        if ( !line.endsWith('\n') )
        {
            line.append('\n');
        }
        shellStream_->SendData(line.toUtf8());
        shellInput_->clear();
    }

    void KelpiePanel::handleShellData(const QByteArray& data)
    {
        if ( data.isEmpty() )
        {
            return;
        }
        shellView_->moveCursor(QTextCursor::End);
        shellView_->insertPlainText(QString::fromUtf8(data));
        shellView_->moveCursor(QTextCursor::End);
    }

    void KelpiePanel::handleShellClosed(const QString& reason)
    {
        if ( !reason.isEmpty() )
        {
            shellView_->appendPlainText(QStringLiteral("\n[Shell closed: %1]").arg(reason));
        }
        stopShell();
    }
}
