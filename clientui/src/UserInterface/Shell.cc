#include <UserInterface/KelpiePanel.hpp>
#include <UserInterface/Pages/ShellPage.hpp>

#include "Internal.hpp"

#include <QTextCursor>

namespace StockmanNamespace::UserInterface
{
    void KelpiePanel::setShellStatus(const QString& status)
    {
        shellPage_->statusLabel->setText(tr("Shell: %1").arg(status));
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
        setWidgetsEnabled({shellPage_->openButton, shellPage_->closeButton, shellPage_->input}, false);
        toastInfo(tr("Opening shell for %1...").arg(targetUuid));

        const uint64_t epoch = ctrl->ConnectionEpoch();
        struct Result {
            uint64_t epoch{0};
            QString targetUuid;
            bool ok{false};
            QString error;
            std::shared_ptr<StockmanNamespace::ProxyStreamHandle> handle;
        };
        runAsyncGuarded<Result>(
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
            [this]() {
                setWidgetsEnabled({shellPage_->openButton}, true);
            },
            [this](const Result& res) {
                auto* ctrl = controller();
                const bool sameEpoch = (ctrl != nullptr) && (ctrl->ConnectionEpoch() == res.epoch);
                const bool sameTarget = res.targetUuid == currentNodeUuid_;
                if ( !sameEpoch || !sameTarget )
                {
                    if ( res.handle )
                    {
                        closeHandleAsync(res.handle);
                    }
                    setWidgetsEnabled({shellPage_->closeButton, shellPage_->input}, false);
                    setShellStatus(tr("disconnected"));
                }
                return sameEpoch && sameTarget;
            },
            [this](const Result& res) {
                setWidgetsEnabled({shellPage_->closeButton, shellPage_->input}, false);
                setShellStatus(tr("disconnected"));
                toastError(tr("Failed to open shell: %1").arg(res.error));
            },
            [this](const Result& res) {
                shellPage_->stream = res.handle;
                connect(shellPage_->stream.get(), &ProxyStreamHandle::DataReceived, this, &KelpiePanel::handleShellData);
                connect(shellPage_->stream.get(), &ProxyStreamHandle::Closed, this, &KelpiePanel::handleShellClosed);

                if ( shellPage_->input )
                {
                    shellPage_->input->setEnabled(true);
                    shellPage_->input->setFocus();
                }
                if ( shellPage_->closeButton ) { shellPage_->closeButton->setEnabled(true);
}
                setShellStatus(tr("connected (%1)").arg(res.targetUuid));
                if ( shellPage_->view ) { shellPage_->view->clear();
}

                if ( !shellPage_->pendingLine.isEmpty() && shellPage_->pendingTarget == res.targetUuid )
                {
                    const QString queued = shellPage_->pendingLine.trimmed();
                    const QByteArray bytes = shellPage_->pendingLine.toUtf8();
                    shellPage_->pendingTarget.clear();
                    shellPage_->pendingLine.clear();
                    shellPage_->stream->SendData(bytes);
                    AppendLog(tr("[%1] >> %2").arg(res.targetUuid, queued));
                }
            });
    }

    void KelpiePanel::stopShell()
    {
        if ( shellPage_ != nullptr && shellPage_->stream )
        {
            closeHandleAsync(shellPage_->stream);
            shellPage_->stream.reset();
        }
        if ( shellPage_ != nullptr && shellPage_->input != nullptr )
        {
            shellPage_->input->setEnabled(false);
        }
        if ( shellPage_ != nullptr && shellPage_->closeButton != nullptr )
        {
            shellPage_->closeButton->setEnabled(false);
        }
        setShellStatus(tr("disconnected"));
    }

    void KelpiePanel::sendShellInput()
    {
        if ( shellPage_ == nullptr || !shellPage_->stream )
        {
            return;
        }
        QString line = shellPage_->input->text();
        if ( line.isEmpty() )
        {
            return;
        }
        if ( !line.endsWith('\n') )
        {
            line.append('\n');
        }
        shellPage_->stream->SendData(line.toUtf8());
        shellPage_->input->clear();
    }

    void KelpiePanel::handleShellData(const QByteArray& data)
    {
        auto* senderHandle = qobject_cast<ProxyStreamHandle*>(sender());
        if ( shellPage_ == nullptr || ((senderHandle != nullptr) && (senderHandle != shellPage_->stream.get())) )
        {
            return;
        }
        if ( data.isEmpty() )
        {
            return;
        }
        shellPage_->view->moveCursor(QTextCursor::End);
        shellPage_->view->insertPlainText(QString::fromUtf8(data));
        shellPage_->view->moveCursor(QTextCursor::End);
    }

    void KelpiePanel::handleShellClosed(const QString& reason)
    {
        auto* senderHandle = qobject_cast<ProxyStreamHandle*>(sender());
        if ( shellPage_ == nullptr || ((senderHandle != nullptr) && (senderHandle != shellPage_->stream.get())) )
        {
            return;
        }
        if ( !reason.isEmpty() )
        {
            shellPage_->view->appendPlainText(QStringLiteral("\n[Shell closed: %1]").arg(reason));
        }
        stopShell();
    }
}
