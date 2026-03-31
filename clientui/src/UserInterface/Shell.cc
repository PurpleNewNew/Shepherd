#include <UserInterface/KelpiePanel.hpp>

#include "Internal.hpp"

#include <QTextCursor>

namespace StockmanNamespace::UserInterface
{
    void KelpiePanel::setShellStatus(const QString& status)
    {
        shell_.statusLabel->setText(tr("Shell: %1").arg(status));
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
        setWidgetsEnabled({shell_.openButton, shell_.closeButton, shell_.input}, false);
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
                setWidgetsEnabled({shell_.openButton}, true);
                auto* ctrl = controller();
                if ( ctrl == nullptr || ctrl->ConnectionEpoch() != res.epoch )
                {
                    if ( res.handle )
                    {
                        closeHandleAsync(res.handle);
                    }
                    setWidgetsEnabled({shell_.closeButton, shell_.input}, false);
                    setShellStatus(tr("disconnected"));
                    return;
                }
                if ( res.targetUuid != currentNodeUuid_ )
                {
                    if ( res.handle )
                    {
                        closeHandleAsync(res.handle);
                    }
                    setWidgetsEnabled({shell_.closeButton, shell_.input}, false);
                    setShellStatus(tr("disconnected"));
                    return;
                }
                if ( !res.ok || !res.handle )
                {
                    setWidgetsEnabled({shell_.closeButton, shell_.input}, false);
                    setShellStatus(tr("disconnected"));
                    toastError(tr("Failed to open shell: %1").arg(res.error));
                    return;
                }

                shell_.stream = res.handle;
                connect(shell_.stream.get(), &ProxyStreamHandle::DataReceived, this, &KelpiePanel::handleShellData);
                connect(shell_.stream.get(), &ProxyStreamHandle::Closed, this, &KelpiePanel::handleShellClosed);

                if ( shell_.input )
                {
                    shell_.input->setEnabled(true);
                    shell_.input->setFocus();
                }
                if ( shell_.closeButton ) { shell_.closeButton->setEnabled(true);
}
                setShellStatus(tr("connected (%1)").arg(res.targetUuid));
                if ( shell_.view ) { shell_.view->clear();
}

                if ( !shell_.pendingLine.isEmpty() && shell_.pendingTarget == res.targetUuid )
                {
                    const QString queued = shell_.pendingLine.trimmed();
                    const QByteArray bytes = shell_.pendingLine.toUtf8();
                    shell_.pendingTarget.clear();
                    shell_.pendingLine.clear();
                    shell_.stream->SendData(bytes);
                    AppendLog(tr("[%1] >> %2").arg(res.targetUuid, queued));
                }
            });
    }

    void KelpiePanel::stopShell()
    {
        if ( shell_.stream )
        {
            closeHandleAsync(shell_.stream);
            shell_.stream.reset();
        }
        if ( shell_.input != nullptr )
        {
            shell_.input->setEnabled(false);
        }
        if ( shell_.closeButton != nullptr )
        {
            shell_.closeButton->setEnabled(false);
        }
        setShellStatus(tr("disconnected"));
    }

    void KelpiePanel::sendShellInput()
    {
        if ( !shell_.stream )
        {
            return;
        }
        QString line = shell_.input->text();
        if ( line.isEmpty() )
        {
            return;
        }
        if ( !line.endsWith('\n') )
        {
            line.append('\n');
        }
        shell_.stream->SendData(line.toUtf8());
        shell_.input->clear();
    }

    void KelpiePanel::handleShellData(const QByteArray& data)
    {
        auto* senderHandle = qobject_cast<ProxyStreamHandle*>(sender());
        if ( (senderHandle != nullptr) && (senderHandle != shell_.stream.get()) )
        {
            return;
        }
        if ( data.isEmpty() )
        {
            return;
        }
        shell_.view->moveCursor(QTextCursor::End);
        shell_.view->insertPlainText(QString::fromUtf8(data));
        shell_.view->moveCursor(QTextCursor::End);
    }

    void KelpiePanel::handleShellClosed(const QString& reason)
    {
        auto* senderHandle = qobject_cast<ProxyStreamHandle*>(sender());
        if ( (senderHandle != nullptr) && (senderHandle != shell_.stream.get()) )
        {
            return;
        }
        if ( !reason.isEmpty() )
        {
            shell_.view->appendPlainText(QStringLiteral("\n[Shell closed: %1]").arg(reason));
        }
        stopShell();
    }
}
