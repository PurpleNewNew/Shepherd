#include <UserInterface/KelpiePanel.hpp>

#include "Internal.hpp"

#include <QTableWidgetItem>

namespace StockmanNamespace::UserInterface
{
    void KelpiePanel::refreshChat()
    {
        if ( workspace_.chatTable == nullptr )
        {
            return;
        }
        workspace_.chatTable->setRowCount(0);
        workspace_.chatIds.clear();

        auto* ctrl = controller();
        if ( ctrl == nullptr )
        {
            return;
        }
        if ( workspace_.refreshChatButton != nullptr ) { workspace_.refreshChatButton->setEnabled(false);
}
        const uint64_t epoch = ctrl->ConnectionEpoch();

        struct Result {
            uint64_t epoch{0};
            bool ok{false};
            QString error;
            std::vector<kelpieui::v1::ChatMessage> messages;
        };

        runAsync<Result>(
            this,
            [ctrl, epoch]() {
                Result res;
                res.epoch = epoch;
                QString error;
                std::vector<kelpieui::v1::ChatMessage> messages;
                res.ok = ctrl->ListChatMessages(200, QString(), &messages, error);
                res.error = error;
                res.messages = std::move(messages);
                return res;
            },
            [this](const Result& res) {
                if ( workspace_.refreshChatButton ) { workspace_.refreshChatButton->setEnabled(true);
}

                auto* ctrl = controller();
                if ( ctrl == nullptr || ctrl->ConnectionEpoch() != res.epoch )
                {
                    return;
                }
                if ( !res.ok )
                {
                    toastError(tr("Chat refresh failed: %1").arg(res.error));
                    return;
                }
                for (const auto& message : res.messages)
                {
                    appendChatMessage(message);
                }
                workspace_.chatTable->scrollToBottom();
            });
    }

    void KelpiePanel::sendChatMessage()
    {
        auto* ctrl = controller();
        if ( ctrl == nullptr )
        {
            toastWarn(tr("gRPC client not connected"));
            return;
        }
        const QString message = (workspace_.chatInput != nullptr) ? workspace_.chatInput->text().trimmed() : QString();
        if ( message.isEmpty() )
        {
            return;
        }
        setWidgetsEnabled({workspace_.sendChatButton, workspace_.chatInput}, false);
        toastInfo(tr("Sending chat message..."));
        const uint64_t epoch = ctrl->ConnectionEpoch();
        struct Result {
            uint64_t epoch{0};
            bool ok{false};
            QString error;
        };
        runAsync<Result>(
            this,
            [ctrl, epoch, message]() {
                Result res;
                res.epoch = epoch;
                QString error;
                res.ok = ctrl->SendChatMessage(message, error);
                res.error = error;
                return res;
            },
            [this](const Result& res) {
                setWidgetsEnabled({workspace_.sendChatButton, workspace_.chatInput}, true);
                auto* ctrl = controller();
                if ( ctrl == nullptr || ctrl->ConnectionEpoch() != res.epoch )
                {
                    return;
                }
                if ( !res.ok )
                {
                    toastError(tr("Send chat failed: %1").arg(res.error));
                    return;
                }
                if ( workspace_.chatInput )
                {
                    workspace_.chatInput->clear();
                }
                toastInfo(tr("Chat message sent"));
                refreshChat();
            });
    }

    void KelpiePanel::appendChatMessage(const kelpieui::v1::ChatMessage& message)
    {
        if ( workspace_.chatTable == nullptr )
        {
            return;
        }
        const QString id = QString::fromStdString(message.id());
        if ( !id.isEmpty() )
        {
            if ( workspace_.chatIds.contains(id) )
            {
                return;
            }
            workspace_.chatIds.insert(id);
        }

        const int row = workspace_.chatTable->rowCount();
        workspace_.chatTable->insertRow(row);
        workspace_.chatTable->setItem(row, 0, new QTableWidgetItem(QString::fromStdString(message.timestamp())));
        workspace_.chatTable->setItem(row, 1, new QTableWidgetItem(QString::fromStdString(message.username())));
        workspace_.chatTable->setItem(row, 2, new QTableWidgetItem(QString::fromStdString(message.role())));
        workspace_.chatTable->setItem(row, 3, new QTableWidgetItem(QString::fromStdString(message.message())));
    }
}
