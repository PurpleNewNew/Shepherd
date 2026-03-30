#include <UserInterface/KelpiePanel.hpp>

#include "Internal.hpp"

#include <QTableWidgetItem>

namespace StockmanNamespace::UserInterface
{
    void KelpiePanel::refreshChat()
    {
        if ( chatTable_ == nullptr )
        {
            return;
        }
        chatTable_->setRowCount(0);
        chatIds_.clear();

        auto* ctrl = controller();
        if ( ctrl == nullptr )
        {
            return;
        }
        if ( refreshChatButton_ != nullptr ) { refreshChatButton_->setEnabled(false);
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
                if ( refreshChatButton_ ) { refreshChatButton_->setEnabled(true);
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
                chatTable_->scrollToBottom();
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
        const QString message = (chatInput_ != nullptr) ? chatInput_->text().trimmed() : QString();
        if ( message.isEmpty() )
        {
            return;
        }
        setWidgetsEnabled({sendChatButton_, chatInput_}, false);
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
                setWidgetsEnabled({sendChatButton_, chatInput_}, true);
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
                if ( chatInput_ )
                {
                    chatInput_->clear();
                }
                toastInfo(tr("Chat message sent"));
                refreshChat();
            });
    }

    void KelpiePanel::appendChatMessage(const kelpieui::v1::ChatMessage& message)
    {
        if ( chatTable_ == nullptr )
        {
            return;
        }
        const QString id = QString::fromStdString(message.id());
        if ( !id.isEmpty() )
        {
            if ( chatIds_.contains(id) )
            {
                return;
            }
            chatIds_.insert(id);
        }

        const int row = chatTable_->rowCount();
        chatTable_->insertRow(row);
        chatTable_->setItem(row, 0, new QTableWidgetItem(QString::fromStdString(message.timestamp())));
        chatTable_->setItem(row, 1, new QTableWidgetItem(QString::fromStdString(message.username())));
        chatTable_->setItem(row, 2, new QTableWidgetItem(QString::fromStdString(message.role())));
        chatTable_->setItem(row, 3, new QTableWidgetItem(QString::fromStdString(message.message())));
    }
}
