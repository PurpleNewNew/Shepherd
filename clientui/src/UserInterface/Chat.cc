#include <UserInterface/KelpiePanel.hpp>
#include <UserInterface/Pages/ChatPage.hpp>

#include "Internal.hpp"

#include <QTableWidgetItem>

namespace StockmanNamespace::UserInterface
{
    void KelpiePanel::refreshChat()
    {
        if ( chatPage_ == nullptr || chatPage_->table == nullptr )
        {
            return;
        }
        chatPage_->table->setRowCount(0);
        chatPage_->messageIds.clear();

        auto* ctrl = controller();
        if ( ctrl == nullptr )
        {
            return;
        }
        if ( chatPage_->refreshButton != nullptr ) { chatPage_->refreshButton->setEnabled(false);
}
        const uint64_t epoch = ctrl->ConnectionEpoch();

        struct Result {
            uint64_t epoch{0};
            bool ok{false};
            QString error;
            std::vector<kelpieui::v1::ChatMessage> messages;
        };

        runAsyncGuarded<Result>(
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
            [this]() {
                if ( chatPage_ != nullptr && chatPage_->refreshButton ) { chatPage_->refreshButton->setEnabled(true);
}
            },
            [this](const Result& res) {
                auto* ctrl = controller();
                return (ctrl != nullptr) && (ctrl->ConnectionEpoch() == res.epoch);
            },
            [this](const Result& res) {
                toastError(tr("Chat refresh failed: %1").arg(res.error));
            },
            [this](const Result& res) {
                for (const auto& message : res.messages)
                {
                    appendChatMessage(message);
                }
                chatPage_->table->scrollToBottom();
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
        const QString message = (chatPage_ != nullptr && chatPage_->input != nullptr) ? chatPage_->input->text().trimmed() : QString();
        if ( message.isEmpty() )
        {
            return;
        }
        setWidgetsEnabled({chatPage_->sendButton, chatPage_->input}, false);
        toastInfo(tr("Sending chat message..."));
        const uint64_t epoch = ctrl->ConnectionEpoch();
        struct Result {
            uint64_t epoch{0};
            bool ok{false};
            QString error;
        };
        runAsyncGuarded<Result>(
            this,
            [ctrl, epoch, message]() {
                Result res;
                res.epoch = epoch;
                QString error;
                res.ok = ctrl->SendChatMessage(message, error);
                res.error = error;
                return res;
            },
            [this]() {
                setWidgetsEnabled({chatPage_->sendButton, chatPage_->input}, true);
            },
            [this](const Result& res) {
                auto* ctrl = controller();
                return (ctrl != nullptr) && (ctrl->ConnectionEpoch() == res.epoch);
            },
            [this](const Result& res) {
                toastError(tr("Send chat failed: %1").arg(res.error));
            },
            [this](const Result&) {
                if ( chatPage_->input )
                {
                    chatPage_->input->clear();
                }
                toastInfo(tr("Chat message sent"));
                refreshChat();
            });
    }

    void KelpiePanel::appendChatMessage(const kelpieui::v1::ChatMessage& message)
    {
        if ( chatPage_ == nullptr || chatPage_->table == nullptr )
        {
            return;
        }
        const QString id = QString::fromStdString(message.id());
        if ( !id.isEmpty() )
        {
            if ( chatPage_->messageIds.contains(id) )
            {
                return;
            }
            chatPage_->messageIds.insert(id);
        }

        const int row = chatPage_->table->rowCount();
        chatPage_->table->insertRow(row);
        chatPage_->table->setItem(row, 0, new QTableWidgetItem(QString::fromStdString(message.timestamp())));
        chatPage_->table->setItem(row, 1, new QTableWidgetItem(QString::fromStdString(message.username())));
        chatPage_->table->setItem(row, 2, new QTableWidgetItem(QString::fromStdString(message.role())));
        chatPage_->table->setItem(row, 3, new QTableWidgetItem(QString::fromStdString(message.message())));
    }
}
