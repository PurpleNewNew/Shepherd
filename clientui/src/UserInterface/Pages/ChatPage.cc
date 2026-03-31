#include <UserInterface/Pages/ChatPage.hpp>

#include <QHeaderView>
#include <QHBoxLayout>
#include <QVBoxLayout>

namespace StockmanNamespace::UserInterface
{
    ChatPage::ChatPage(QWidget* parent)
        : QWidget(parent)
    {
        auto* chatLayout = new QVBoxLayout(this);
        chatLayout->setContentsMargins(0, 0, 0, 0);

        table = new QTableWidget(this);
        table->setColumnCount(4);
        table->setHorizontalHeaderLabels({tr("Time"), tr("User"), tr("Role"), tr("Message")});
        table->horizontalHeader()->setStretchLastSection(true);
        table->setEditTriggers(QAbstractItemView::NoEditTriggers);
        chatLayout->addWidget(table, 1);

        auto* chatControlLayout = new QHBoxLayout();
        input = new QLineEdit(this);
        input->setPlaceholderText(tr("Message to operators"));
        sendButton = new QPushButton(tr("Send"), this);
        refreshButton = new QPushButton(tr("Refresh"), this);
        chatControlLayout->addWidget(input, 1);
        chatControlLayout->addWidget(sendButton);
        chatControlLayout->addWidget(refreshButton);
        chatLayout->addLayout(chatControlLayout);
    }
}
