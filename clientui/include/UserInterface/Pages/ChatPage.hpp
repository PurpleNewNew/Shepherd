#ifndef STOCKMAN_CHAT_PAGE_HPP
#define STOCKMAN_CHAT_PAGE_HPP

#include <QLineEdit>
#include <QPushButton>
#include <QSet>
#include <QTableWidget>
#include <QWidget>

namespace StockmanNamespace::UserInterface
{
    class ChatPage : public QWidget
    {
    public:
        explicit ChatPage(QWidget* parent = nullptr);

        QTableWidget* table = nullptr;
        QLineEdit* input = nullptr;
        QPushButton* sendButton = nullptr;
        QPushButton* refreshButton = nullptr;
        QSet<QString> messageIds;
    };
}

#endif // STOCKMAN_CHAT_PAGE_HPP
