#ifndef STOCKMAN_LOOT_PAGE_HPP
#define STOCKMAN_LOOT_PAGE_HPP

#include <QComboBox>
#include <QLineEdit>
#include <QPushButton>
#include <QSet>
#include <QSpinBox>
#include <QTableWidget>
#include <QWidget>

namespace StockmanNamespace::UserInterface
{
    class LootPage : public QWidget
    {
    public:
        explicit LootPage(QWidget* parent = nullptr);

        QTableWidget* table = nullptr;
        QLineEdit* targetInput = nullptr;
        QLineEdit* tagInput = nullptr;
        QComboBox* categoryBox = nullptr;
        QSpinBox* limitSpin = nullptr;
        QPushButton* refreshButton = nullptr;
        QPushButton* submitButton = nullptr;
        QPushButton* downloadButton = nullptr;
        QSet<QString> lootIds;
    };
}

#endif // STOCKMAN_LOOT_PAGE_HPP
