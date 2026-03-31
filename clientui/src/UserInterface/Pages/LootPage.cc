#include <UserInterface/Pages/LootPage.hpp>

#include <QHeaderView>
#include <QHBoxLayout>
#include <QLabel>
#include <QVBoxLayout>

#include "proto/kelpieui/v1/kelpieui.pb.h"

namespace StockmanNamespace::UserInterface
{
    LootPage::LootPage(QWidget* parent)
        : QWidget(parent)
    {
        auto* lootLayout = new QVBoxLayout(this);
        lootLayout->setContentsMargins(0, 0, 0, 0);

        auto* lootControlLayout = new QHBoxLayout();
        targetInput = new QLineEdit(this);
        targetInput->setPlaceholderText(tr("Target UUID (optional)"));
        tagInput = new QLineEdit(this);
        tagInput->setPlaceholderText(tr("Tags (comma separated)"));
        categoryBox = new QComboBox(this);
        categoryBox->addItem(tr("All"), kelpieui::v1::LOOT_CATEGORY_UNSPECIFIED);
        categoryBox->addItem(tr("File"), kelpieui::v1::LOOT_CATEGORY_FILE);
        categoryBox->addItem(tr("Screenshot"), kelpieui::v1::LOOT_CATEGORY_SCREENSHOT);
        categoryBox->addItem(tr("Ticket"), kelpieui::v1::LOOT_CATEGORY_TICKET);
        limitSpin = new QSpinBox(this);
        limitSpin->setRange(1, 500);
        limitSpin->setValue(100);
        refreshButton = new QPushButton(tr("Refresh"), this);
        submitButton = new QPushButton(tr("Submit File"), this);
        downloadButton = new QPushButton(tr("Download Selected"), this);
        lootControlLayout->addWidget(new QLabel(tr("Target:"), this));
        lootControlLayout->addWidget(targetInput);
        lootControlLayout->addWidget(new QLabel(tr("Tags:"), this));
        lootControlLayout->addWidget(tagInput);
        lootControlLayout->addWidget(new QLabel(tr("Category:"), this));
        lootControlLayout->addWidget(categoryBox);
        lootControlLayout->addWidget(new QLabel(tr("Limit:"), this));
        lootControlLayout->addWidget(limitSpin);
        lootControlLayout->addWidget(refreshButton);
        lootControlLayout->addWidget(submitButton);
        lootControlLayout->addWidget(downloadButton);
        lootLayout->addLayout(lootControlLayout);

        table = new QTableWidget(this);
        table->setColumnCount(6);
        table->setHorizontalHeaderLabels({tr("Created"), tr("Name"), tr("Category"), tr("Target"), tr("Size"), tr("Tags")});
        table->horizontalHeader()->setStretchLastSection(true);
        table->setEditTriggers(QAbstractItemView::NoEditTriggers);
        lootLayout->addWidget(table, 1);
    }
}
