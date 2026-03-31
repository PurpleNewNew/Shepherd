#include <UserInterface/KelpiePanel.hpp>
#include <UserInterface/Pages/LootPage.hpp>

#include "Internal.hpp"

#include <QFile>
#include <QFileDialog>
#include <QFileInfo>
#include <QTableWidgetItem>

namespace StockmanNamespace::UserInterface
{
    void KelpiePanel::refreshLoot()
    {
        if ( lootPage_ == nullptr || lootPage_->table == nullptr )
        {
            return;
        }
        lootPage_->table->setRowCount(0);
        lootPage_->lootIds.clear();

        auto* ctrl = controller();
        if ( ctrl == nullptr )
        {
            return;
        }
        if ( lootPage_->refreshButton != nullptr ) { lootPage_->refreshButton->setEnabled(false);
}
        const uint64_t epoch = ctrl->ConnectionEpoch();
        const QString target = (lootPage_->targetInput != nullptr) ? lootPage_->targetInput->text().trimmed() : QString();
        QStringList tags;
        if ( (lootPage_->tagInput != nullptr) && !lootPage_->tagInput->text().trimmed().isEmpty() )
        {
            const auto parts = lootPage_->tagInput->text().split(',', Qt::SkipEmptyParts);
            for ( const auto& part : parts )
            {
                const QString trimmed = part.trimmed();
                if ( !trimmed.isEmpty() )
                {
                    tags.push_back(trimmed);
                }
            }
        }
        const int limit = (lootPage_->limitSpin != nullptr) ? lootPage_->limitSpin->value() : 100;
        const auto category = (lootPage_->categoryBox != nullptr)
                                  ? static_cast<kelpieui::v1::LootCategory>(lootPage_->categoryBox->currentData().toInt())
                                  : kelpieui::v1::LOOT_CATEGORY_UNSPECIFIED;

        struct Result {
            uint64_t epoch{0};
            bool ok{false};
            QString error;
            std::vector<kelpieui::v1::LootItem> items;
        };

        runAsyncGuarded<Result>(
            this,
            [ctrl, target, category, limit, tags, epoch]() {
                Result res;
                res.epoch = epoch;
                QString error;
                std::vector<kelpieui::v1::LootItem> items;
                res.ok = ctrl->ListLoot(target, category, limit, QString(), tags, &items, error);
                res.error = error;
                res.items = std::move(items);
                return res;
            },
            [this]() {
                if ( lootPage_ != nullptr && lootPage_->refreshButton ) { lootPage_->refreshButton->setEnabled(true);
}
            },
            [this](const Result& res) {
                auto* ctrl = controller();
                return (ctrl != nullptr) && (ctrl->ConnectionEpoch() == res.epoch);
            },
            [this](const Result& res) {
                toastError(tr("Loot refresh failed: %1").arg(res.error));
            },
            [this](const Result& res) {
                for (const auto& item : res.items)
                {
                    appendLootItem(item);
                }
            });
    }

    void KelpiePanel::appendLootItem(const kelpieui::v1::LootItem& item)
    {
        if ( lootPage_ == nullptr || lootPage_->table == nullptr )
        {
            return;
        }
        const QString id = QString::fromStdString(item.loot_id());
        if ( !id.isEmpty() )
        {
            if ( lootPage_->lootIds.contains(id) )
            {
                return;
            }
            lootPage_->lootIds.insert(id);
        }

        const int row = lootPage_->table->rowCount();
        lootPage_->table->insertRow(row);
        auto* createdItem = new QTableWidgetItem(QString::fromStdString(item.created_at()));
        createdItem->setData(Qt::UserRole, id);
        createdItem->setData(Qt::UserRole + 1, QString::fromStdString(item.storage_ref()));
        createdItem->setData(Qt::UserRole + 2, QString::fromStdString(item.name()));
        lootPage_->table->setItem(row, 0, createdItem);
        lootPage_->table->setItem(row, 1, new QTableWidgetItem(QString::fromStdString(item.name())));
        lootPage_->table->setItem(row, 2, new QTableWidgetItem(lootCategoryText(item.category())));
        lootPage_->table->setItem(row, 3, new QTableWidgetItem(QString::fromStdString(item.target_uuid())));
        lootPage_->table->setItem(row, 4, new QTableWidgetItem(QString::number(item.size())));
        lootPage_->table->setItem(row, 5, new QTableWidgetItem(joinTags(item.tags())));
    }

    QString KelpiePanel::selectedLootId() const
    {
        if ( lootPage_ == nullptr || lootPage_->table == nullptr )
        {
            return {};
        }
        const int row = lootPage_->table->currentRow();
        if ( row < 0 )
        {
            return {};
        }
        for ( int col = 0; col < lootPage_->table->columnCount(); ++col )
        {
            auto* item = lootPage_->table->item(row, col);
            if ( item == nullptr )
            {
                continue;
            }
            const QString id = item->data(Qt::UserRole).toString().trimmed();
            if ( !id.isEmpty() )
            {
                return id;
            }
        }
        return {};
    }

    void KelpiePanel::submitLootFromFile()
    {
        auto* ctrl = controller();
        if ( ctrl == nullptr )
        {
            toastWarn(tr("gRPC client not connected"));
            return;
        }
        QString target = (lootPage_ != nullptr && lootPage_->targetInput != nullptr) ? lootPage_->targetInput->text().trimmed() : QString();
        if ( target.isEmpty() )
        {
            target = currentNodeUuid_;
        }
        if ( target.isEmpty() )
        {
            toastWarn(tr("Provide target UUID or select a node first"));
            return;
        }

        QString localPath = QFileDialog::getOpenFileName(this, tr("Select file to submit as loot"));
        if ( localPath.isEmpty() )
        {
            return;
        }
        QFileInfo info(localPath);
        if ( !info.exists() || !info.isFile() )
        {
            toastWarn(tr("Selected file is invalid"));
            return;
        }

        const auto selectedCategory = (lootPage_ != nullptr && lootPage_->categoryBox != nullptr)
                                          ? static_cast<kelpieui::v1::LootCategory>(lootPage_->categoryBox->currentData().toInt())
                                          : kelpieui::v1::LOOT_CATEGORY_UNSPECIFIED;
        const auto category = selectedCategory == kelpieui::v1::LOOT_CATEGORY_UNSPECIFIED
                                  ? kelpieui::v1::LOOT_CATEGORY_FILE
                                  : selectedCategory;

        QStringList tags;
        if ( (lootPage_ != nullptr) && (lootPage_->tagInput != nullptr) && !lootPage_->tagInput->text().trimmed().isEmpty() )
        {
            const auto rawTags = lootPage_->tagInput->text().split(',', Qt::SkipEmptyParts);
            for ( const auto& raw : rawTags )
            {
                const QString t = raw.trimmed();
                if ( !t.isEmpty() )
                {
                    tags.push_back(t);
                }
            }
        }
        setWidgetsEnabled({lootPage_->submitButton, lootPage_->refreshButton, lootPage_->table}, false);
        toastInfo(tr("Submitting loot %1...").arg(info.fileName()));
        const uint64_t epoch = ctrl->ConnectionEpoch();

        struct Result {
            uint64_t epoch{0};
            bool ok{false};
            QString error;
            kelpieui::v1::LootItem created;
        };

        runAsyncGuarded<Result>(
            this,
            [ctrl, epoch, target, category, name = info.fileName(), localPath, tags, size = info.size()]() {
                Result res;
                res.epoch = epoch;
                QString error;
                kelpieui::v1::LootItem created;
                res.ok = ctrl->SubmitLoot(target,
                                          category,
                                          name,
                                          localPath,
                                          {},
                                          tags,
                                          QString(),
                                          static_cast<uint64_t>(size),
                                          true,
                                          &created,
                                          error);
                res.error = error;
                res.created = created;
                return res;
            },
            [this]() {
                setWidgetsEnabled({lootPage_->submitButton, lootPage_->refreshButton, lootPage_->table}, true);
            },
            [this](const Result& res) {
                auto* ctrl = controller();
                return (ctrl != nullptr) && (ctrl->ConnectionEpoch() == res.epoch);
            },
            [this](const Result& res) {
                toastError(tr("Submit loot failed: %1").arg(res.error));
            },
            [this](const Result& res) {
                appendLootItem(res.created);
                toastInfo(tr("Loot submitted: %1").arg(QString::fromStdString(res.created.loot_id())));
                refreshLoot();
            });
    }

    void KelpiePanel::downloadSelectedLoot()
    {
        auto* ctrl = controller();
        if ( ctrl == nullptr )
        {
            toastWarn(tr("gRPC client not connected"));
            return;
        }
        const QString lootId = selectedLootId();
        if ( lootId.isEmpty() )
        {
            toastWarn(tr("Select a loot row first"));
            return;
        }
        QString defaultName = lootId;
        if ( lootPage_ != nullptr && lootPage_->table != nullptr )
        {
            const int row = lootPage_->table->currentRow();
            auto* nameItem = row >= 0 ? lootPage_->table->item(row, 1) : nullptr;
            if ( (nameItem != nullptr) && !nameItem->text().trimmed().isEmpty() )
            {
                defaultName = nameItem->text().trimmed();
            }
        }
        const QString savePath = QFileDialog::getSaveFileName(this, tr("Save loot content"), defaultName);
        if ( savePath.isEmpty() )
        {
            return;
        }

        setWidgetsEnabled({lootPage_->downloadButton, lootPage_->refreshButton, lootPage_->table}, false);
        toastInfo(tr("Downloading loot %1...").arg(lootId));
        const uint64_t epoch = ctrl->ConnectionEpoch();

        struct Result {
            uint64_t epoch{0};
            QString lootId;
            QString savePath;
            bool ok{false};
            QString error;
            qint64 bytes{0};
        };

        runAsyncGuarded<Result>(
            this,
            [ctrl, epoch, lootId, savePath]() {
                Result res;
                res.epoch = epoch;
                res.lootId = lootId;
                res.savePath = savePath;

                QString error;
                kelpieui::v1::LootItem item;
                QByteArray content;
                if ( !ctrl->GetLootContent(lootId, &item, &content, error) )
                {
                    res.ok = false;
                    res.error = error;
                    return res;
                }
                if ( content.isEmpty() )
                {
                    res.ok = false;
                    res.error = QObject::tr("Loot content is empty (storage_ref=%1)")
                                    .arg(QString::fromStdString(item.storage_ref()));
                    return res;
                }

                QFile out(savePath);
                if ( !out.open(QIODevice::WriteOnly | QIODevice::Truncate) )
                {
                    res.ok = false;
                    res.error = QObject::tr("Cannot open file for writing: %1").arg(savePath);
                    return res;
                }
                const qint64 written = out.write(content);
                out.close();
                if ( written != content.size() )
                {
                    res.ok = false;
                    res.error = QObject::tr("Loot download incomplete: wrote %1 / %2")
                                    .arg(written)
                                    .arg(content.size());
                    return res;
                }
                res.ok = true;
                res.bytes = content.size();
                return res;
            },
            [this]() {
                setWidgetsEnabled({lootPage_->downloadButton, lootPage_->refreshButton, lootPage_->table}, true);
            },
            [this](const Result& res) {
                auto* ctrl = controller();
                return (ctrl != nullptr) && (ctrl->ConnectionEpoch() == res.epoch);
            },
            [this](const Result& res) {
                toastError(tr("Loot download failed: %1").arg(res.error));
            },
            [this](const Result& res) {
                toastInfo(tr("Loot saved: %1 (%2 bytes)")
                              .arg(QFileInfo(res.savePath).fileName())
                              .arg(res.bytes));
            });
    }
}
