#include <UserInterface/KelpiePanel.hpp>

#include "Internal.hpp"

#include <QFile>
#include <QFileDialog>
#include <QFileInfo>
#include <QTableWidgetItem>

namespace StockmanNamespace::UserInterface
{
    void KelpiePanel::refreshLoot()
    {
        if ( lootTable_ == nullptr )
        {
            return;
        }
        lootTable_->setRowCount(0);
        lootIds_.clear();

        auto* ctrl = controller();
        if ( ctrl == nullptr )
        {
            return;
        }
        if ( refreshLootButton_ != nullptr ) { refreshLootButton_->setEnabled(false);
}
        const uint64_t epoch = ctrl->ConnectionEpoch();
        const QString target = (lootTargetInput_ != nullptr) ? lootTargetInput_->text().trimmed() : QString();
        QStringList tags;
        if ( (lootTagInput_ != nullptr) && !lootTagInput_->text().trimmed().isEmpty() )
        {
            const auto parts = lootTagInput_->text().split(',', Qt::SkipEmptyParts);
            for ( const auto& part : parts )
            {
                const QString trimmed = part.trimmed();
                if ( !trimmed.isEmpty() )
                {
                    tags.push_back(trimmed);
                }
            }
        }
        const int limit = (lootLimitSpin_ != nullptr) ? lootLimitSpin_->value() : 100;
        const auto category = (lootCategoryBox_ != nullptr)
                                  ? static_cast<kelpieui::v1::LootCategory>(lootCategoryBox_->currentData().toInt())
                                  : kelpieui::v1::LOOT_CATEGORY_UNSPECIFIED;

        struct Result {
            uint64_t epoch{0};
            bool ok{false};
            QString error;
            std::vector<kelpieui::v1::LootItem> items;
        };

        runAsync<Result>(
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
            [this](const Result& res) {
                if ( refreshLootButton_ ) { refreshLootButton_->setEnabled(true);
}
                auto* ctrl = controller();
                if ( ctrl == nullptr || ctrl->ConnectionEpoch() != res.epoch )
                {
                    return;
                }
                if ( !res.ok )
                {
                    toastError(tr("Loot refresh failed: %1").arg(res.error));
                    return;
                }
                for (const auto& item : res.items)
                {
                    appendLootItem(item);
                }
            });
    }

    void KelpiePanel::appendLootItem(const kelpieui::v1::LootItem& item)
    {
        if ( lootTable_ == nullptr )
        {
            return;
        }
        const QString id = QString::fromStdString(item.loot_id());
        if ( !id.isEmpty() )
        {
            if ( lootIds_.contains(id) )
            {
                return;
            }
            lootIds_.insert(id);
        }

        const int row = lootTable_->rowCount();
        lootTable_->insertRow(row);
        auto* createdItem = new QTableWidgetItem(QString::fromStdString(item.created_at()));
        createdItem->setData(Qt::UserRole, id);
        createdItem->setData(Qt::UserRole + 1, QString::fromStdString(item.storage_ref()));
        createdItem->setData(Qt::UserRole + 2, QString::fromStdString(item.name()));
        lootTable_->setItem(row, 0, createdItem);
        lootTable_->setItem(row, 1, new QTableWidgetItem(QString::fromStdString(item.name())));
        lootTable_->setItem(row, 2, new QTableWidgetItem(lootCategoryText(item.category())));
        lootTable_->setItem(row, 3, new QTableWidgetItem(QString::fromStdString(item.target_uuid())));
        lootTable_->setItem(row, 4, new QTableWidgetItem(QString::number(item.size())));
        lootTable_->setItem(row, 5, new QTableWidgetItem(joinTags(item.tags())));
    }

    QString KelpiePanel::selectedLootId() const
    {
        if ( lootTable_ == nullptr )
        {
            return {};
        }
        const int row = lootTable_->currentRow();
        if ( row < 0 )
        {
            return {};
        }
        for ( int col = 0; col < lootTable_->columnCount(); ++col )
        {
            auto* item = lootTable_->item(row, col);
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
        QString target = (lootTargetInput_ != nullptr) ? lootTargetInput_->text().trimmed() : QString();
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

        const auto selectedCategory = (lootCategoryBox_ != nullptr)
                                          ? static_cast<kelpieui::v1::LootCategory>(lootCategoryBox_->currentData().toInt())
                                          : kelpieui::v1::LOOT_CATEGORY_UNSPECIFIED;
        const auto category = selectedCategory == kelpieui::v1::LOOT_CATEGORY_UNSPECIFIED
                                  ? kelpieui::v1::LOOT_CATEGORY_FILE
                                  : selectedCategory;

        QStringList tags;
        if ( (lootTagInput_ != nullptr) && !lootTagInput_->text().trimmed().isEmpty() )
        {
            const auto rawTags = lootTagInput_->text().split(',', Qt::SkipEmptyParts);
            for ( const auto& raw : rawTags )
            {
                const QString t = raw.trimmed();
                if ( !t.isEmpty() )
                {
                    tags.push_back(t);
                }
            }
        }
        setWidgetsEnabled({submitLootButton_, refreshLootButton_, lootTable_}, false);
        toastInfo(tr("Submitting loot %1...").arg(info.fileName()));
        const uint64_t epoch = ctrl->ConnectionEpoch();

        struct Result {
            uint64_t epoch{0};
            bool ok{false};
            QString error;
            kelpieui::v1::LootItem created;
        };

        runAsync<Result>(
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
            [this](const Result& res) {
                setWidgetsEnabled({submitLootButton_, refreshLootButton_, lootTable_}, true);
                auto* ctrl = controller();
                if ( ctrl == nullptr || ctrl->ConnectionEpoch() != res.epoch )
                {
                    return;
                }
                if ( !res.ok )
                {
                    toastError(tr("Submit loot failed: %1").arg(res.error));
                    return;
                }
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
        if ( lootTable_ != nullptr )
        {
            const int row = lootTable_->currentRow();
            auto* nameItem = row >= 0 ? lootTable_->item(row, 1) : nullptr;
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

        setWidgetsEnabled({downloadLootButton_, refreshLootButton_, lootTable_}, false);
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

        runAsync<Result>(
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
            [this](const Result& res) {
                setWidgetsEnabled({downloadLootButton_, refreshLootButton_, lootTable_}, true);
                auto* ctrl = controller();
                if ( ctrl == nullptr || ctrl->ConnectionEpoch() != res.epoch )
                {
                    return;
                }
                if ( !res.ok )
                {
                    toastError(tr("Loot download failed: %1").arg(res.error));
                    return;
                }
                toastInfo(tr("Loot saved: %1 (%2 bytes)")
                              .arg(QFileInfo(res.savePath).fileName())
                              .arg(res.bytes));
            });
    }
}
