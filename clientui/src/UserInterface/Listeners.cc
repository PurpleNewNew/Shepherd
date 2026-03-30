#include <UserInterface/KelpiePanel.hpp>

#include "Internal.hpp"

#include <QInputDialog>
#include <QMessageBox>
#include <QTableWidgetItem>

#include <optional>

namespace StockmanNamespace::UserInterface
{
    namespace
    {
        void populatePivotListenersTable(QTableWidget* table, const std::vector<kelpieui::v1::PivotListener>& listeners)
        {
            if ( table == nullptr )
            {
                return;
            }
            table->setRowCount(static_cast<int>(listeners.size()));
            for ( std::size_t index = 0; index < listeners.size(); ++index )
            {
                const int row = static_cast<int>(index);
                const auto& listener = listeners[index];
                table->setItem(row, 0, new QTableWidgetItem(QString::fromStdString(listener.listener_id())));
                table->setItem(row, 1, new QTableWidgetItem(QString::fromStdString(listener.protocol())));
                table->setItem(row, 2, new QTableWidgetItem(QString::fromStdString(listener.bind())));
                table->setItem(row, 3, new QTableWidgetItem(pivotListenerModeText(listener.mode())));
                table->setItem(row, 4, new QTableWidgetItem(QString::fromStdString(listener.status())));
                table->setItem(row, 5, new QTableWidgetItem(QString::fromStdString(listener.target_uuid())));
            }
        }

        void populateControllerListenersTable(QTableWidget* table, const std::vector<kelpieui::v1::ControllerListener>& listeners)
        {
            if ( table == nullptr )
            {
                return;
            }
            table->setRowCount(static_cast<int>(listeners.size()));
            for ( std::size_t index = 0; index < listeners.size(); ++index )
            {
                const int row = static_cast<int>(index);
                const auto& listener = listeners[index];
                table->setItem(row, 0, new QTableWidgetItem(QString::fromStdString(listener.listener_id())));
                table->setItem(row, 1, new QTableWidgetItem(QString::fromStdString(listener.protocol())));
                table->setItem(row, 2, new QTableWidgetItem(QString::fromStdString(listener.bind())));
                table->setItem(row, 3, new QTableWidgetItem(controllerListenerStatusText(listener.status())));
                table->setItem(row, 4, new QTableWidgetItem(QString::fromStdString(listener.updated_at())));
            }
        }
    }

    void KelpiePanel::refreshListeners()
    {
        auto* ctrl = controller();
        if ( ctrl == nullptr )
        {
            if ( pivotListenersTable_ != nullptr ) { pivotListenersTable_->setRowCount(0); }
            if ( controllerListenersTable_ != nullptr ) { controllerListenersTable_->setRowCount(0); }
            return;
        }
        setWidgetsEnabled(
            {createPivotListenerButton_,
             updatePivotListenerButton_,
             deletePivotListenerButton_,
             pivotListenersTable_,
             createControllerListenerButton_,
             updateControllerListenerButton_,
             deleteControllerListenerButton_,
             controllerListenersTable_,
             refreshListenersButton_},
            false);
        const uint64_t epoch = ctrl->ConnectionEpoch();
        struct Result {
            uint64_t epoch{0};
            bool pivotOk{false};
            bool controllerOk{false};
            QString pivotError;
            QString controllerError;
            std::vector<kelpieui::v1::PivotListener> pivotListeners;
            std::vector<kelpieui::v1::ControllerListener> controllerListeners;
        };
        runAsync<Result>(
            this,
            [ctrl, epoch]() {
                Result res;
                res.epoch = epoch;
                QString pivotError;
                QString controllerError;
                std::vector<kelpieui::v1::PivotListener> pivotListeners;
                std::vector<kelpieui::v1::ControllerListener> controllerListeners;
                res.pivotOk = ctrl->ListPivotListeners(&pivotListeners, pivotError);
                res.controllerOk = ctrl->ListControllerListeners(&controllerListeners, controllerError);
                res.pivotError = pivotError;
                res.controllerError = controllerError;
                res.pivotListeners = std::move(pivotListeners);
                res.controllerListeners = std::move(controllerListeners);
                return res;
            },
            [this](const Result& res) {
                setWidgetsEnabled(
                    {createPivotListenerButton_,
                     updatePivotListenerButton_,
                     deletePivotListenerButton_,
                     pivotListenersTable_,
                     createControllerListenerButton_,
                     updateControllerListenerButton_,
                     deleteControllerListenerButton_,
                     controllerListenersTable_,
                     refreshListenersButton_},
                    true);
                auto* ctrl = controller();
                if ( ctrl == nullptr || ctrl->ConnectionEpoch() != res.epoch )
                {
                    return;
                }
                if ( res.pivotOk )
                {
                    populatePivotListenersTable(pivotListenersTable_, res.pivotListeners);
                }
                else if ( pivotListenersTable_ != nullptr )
                {
                    pivotListenersTable_->setRowCount(0);
                }
                if ( res.controllerOk )
                {
                    populateControllerListenersTable(controllerListenersTable_, res.controllerListeners);
                }
                else if ( controllerListenersTable_ != nullptr )
                {
                    controllerListenersTable_->setRowCount(0);
                }

                if ( !res.pivotOk || !res.controllerOk )
                {
                    QStringList errors;
                    if ( !res.pivotOk )
                    {
                        errors << tr("Pivot listeners: %1").arg(res.pivotError);
                    }
                    if ( !res.controllerOk )
                    {
                        errors << tr("Kelpie listeners: %1").arg(res.controllerError);
                    }
                    toastError(tr("Listeners refresh failed: %1").arg(errors.join(QStringLiteral("; "))));
                }
            });
    }

    QString KelpiePanel::currentPivotListenerId() const
    {
        if ( pivotListenersTable_ == nullptr )
        {
            return {};
        }
        const int row = pivotListenersTable_->currentRow();
        if ( row < 0 )
        {
            return {};
        }
        auto* idItem = pivotListenersTable_->item(row, 0);
        return (idItem != nullptr) ? idItem->text().trimmed() : QString();
    }

    QString KelpiePanel::currentControllerListenerId() const
    {
        if ( controllerListenersTable_ == nullptr )
        {
            return {};
        }
        const int row = controllerListenersTable_->currentRow();
        if ( row < 0 )
        {
            return {};
        }
        auto* idItem = controllerListenersTable_->item(row, 0);
        return (idItem != nullptr) ? idItem->text().trimmed() : QString();
    }

    void KelpiePanel::createPivotListener()
    {
        auto* ctrl = controller();
        if ( ctrl == nullptr )
        {
            toastWarn(tr("gRPC client not connected"));
            return;
        }
        bool ok = false;
        QString target = QInputDialog::getText(
            this, tr("Create Pivot Listener"), tr("Target UUID:"), QLineEdit::Normal, currentNodeUuid_, &ok);
        target = target.trimmed();
        if ( !ok || target.isEmpty() )
        {
            return;
        }
        QString protocol = QInputDialog::getText(
            this, tr("Create Pivot Listener"), tr("Protocol:"), QLineEdit::Normal, QStringLiteral("tcp"), &ok);
        protocol = protocol.trimmed();
        if ( !ok || protocol.isEmpty() )
        {
            return;
        }
        QString bind = QInputDialog::getText(
            this, tr("Create Pivot Listener"), tr("Bind:"), QLineEdit::Normal, QStringLiteral("0.0.0.0:9001"), &ok);
        bind = bind.trimmed();
        if ( !ok || bind.isEmpty() )
        {
            return;
        }
        const QStringList modes{tr("Normal"), tr("Iptables"), tr("SoReuse")};
        QString modeText = QInputDialog::getItem(
            this, tr("Create Pivot Listener"), tr("Mode:"), modes, 0, false, &ok);
        if ( !ok )
        {
            return;
        }

        kelpieui::v1::PivotListenerSpec spec;
        spec.set_protocol(protocol.toStdString());
        spec.set_bind(bind.toStdString());
        spec.set_mode(parsePivotMode(modeText));

        setWidgetsEnabled({createPivotListenerButton_, pivotListenersTable_, refreshListenersButton_}, false);
        toastInfo(tr("Creating pivot listener..."));
        const uint64_t epoch = ctrl->ConnectionEpoch();
        struct Result {
            uint64_t epoch{0};
            bool ok{false};
            QString error;
            kelpieui::v1::PivotListener created;
        };
        runAsync<Result>(
            this,
            [ctrl, epoch, target, spec]() {
                Result res;
                res.epoch = epoch;
                QString error;
                kelpieui::v1::PivotListener created;
                res.ok = ctrl->CreatePivotListener(target, spec, &created, error);
                res.error = error;
                res.created = created;
                return res;
            },
            [this](const Result& res) {
                setWidgetsEnabled({createPivotListenerButton_, pivotListenersTable_, refreshListenersButton_}, true);
                auto* ctrl = controller();
                if ( ctrl == nullptr || ctrl->ConnectionEpoch() != res.epoch )
                {
                    return;
                }
                if ( !res.ok )
                {
                    toastError(tr("Create pivot listener failed: %1").arg(res.error));
                    return;
                }
                toastInfo(tr("Pivot listener created: %1").arg(QString::fromStdString(res.created.listener_id())));
                refreshListeners();
            });
    }

    void KelpiePanel::editPivotListener()
    {
        auto* ctrl = controller();
        if ( ctrl == nullptr )
        {
            toastWarn(tr("gRPC client not connected"));
            return;
        }
        const QString listenerId = currentPivotListenerId();
        if ( listenerId.isEmpty() )
        {
            toastWarn(tr("Select a pivot listener row first"));
            return;
        }
        const int row = pivotListenersTable_->currentRow();
        auto* protoItem = pivotListenersTable_->item(row, 1);
        auto* bindItem = pivotListenersTable_->item(row, 2);
        auto* modeItem = pivotListenersTable_->item(row, 3);
        bool ok = false;
        QString protocol = QInputDialog::getText(
            this,
            tr("Edit Pivot Listener"),
            tr("Protocol:"),
            QLineEdit::Normal,
            (protoItem != nullptr) ? protoItem->text() : QStringLiteral("tcp"),
            &ok);
        protocol = protocol.trimmed();
        if ( !ok || protocol.isEmpty() )
        {
            return;
        }
        QString bind = QInputDialog::getText(
            this,
            tr("Edit Pivot Listener"),
            tr("Bind:"),
            QLineEdit::Normal,
            (bindItem != nullptr) ? bindItem->text() : QString(),
            &ok);
        bind = bind.trimmed();
        if ( !ok || bind.isEmpty() )
        {
            return;
        }
        const QStringList modes{tr("Normal"), tr("Iptables"), tr("SoReuse")};
        const qsizetype foundMode = modes.indexOf((modeItem != nullptr) ? modeItem->text() : QString());
        const int modeIndex = foundMode < 0 ? 0 : static_cast<int>(foundMode);
        QString modeText = QInputDialog::getItem(
            this, tr("Edit Pivot Listener"), tr("Mode:"), modes, modeIndex, false, &ok);
        if ( !ok )
        {
            return;
        }
        QString desiredStatus = QInputDialog::getText(
            this,
            tr("Edit Pivot Listener"),
            tr("Desired status (optional):"),
            QLineEdit::Normal,
            QString(),
            &ok);
        if ( !ok )
        {
            return;
        }

        kelpieui::v1::PivotListenerSpec spec;
        spec.set_protocol(protocol.toStdString());
        spec.set_bind(bind.toStdString());
        spec.set_mode(parsePivotMode(modeText));

        setWidgetsEnabled({updatePivotListenerButton_, deletePivotListenerButton_, createPivotListenerButton_, pivotListenersTable_, refreshListenersButton_}, false);
        toastInfo(tr("Updating pivot listener %1...").arg(listenerId));
        const uint64_t epoch = ctrl->ConnectionEpoch();
        struct Result {
            uint64_t epoch{0};
            QString listenerId;
            bool ok{false};
            QString error;
            kelpieui::v1::PivotListener updated;
        };
        const QString desired = desiredStatus.trimmed();
        runAsync<Result>(
            this,
            [ctrl, epoch, listenerId, spec, desired]() {
                Result res;
                res.epoch = epoch;
                res.listenerId = listenerId;
                QString error;
                kelpieui::v1::PivotListener updated;
                res.ok = ctrl->UpdatePivotListener(listenerId, spec, desired, &updated, error);
                res.error = error;
                res.updated = updated;
                return res;
            },
            [this](const Result& res) {
                setWidgetsEnabled({updatePivotListenerButton_, deletePivotListenerButton_, createPivotListenerButton_, pivotListenersTable_, refreshListenersButton_}, true);
                auto* ctrl = controller();
                if ( ctrl == nullptr || ctrl->ConnectionEpoch() != res.epoch )
                {
                    return;
                }
                if ( !res.ok )
                {
                    toastError(tr("Update pivot listener failed: %1").arg(res.error));
                    return;
                }
                toastInfo(tr("Pivot listener updated: %1").arg(QString::fromStdString(res.updated.listener_id())));
                refreshListeners();
            });
    }

    void KelpiePanel::deletePivotListener()
    {
        auto* ctrl = controller();
        if ( ctrl == nullptr )
        {
            toastWarn(tr("gRPC client not connected"));
            return;
        }
        const QString listenerId = currentPivotListenerId();
        if ( listenerId.isEmpty() )
        {
            toastWarn(tr("Select a pivot listener row first"));
            return;
        }
        if ( QMessageBox::question(
                 this,
                 tr("Delete Pivot Listener"),
                 tr("Delete pivot listener %1?").arg(listenerId)) != QMessageBox::Yes )
        {
            return;
        }
        setWidgetsEnabled({deletePivotListenerButton_, updatePivotListenerButton_, createPivotListenerButton_, pivotListenersTable_, refreshListenersButton_}, false);
        toastInfo(tr("Deleting pivot listener %1...").arg(listenerId));
        const uint64_t epoch = ctrl->ConnectionEpoch();
        struct Result {
            uint64_t epoch{0};
            QString listenerId;
            bool ok{false};
            QString error;
        };
        runAsync<Result>(
            this,
            [ctrl, epoch, listenerId]() {
                Result res;
                res.epoch = epoch;
                res.listenerId = listenerId;
                QString error;
                res.ok = ctrl->DeletePivotListener(listenerId, error);
                res.error = error;
                return res;
            },
            [this](const Result& res) {
                setWidgetsEnabled({deletePivotListenerButton_, updatePivotListenerButton_, createPivotListenerButton_, pivotListenersTable_, refreshListenersButton_}, true);
                auto* ctrl = controller();
                if ( ctrl == nullptr || ctrl->ConnectionEpoch() != res.epoch )
                {
                    return;
                }
                if ( !res.ok )
                {
                    toastError(tr("Delete pivot listener failed: %1").arg(res.error));
                    return;
                }
                toastInfo(tr("Pivot listener deleted: %1").arg(res.listenerId));
                refreshListeners();
            });
    }

    void KelpiePanel::createControllerListener()
    {
        auto* ctrl = controller();
        if ( ctrl == nullptr )
        {
            toastWarn(tr("gRPC client not connected"));
            return;
        }
        bool ok = false;
        QString protocol = QInputDialog::getText(
            this, tr("Create Kelpie Listener"), tr("Protocol:"), QLineEdit::Normal, QStringLiteral("grpc"), &ok);
        protocol = protocol.trimmed();
        if ( !ok || protocol.isEmpty() )
        {
            return;
        }
        QString bind = QInputDialog::getText(
            this, tr("Create Kelpie Listener"), tr("Bind:"), QLineEdit::Normal, QStringLiteral("0.0.0.0:8443"), &ok);
        bind = bind.trimmed();
        if ( !ok || bind.isEmpty() )
        {
            return;
        }

        kelpieui::v1::ControllerListenerSpec spec;
        spec.set_protocol(protocol.toStdString());
        spec.set_bind(bind.toStdString());
        setWidgetsEnabled({createControllerListenerButton_, updateControllerListenerButton_, deleteControllerListenerButton_, controllerListenersTable_, refreshListenersButton_}, false);
        toastInfo(tr("Creating Kelpie listener..."));
        const uint64_t epoch = ctrl->ConnectionEpoch();
        struct Result {
            uint64_t epoch{0};
            bool ok{false};
            QString error;
            kelpieui::v1::ControllerListener created;
        };
        runAsync<Result>(
            this,
            [ctrl, epoch, spec]() {
                Result res;
                res.epoch = epoch;
                QString error;
                kelpieui::v1::ControllerListener created;
                res.ok = ctrl->CreateControllerListener(spec, &created, error);
                res.error = error;
                res.created = created;
                return res;
            },
            [this](const Result& res) {
                setWidgetsEnabled({createControllerListenerButton_, updateControllerListenerButton_, deleteControllerListenerButton_, controllerListenersTable_, refreshListenersButton_}, true);
                auto* ctrl = controller();
                if ( ctrl == nullptr || ctrl->ConnectionEpoch() != res.epoch )
                {
                    return;
                }
                if ( !res.ok )
                {
                    toastError(tr("Create Kelpie listener failed: %1").arg(res.error));
                    return;
                }
                toastInfo(tr("Kelpie listener created: %1").arg(QString::fromStdString(res.created.listener_id())));
                refreshListeners();
            });
    }

    void KelpiePanel::editControllerListener()
    {
        auto* ctrl = controller();
        if ( ctrl == nullptr )
        {
            toastWarn(tr("gRPC client not connected"));
            return;
        }
        const QString listenerId = currentControllerListenerId();
        if ( listenerId.isEmpty() )
        {
            toastWarn(tr("Select a Kelpie listener row first"));
            return;
        }
        const int row = controllerListenersTable_->currentRow();
        auto* protoItem = controllerListenersTable_->item(row, 1);
        auto* bindItem = controllerListenersTable_->item(row, 2);
        bool ok = false;
        QString protocol = QInputDialog::getText(
            this,
            tr("Edit Kelpie Listener"),
            tr("Protocol:"),
            QLineEdit::Normal,
            (protoItem != nullptr) ? protoItem->text() : QStringLiteral("grpc"),
            &ok);
        protocol = protocol.trimmed();
        if ( !ok || protocol.isEmpty() )
        {
            return;
        }
        QString bind = QInputDialog::getText(
            this,
            tr("Edit Kelpie Listener"),
            tr("Bind:"),
            QLineEdit::Normal,
            (bindItem != nullptr) ? bindItem->text() : QString(),
            &ok);
        bind = bind.trimmed();
        if ( !ok || bind.isEmpty() )
        {
            return;
        }
        const QStringList statuses{
            tr("Keep"),
            tr("Pending"),
            tr("Running"),
            tr("Stopped"),
            tr("Failed"),
        };
        QString statusChoice = QInputDialog::getItem(
            this, tr("Edit Kelpie Listener"), tr("Desired status:"), statuses, 0, false, &ok);
        if ( !ok )
        {
            return;
        }

        kelpieui::v1::ControllerListenerSpec spec;
        spec.set_protocol(protocol.toStdString());
        spec.set_bind(bind.toStdString());
        std::optional<kelpieui::v1::ControllerListenerStatus> desiredStatus;
        const QString normalized = statusChoice.trimmed().toLower();
        if ( normalized == QStringLiteral("pending") )
        {
            desiredStatus = kelpieui::v1::CONTROLLER_LISTENER_STATUS_PENDING;
        }
        else if ( normalized == QStringLiteral("running") )
        {
            desiredStatus = kelpieui::v1::CONTROLLER_LISTENER_STATUS_RUNNING;
        }
        else if ( normalized == QStringLiteral("stopped") )
        {
            desiredStatus = kelpieui::v1::CONTROLLER_LISTENER_STATUS_STOPPED;
        }
        else if ( normalized == QStringLiteral("failed") )
        {
            desiredStatus = kelpieui::v1::CONTROLLER_LISTENER_STATUS_FAILED;
        }

        setWidgetsEnabled({createControllerListenerButton_, updateControllerListenerButton_, deleteControllerListenerButton_, controllerListenersTable_, refreshListenersButton_}, false);
        toastInfo(tr("Updating Kelpie listener %1...").arg(listenerId));
        const uint64_t epoch = ctrl->ConnectionEpoch();
        struct Result {
            uint64_t epoch{0};
            QString listenerId;
            bool ok{false};
            QString error;
            kelpieui::v1::ControllerListener updated;
        };
        runAsync<Result>(
            this,
            [ctrl, epoch, listenerId, spec, desiredStatus]() {
                Result res;
                res.epoch = epoch;
                res.listenerId = listenerId;
                QString error;
                kelpieui::v1::ControllerListener updated;
                res.ok = ctrl->UpdateControllerListener(listenerId, &spec, desiredStatus, &updated, error);
                res.error = error;
                res.updated = updated;
                return res;
            },
            [this](const Result& res) {
                setWidgetsEnabled({createControllerListenerButton_, updateControllerListenerButton_, deleteControllerListenerButton_, controllerListenersTable_, refreshListenersButton_}, true);
                auto* ctrl = controller();
                if ( ctrl == nullptr || ctrl->ConnectionEpoch() != res.epoch )
                {
                    return;
                }
                if ( !res.ok )
                {
                    toastError(tr("Update Kelpie listener failed: %1").arg(res.error));
                    return;
                }
                toastInfo(tr("Kelpie listener updated: %1").arg(QString::fromStdString(res.updated.listener_id())));
                refreshListeners();
            });
    }

    void KelpiePanel::deleteControllerListener()
    {
        auto* ctrl = controller();
        if ( ctrl == nullptr )
        {
            toastWarn(tr("gRPC client not connected"));
            return;
        }
        const QString listenerId = currentControllerListenerId();
        if ( listenerId.isEmpty() )
        {
            toastWarn(tr("Select a Kelpie listener row first"));
            return;
        }
        if ( QMessageBox::question(
                 this,
                 tr("Delete Kelpie Listener"),
                 tr("Delete Kelpie listener %1?").arg(listenerId)) != QMessageBox::Yes )
        {
            return;
        }
        setWidgetsEnabled({createControllerListenerButton_, updateControllerListenerButton_, deleteControllerListenerButton_, controllerListenersTable_, refreshListenersButton_}, false);
        toastInfo(tr("Deleting Kelpie listener %1...").arg(listenerId));
        const uint64_t epoch = ctrl->ConnectionEpoch();
        struct Result {
            uint64_t epoch{0};
            QString listenerId;
            bool ok{false};
            QString error;
            kelpieui::v1::ControllerListener removed;
        };
        runAsync<Result>(
            this,
            [ctrl, epoch, listenerId]() {
                Result res;
                res.epoch = epoch;
                res.listenerId = listenerId;
                QString error;
                kelpieui::v1::ControllerListener removed;
                res.ok = ctrl->DeleteControllerListener(listenerId, &removed, error);
                res.error = error;
                res.removed = removed;
                return res;
            },
            [this](const Result& res) {
                setWidgetsEnabled({createControllerListenerButton_, updateControllerListenerButton_, deleteControllerListenerButton_, controllerListenersTable_, refreshListenersButton_}, true);
                auto* ctrl = controller();
                if ( ctrl == nullptr || ctrl->ConnectionEpoch() != res.epoch )
                {
                    return;
                }
                if ( !res.ok )
                {
                    toastError(tr("Delete Kelpie listener failed: %1").arg(res.error));
                    return;
                }
                toastInfo(tr("Kelpie listener deleted: %1").arg(res.listenerId));
                refreshListeners();
            });
    }
}
