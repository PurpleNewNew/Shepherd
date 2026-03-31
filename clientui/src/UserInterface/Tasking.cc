#include <UserInterface/KelpiePanel.hpp>

#include "Internal.hpp"

#include <QMessageBox>
#include <QTableWidgetItem>

#include <Stockman/KelpieState.hpp>

#include <algorithm>
#include <optional>

namespace StockmanNamespace::UserInterface
{
    void KelpiePanel::refreshSessionList()
    {
        auto* ctrl = controller();
        if ( ctrl == nullptr || sessionListTable_ == nullptr )
        {
            return;
        }
        if ( refreshSessionListButton_ != nullptr ) { refreshSessionListButton_->setEnabled(false);
}
        const uint64_t epoch = ctrl->ConnectionEpoch();
        const QString targetsText = (sessionListTargetsInput_ != nullptr) ? sessionListTargetsInput_->text() : QString();
        const bool includeInactive = (sessionIncludeInactiveCheck_ != nullptr) ? sessionIncludeInactiveCheck_->isChecked() : false;
        const auto status =
            (sessionStatusBox_ != nullptr) ? static_cast<kelpieui::v1::SessionStatus>(sessionStatusBox_->currentData().toInt())
                              : kelpieui::v1::SESSION_STATUS_UNSPECIFIED;

        QStringList targets;
        for (const auto& part : targetsText.split(',', Qt::SkipEmptyParts))
        {
            const QString t = part.trimmed();
            if ( !t.isEmpty() )
            {
                targets.push_back(t);
            }
        }
        std::vector<kelpieui::v1::SessionStatus> statuses;
        if ( status != kelpieui::v1::SESSION_STATUS_UNSPECIFIED )
        {
            statuses.push_back(status);
        }

        struct Result {
            uint64_t epoch{0};
            bool ok{false};
            QString error;
            std::vector<kelpieui::v1::SessionInfo> sessions;
        };

        runAsync<Result>(
            this,
            [ctrl, targets, statuses = std::move(statuses), includeInactive, epoch]() mutable {
                Result res;
                res.epoch = epoch;
                QString error;
                std::vector<kelpieui::v1::SessionInfo> sessions;
                res.ok = ctrl->ListSessions(targets, statuses, includeInactive, &sessions, error);
                res.error = error;
                res.sessions = std::move(sessions);
                return res;
            },
            [this](const Result& res) {
                if ( refreshSessionListButton_ ) { refreshSessionListButton_->setEnabled(true);
}

                auto* ctrl = controller();
                if ( ctrl == nullptr || ctrl->ConnectionEpoch() != res.epoch )
                {
                    return;
                }
                if ( !res.ok )
                {
                    toastError(tr("Session list refresh failed: %1").arg(res.error));
                    sessionListTable_->setRowCount(0);
                    return;
                }

                sessionListTable_->setRowCount(static_cast<int>(res.sessions.size()));
                for (size_t i = 0; i < res.sessions.size(); ++i)
                {
                    const auto& s = res.sessions[i];
                    const int row = static_cast<int>(i);
                    sessionListTable_->setItem(row, 0, new QTableWidgetItem(QString::fromStdString(s.target_uuid())));
                    sessionListTable_->setItem(row, 1, new QTableWidgetItem(sessionStatusText(s)));
                    sessionListTable_->setItem(row, 2, new QTableWidgetItem(s.active() ? tr("yes") : tr("no")));
                    sessionListTable_->setItem(row, 3, new QTableWidgetItem(s.connected() ? tr("yes") : tr("no")));
                    sessionListTable_->setItem(row, 4, new QTableWidgetItem(QString::fromStdString(s.remote_addr())));
                    sessionListTable_->setItem(row, 5, new QTableWidgetItem(QString::fromStdString(s.upstream())));
                    sessionListTable_->setItem(row, 6, new QTableWidgetItem(QString::fromStdString(s.downstream())));
                    sessionListTable_->setItem(row, 7, new QTableWidgetItem(QString::fromStdString(s.network_id())));
                    sessionListTable_->setItem(row, 8, new QTableWidgetItem(QString::fromStdString(s.last_seen())));
                    sessionListTable_->setItem(row, 9, new QTableWidgetItem(QString::fromStdString(s.last_error())));
                    sessionListTable_->setItem(row, 10, new QTableWidgetItem(QString::fromStdString(s.sleep_profile())));
                }
            });
    }

    void KelpiePanel::markCurrentSession(kelpieui::v1::SessionMarkAction action)
    {
        auto* ctrl = controller();
        if ( ctrl == nullptr )
        {
            toastWarn(tr("gRPC client not connected"));
            return;
        }
        if ( currentNodeUuid_.isEmpty() )
        {
            toastWarn(tr("Select a node first"));
            return;
        }
        const QString targetUuid = currentNodeUuid_;
        const QString reason = (sessionReasonInput_ != nullptr) ? sessionReasonInput_->text().trimmed() : QString();
        setWidgetsEnabled({markAliveButton_, markDeadButton_}, false);
        toastInfo(action == kelpieui::v1::SESSION_MARK_ACTION_DEAD ? tr("Marking session dead...") : tr("Marking session alive..."));

        const uint64_t epoch = ctrl->ConnectionEpoch();
        struct Result {
            uint64_t epoch{0};
            QString targetUuid;
            kelpieui::v1::SessionMarkAction action{kelpieui::v1::SESSION_MARK_ACTION_UNSPECIFIED};
            bool ok{false};
            QString error;
        };

        runAsync<Result>(
            this,
            [ctrl, epoch, targetUuid, action, reason]() {
                Result res;
                res.epoch = epoch;
                res.targetUuid = targetUuid;
                res.action = action;
                QString error;
                res.ok = ctrl->MarkSession(targetUuid, action, reason, error);
                res.error = error;
                return res;
            },
            [this](const Result& res) {
                setWidgetsEnabled({markAliveButton_, markDeadButton_}, true);
                auto* ctrl = controller();
                if ( ctrl == nullptr || ctrl->ConnectionEpoch() != res.epoch )
                {
                    return;
                }
                if ( !res.ok )
                {
                    toastError(tr("Mark session failed: %1").arg(res.error));
                    return;
                }
                toastInfo(res.action == kelpieui::v1::SESSION_MARK_ACTION_DEAD
                              ? tr("Session marked dead: %1").arg(res.targetUuid)
                              : tr("Session marked alive: %1").arg(res.targetUuid));
                ctrl->RequestSnapshotRefresh();
            });
    }

    void KelpiePanel::queryNodeStatus()
    {
        auto* ctrl = controller();
        if ( ctrl == nullptr )
        {
            toastWarn(tr("gRPC client not connected"));
            return;
        }
        if ( currentNodeUuid_.isEmpty() )
        {
            toastWarn(tr("Select a node first"));
            return;
        }

        const QString targetUuid = currentNodeUuid_;
        setWidgetsEnabled({nodeStatusButton_}, false);
        toastInfo(tr("Querying node status for %1...").arg(targetUuid));

        const uint64_t epoch = ctrl->ConnectionEpoch();
        struct Result {
            uint64_t epoch{0};
            QString targetUuid;
            bool ok{false};
            QString error;
            kelpieui::v1::NodeStatusResponse response;
        };

        runAsync<Result>(
            this,
            [ctrl, epoch, targetUuid]() {
                Result res;
                res.epoch = epoch;
                res.targetUuid = targetUuid;
                QString error;
                kelpieui::v1::NodeStatusResponse response;
                res.ok = ctrl->NodeStatus(targetUuid, &response, error);
                res.error = error;
                res.response = response;
                return res;
            },
            [this](const Result& res) {
                setWidgetsEnabled({nodeStatusButton_}, true);
                auto* ctrl = controller();
                if ( ctrl == nullptr || ctrl->ConnectionEpoch() != res.epoch )
                {
                    return;
                }
                if ( !res.ok )
                {
                    toastError(tr("Node status failed: %1").arg(res.error));
                    return;
                }

                const auto& node = res.response.node();
                const QString alias = QString::fromStdString(node.alias());
                const QString status = QString::fromStdString(node.status());
                const int streamCount = res.response.streams_size();
                const int listenerCount = res.response.pivot_listeners_size();

                const QString statusText = status.isEmpty() ? tr("unknown") : status;
                toastInfo(
                    tr("Node %1 status=%2 streams=%3 pivot-listeners=%4")
                        .arg(res.targetUuid, statusText)
                        .arg(streamCount)
                        .arg(listenerCount));
                AppendLog(
                    tr("NodeStatus %1 alias=%2 status=%3 streams=%4 pivot_listeners=%5")
                        .arg(res.targetUuid,
                             alias.isEmpty() ? tr("-") : alias,
                             statusText)
                        .arg(streamCount)
                        .arg(listenerCount));

                if ( auto* item = nodesTree_ ? nodesTree_->currentItem() : nullptr )
                {
                    if ( item->text(0) == res.targetUuid )
                    {
                        item->setText(2, statusText);
                        item->setText(10, QString::number(streamCount));
                    }
                }

                if ( streams_.diagnosticsTable != nullptr )
                {
                    streams_.diagnosticsTable->setRowCount(streamCount);
                    for ( int i = 0; i < streamCount; ++i )
                    {
                        const auto& s = res.response.streams(i);
                        streams_.diagnosticsTable->setItem(i, 0, new QTableWidgetItem(QString::number(s.stream_id())));
                        streams_.diagnosticsTable->setItem(i, 1, new QTableWidgetItem(QString::fromStdString(s.target_uuid())));
                        streams_.diagnosticsTable->setItem(i, 2, new QTableWidgetItem(QString::fromStdString(s.kind())));
                        streams_.diagnosticsTable->setItem(i, 3, new QTableWidgetItem(QString::number(s.pending())));
                        streams_.diagnosticsTable->setItem(i, 4, new QTableWidgetItem(QString::number(s.inflight())));
                        streams_.diagnosticsTable->setItem(
                            i,
                            5,
                            new QTableWidgetItem(
                                tr("%1 / %2")
                                    .arg(QString::fromStdString(s.rto()),
                                         QString::fromStdString(s.last_activity()))));
                    }
                }
                if ( pivotListenersTable_ != nullptr )
                {
                    pivotListenersTable_->setRowCount(listenerCount);
                    for ( int i = 0; i < listenerCount; ++i )
                    {
                        const auto& listener = res.response.pivot_listeners(i);
                        pivotListenersTable_->setItem(i, 0, new QTableWidgetItem(QString::fromStdString(listener.listener_id())));
                        pivotListenersTable_->setItem(i, 1, new QTableWidgetItem(QString::fromStdString(listener.protocol())));
                        pivotListenersTable_->setItem(i, 2, new QTableWidgetItem(QString::fromStdString(listener.bind())));
                        pivotListenersTable_->setItem(i, 3, new QTableWidgetItem(pivotListenerModeText(listener.mode())));
                        pivotListenersTable_->setItem(i, 4, new QTableWidgetItem(QString::fromStdString(listener.status())));
                        pivotListenersTable_->setItem(i, 5, new QTableWidgetItem(QString::fromStdString(listener.target_uuid())));
                    }
                }

                refreshStreamDiagnostics();
            });
    }

    void KelpiePanel::repairCurrentSession()
    {
        auto* ctrl = controller();
        if ( ctrl == nullptr )
        {
            toastWarn(tr("gRPC client not connected"));
            return;
        }
        if ( currentNodeUuid_.isEmpty() )
        {
            toastWarn(tr("Select a node first"));
            return;
        }
        const QString targetUuid = currentNodeUuid_;
        const bool force = (repairForceCheck_ != nullptr) ? repairForceCheck_->isChecked() : false;
        const QString reason = (sessionReasonInput_ != nullptr) ? sessionReasonInput_->text().trimmed() : QString();
        setWidgetsEnabled({repairSessionButton_}, false);
        toastInfo(tr("Requesting repair for %1...").arg(targetUuid));

        const uint64_t epoch = ctrl->ConnectionEpoch();
        struct Result {
            uint64_t epoch{0};
            QString targetUuid;
            bool force{false};
            bool ok{false};
            QString error;
        };
        runAsync<Result>(
            this,
            [ctrl, epoch, targetUuid, force, reason]() {
                Result res;
                res.epoch = epoch;
                res.targetUuid = targetUuid;
                res.force = force;
                QString error;
                res.ok = ctrl->RepairSession(targetUuid, force, reason, error);
                res.error = error;
                return res;
            },
            [this](const Result& res) {
                setWidgetsEnabled({repairSessionButton_}, true);
                auto* ctrl = controller();
                if ( ctrl == nullptr || ctrl->ConnectionEpoch() != res.epoch )
                {
                    return;
                }
                if ( !res.ok )
                {
                    toastError(tr("Repair session failed: %1").arg(res.error));
                    return;
                }
                toastInfo(tr("Repair requested for %1 (force=%2)").arg(res.targetUuid).arg(res.force));
                refreshRepairs();
                ctrl->RequestSnapshotRefresh();
            });
    }

    void KelpiePanel::reconnectCurrentSession()
    {
        auto* ctrl = controller();
        if ( ctrl == nullptr )
        {
            toastWarn(tr("gRPC client not connected"));
            return;
        }
        if ( currentNodeUuid_.isEmpty() )
        {
            toastWarn(tr("Select a node first"));
            return;
        }
        const QString targetUuid = currentNodeUuid_;
        const QString reason = (sessionReasonInput_ != nullptr) ? sessionReasonInput_->text().trimmed() : QString();
        setWidgetsEnabled({reconnectSessionButton_}, false);
        toastInfo(tr("Requesting reconnect for %1...").arg(targetUuid));

        const uint64_t epoch = ctrl->ConnectionEpoch();
        struct Result {
            uint64_t epoch{0};
            QString targetUuid;
            bool ok{false};
            QString error;
        };
        runAsync<Result>(
            this,
            [ctrl, epoch, targetUuid, reason]() {
                Result res;
                res.epoch = epoch;
                res.targetUuid = targetUuid;
                QString error;
                res.ok = ctrl->ReconnectSession(targetUuid, reason, error);
                res.error = error;
                return res;
            },
            [this](const Result& res) {
                setWidgetsEnabled({reconnectSessionButton_}, true);
                auto* ctrl = controller();
                if ( ctrl == nullptr || ctrl->ConnectionEpoch() != res.epoch )
                {
                    return;
                }
                if ( !res.ok )
                {
                    toastError(tr("Reconnect session failed: %1").arg(res.error));
                    return;
                }
                toastInfo(tr("Reconnect requested for %1").arg(res.targetUuid));
                ctrl->RequestSnapshotRefresh();
            });
    }

    void KelpiePanel::terminateCurrentSession()
    {
        auto* ctrl = controller();
        if ( ctrl == nullptr )
        {
            toastWarn(tr("gRPC client not connected"));
            return;
        }
        if ( currentNodeUuid_.isEmpty() )
        {
            toastWarn(tr("Select a node first"));
            return;
        }
        if ( QMessageBox::question(
                 this,
                 tr("Terminate Session"),
                 tr("Terminate current session for node %1?").arg(currentNodeUuid_)) != QMessageBox::Yes )
        {
            return;
        }
        const QString targetUuid = currentNodeUuid_;
        const QString reason = (sessionReasonInput_ != nullptr) ? sessionReasonInput_->text().trimmed() : QString();
        setWidgetsEnabled({terminateSessionButton_}, false);
        toastInfo(tr("Terminating session for %1...").arg(targetUuid));

        const uint64_t epoch = ctrl->ConnectionEpoch();
        struct Result {
            uint64_t epoch{0};
            QString targetUuid;
            bool ok{false};
            QString error;
        };
        runAsync<Result>(
            this,
            [ctrl, epoch, targetUuid, reason]() {
                Result res;
                res.epoch = epoch;
                res.targetUuid = targetUuid;
                QString error;
                res.ok = ctrl->TerminateSession(targetUuid, reason, error);
                res.error = error;
                return res;
            },
            [this](const Result& res) {
                setWidgetsEnabled({terminateSessionButton_}, true);
                auto* ctrl = controller();
                if ( ctrl == nullptr || ctrl->ConnectionEpoch() != res.epoch )
                {
                    return;
                }
                if ( !res.ok )
                {
                    toastError(tr("Terminate session failed: %1").arg(res.error));
                    return;
                }
                toastInfo(tr("Terminate requested for %1").arg(res.targetUuid));
                ctrl->RequestSnapshotRefresh();
            });
    }

    void KelpiePanel::shutdownCurrentNode()
    {
        auto* ctrl = controller();
        if ( ctrl == nullptr )
        {
            toastWarn(tr("gRPC client not connected"));
            return;
        }
        if ( currentNodeUuid_.isEmpty() )
        {
            toastWarn(tr("Select a node first"));
            return;
        }
        if ( QMessageBox::question(
                 this,
                 tr("Shutdown Node"),
                 tr("Shutdown node %1?").arg(currentNodeUuid_)) != QMessageBox::Yes )
        {
            return;
        }
        const QString targetUuid = currentNodeUuid_;
        setWidgetsEnabled({shutdownNodeButton_}, false);
        toastWarn(tr("Shutting down node %1...").arg(targetUuid));

        const uint64_t epoch = ctrl->ConnectionEpoch();
        struct Result {
            uint64_t epoch{0};
            QString targetUuid;
            bool ok{false};
            QString error;
        };
        runAsync<Result>(
            this,
            [ctrl, epoch, targetUuid]() {
                Result res;
                res.epoch = epoch;
                res.targetUuid = targetUuid;
                QString error;
                res.ok = ctrl->ShutdownNode(targetUuid, error);
                res.error = error;
                return res;
            },
            [this](const Result& res) {
                setWidgetsEnabled({shutdownNodeButton_}, true);
                auto* ctrl = controller();
                if ( ctrl == nullptr || ctrl->ConnectionEpoch() != res.epoch )
                {
                    return;
                }
                if ( !res.ok )
                {
                    toastError(tr("Shutdown node failed: %1").arg(res.error));
                    return;
                }
                toastWarn(tr("Shutdown requested for %1").arg(res.targetUuid));
                ctrl->RequestSnapshotRefresh();
            });
    }

    void KelpiePanel::refreshSleep()
    {
        auto* ctrl = controller();
        if ( ctrl == nullptr )
        {
            return;
        }
        const QString targetUuid = currentNodeUuid_.trimmed();
        if ( targetUuid.isEmpty() )
        {
            return;
        }

        if ( tasking_.sleepSecondsInput != nullptr ) { tasking_.sleepSecondsInput->setEnabled(false);
}
        if ( tasking_.workSecondsInput != nullptr ) { tasking_.workSecondsInput->setEnabled(false);
}
        if ( tasking_.jitterInput != nullptr ) { tasking_.jitterInput->setEnabled(false);
}
        const uint64_t epoch = ctrl->ConnectionEpoch();

        struct Result {
            uint64_t epoch{0};
            QString targetUuid;
            bool ok{false};
            QString error;
            bool found{false};
            std::optional<int> sleepSeconds;
            std::optional<int> workSeconds;
            std::optional<double> jitterPercent;
        };

        runAsync<Result>(
            this,
            [ctrl, epoch, targetUuid]() {
                Result res;
                res.epoch = epoch;
                res.targetUuid = targetUuid;
                QString error;
                std::vector<kelpieui::v1::SleepProfile> profiles;
                res.ok = ctrl->ListSleepProfiles(&profiles, error);
                res.error = error;
                if ( !res.ok )
                {
                    return res;
                }
                for (const auto& p : profiles)
                {
                    if ( p.target_uuid() == targetUuid.toStdString() )
                    {
                        res.found = true;
                        if ( p.has_sleep_seconds() ) { res.sleepSeconds = static_cast<int>(p.sleep_seconds());
}
                        if ( p.has_work_seconds() ) { res.workSeconds = static_cast<int>(p.work_seconds());
}
                        if ( p.has_jitter_percent() ) { res.jitterPercent = p.jitter_percent();
}
                        break;
                    }
                }
                return res;
            },
            [this](const Result& res) {
                if ( tasking_.sleepSecondsInput ) { tasking_.sleepSecondsInput->setEnabled(true);
}
                if ( tasking_.workSecondsInput ) { tasking_.workSecondsInput->setEnabled(true);
}
                if ( tasking_.jitterInput ) { tasking_.jitterInput->setEnabled(true);
}

                auto* ctrl = controller();
                if ( ctrl == nullptr || ctrl->ConnectionEpoch() != res.epoch )
                {
                    return;
                }
                if ( res.targetUuid != currentNodeUuid_ )
                {
                    return;
                }
                if ( !res.ok )
                {
                    toastError(tr("Sleep profiles failed: %1").arg(res.error));
                    return;
                }
                if ( !res.found )
                {
                    if ( tasking_.sleepSecondsInput ) { tasking_.sleepSecondsInput->clear();
}
                    if ( tasking_.workSecondsInput ) { tasking_.workSecondsInput->clear();
}
                    if ( tasking_.jitterInput ) { tasking_.jitterInput->clear();
}
                    return;
                }
                if ( tasking_.sleepSecondsInput )
                {
                    tasking_.sleepSecondsInput->setText(res.sleepSeconds ? QString::number(*res.sleepSeconds) : QString());
                }
                if ( tasking_.workSecondsInput )
                {
                    tasking_.workSecondsInput->setText(res.workSeconds ? QString::number(*res.workSeconds) : QString());
                }
                if ( tasking_.jitterInput )
                {
                    tasking_.jitterInput->setText(res.jitterPercent ? QString::number(*res.jitterPercent) : QString());
                }
            });
    }

    void KelpiePanel::updateSleep()
    {
        auto* ctrl = controller();
        if ( ctrl == nullptr )
        {
            return;
        }
        if ( currentNodeUuid_.isEmpty() )
        {
            toastWarn(tr("Select a node first"));
            return;
        }
        std::optional<int> sleep, work;
        std::optional<double> jitter;
        bool ok = false;
        int s = (tasking_.sleepSecondsInput != nullptr) ? tasking_.sleepSecondsInput->text().toInt(&ok) : 0;
        if ( ok ) { sleep = s;
}
        int w = (tasking_.workSecondsInput != nullptr) ? tasking_.workSecondsInput->text().toInt(&ok) : 0;
        if ( ok ) { work = w;
}
        double j = (tasking_.jitterInput != nullptr) ? tasking_.jitterInput->text().toDouble(&ok) : 0.0;
        if ( ok ) { jitter = j;
}
        const QString targetUuid = currentNodeUuid_;
        setWidgetsEnabled({tasking_.updateSleepButton, tasking_.sleepSecondsInput, tasking_.workSecondsInput, tasking_.jitterInput}, false);
        toastInfo(tr("Updating sleep profile for %1...").arg(targetUuid));
        const uint64_t epoch = ctrl->ConnectionEpoch();
        struct Result {
            uint64_t epoch{0};
            QString targetUuid;
            bool ok{false};
            QString error;
        };
        runAsync<Result>(
            this,
            [ctrl, epoch, targetUuid, sleep, work, jitter]() {
                Result res;
                res.epoch = epoch;
                res.targetUuid = targetUuid;
                QString error;
                res.ok = ctrl->UpdateSleep(targetUuid, sleep, work, jitter, error);
                res.error = error;
                return res;
            },
            [this](const Result& res) {
                setWidgetsEnabled({tasking_.updateSleepButton, tasking_.sleepSecondsInput, tasking_.workSecondsInput, tasking_.jitterInput}, true);
                auto* ctrl = controller();
                if ( ctrl == nullptr || ctrl->ConnectionEpoch() != res.epoch )
                {
                    return;
                }
                if ( !res.ok )
                {
                    toastError(tr("Update sleep failed: %1").arg(res.error));
                    return;
                }
                toastInfo(tr("Updated sleep for %1").arg(res.targetUuid));
                refreshSleep();
                ctrl->RequestSnapshotRefresh();
            });
    }

    void KelpiePanel::refreshDials()
    {
        auto* ctrl = controller();
        if ( (ctrl == nullptr) || (tasking_.dialTable == nullptr) )
        {
            return;
        }
        tasking_.dialTable->setEnabled(false);
        const uint64_t epoch = ctrl->ConnectionEpoch();

        struct Result {
            uint64_t epoch{0};
            bool ok{false};
            QString error;
            std::vector<kelpieui::v1::DialStatus> dials;
        };

        runAsync<Result>(
            this,
            [ctrl, epoch]() {
                Result res;
                res.epoch = epoch;
                QString error;
                std::vector<kelpieui::v1::DialStatus> dials;
                res.ok = ctrl->ListDials(&dials, error);
                res.error = error;
                res.dials = std::move(dials);
                return res;
            },
            [this](const Result& res) {
                if ( tasking_.dialTable ) { tasking_.dialTable->setEnabled(true);
}

                auto* ctrl = controller();
                if ( ctrl == nullptr || ctrl->ConnectionEpoch() != res.epoch )
                {
                    return;
                }
                if ( !res.ok )
                {
                    toastError(tr("Dial list failed: %1").arg(res.error));
                    if ( tasking_.dialTable ) { tasking_.dialTable->setRowCount(0);
}
                    return;
                }
                tasking_.dialTable->setRowCount(static_cast<int>(res.dials.size()));
                for (size_t i = 0; i < res.dials.size(); ++i)
                {
                    const auto& d = res.dials[i];
                    tasking_.dialTable->setItem(static_cast<int>(i), 0, new QTableWidgetItem(QString::fromStdString(d.dial_id())));
                    tasking_.dialTable->setItem(static_cast<int>(i), 1, new QTableWidgetItem(QString::fromStdString(d.target_uuid())));
                    tasking_.dialTable->setItem(static_cast<int>(i), 2, new QTableWidgetItem(QString::fromStdString(d.address())));
                    QString stateText;
                    switch ( d.state() )
                    {
                        case kelpieui::v1::DIAL_STATE_ENQUEUED: stateText = tr("Enqueued"); break;
                        case kelpieui::v1::DIAL_STATE_RUNNING: stateText = tr("Running"); break;
                        case kelpieui::v1::DIAL_STATE_SUCCEEDED: stateText = tr("Succeeded"); break;
                        case kelpieui::v1::DIAL_STATE_FAILED: stateText = tr("Failed"); break;
                        case kelpieui::v1::DIAL_STATE_CANCELED: stateText = tr("Canceled"); break;
                        default: stateText = tr("Unknown"); break;
                    }
                    tasking_.dialTable->setItem(static_cast<int>(i), 3, new QTableWidgetItem(stateText));
                    QString reason = QString::fromStdString(d.reason());
                    if ( reason.isEmpty() )
                    {
                        reason = QString::fromStdString(d.error());
                    }
                    tasking_.dialTable->setItem(static_cast<int>(i), 4, new QTableWidgetItem(reason));
                }
            });
    }

    void KelpiePanel::startDial()
    {
        auto* ctrl = controller();
        if ( ctrl == nullptr )
        {
            return;
        }
        if ( currentNodeUuid_.isEmpty() )
        {
            toastWarn(tr("Select a node first"));
            return;
        }
        const QString addr = (tasking_.dialAddressInput != nullptr) ? tasking_.dialAddressInput->text() : QString();
        if ( addr.isEmpty() )
        {
            toastWarn(tr("Dial address required"));
            return;
        }
        const QString reason = (tasking_.dialReasonInput != nullptr) ? tasking_.dialReasonInput->text() : QString();
        const QString targetUuid = currentNodeUuid_;
        setWidgetsEnabled({tasking_.startDialButton, tasking_.dialAddressInput, tasking_.dialReasonInput}, false);
        toastInfo(tr("Starting dial for %1...").arg(targetUuid));

        const uint64_t epoch = ctrl->ConnectionEpoch();
        struct Result {
            uint64_t epoch{0};
            bool ok{false};
            QString error;
            kelpieui::v1::StartDialResponse resp;
        };
        runAsync<Result>(
            this,
            [ctrl, epoch, targetUuid, addr, reason]() {
                Result res;
                res.epoch = epoch;
                QString error;
                kelpieui::v1::StartDialResponse resp;
                res.ok = ctrl->StartDial(targetUuid, addr, reason, &resp, error);
                res.error = error;
                res.resp = resp;
                return res;
            },
            [this](const Result& res) {
                setWidgetsEnabled({tasking_.startDialButton, tasking_.dialAddressInput, tasking_.dialReasonInput}, true);
                auto* ctrl = controller();
                if ( ctrl == nullptr || ctrl->ConnectionEpoch() != res.epoch )
                {
                    return;
                }
                if ( !res.ok )
                {
                    toastError(tr("Start dial failed: %1").arg(res.error));
                    if ( shell_.opsStatusLabel ) { shell_.opsStatusLabel->setText(tr("Dial failed: %1").arg(res.error));
}
                    return;
                }
                const QString id = QString::fromStdString(res.resp.dial_id());
                toastInfo(tr("Dial %1 accepted=%2").arg(id).arg(res.resp.accepted()));
                if ( shell_.opsStatusLabel ) { shell_.opsStatusLabel->setText(tr("Dial %1 accepted").arg(id));
}
                refreshDials();
            });
    }

    void KelpiePanel::cancelDial()
    {
        auto* ctrl = controller();
        if ( ctrl == nullptr )
        {
            return;
        }
        auto sel = (tasking_.dialTable != nullptr) ? tasking_.dialTable->currentRow() : -1;
        if ( sel < 0 )
        {
            toastWarn(tr("Select a dial row first"));
            return;
        }
        auto* idItem = tasking_.dialTable->item(sel, 0);
        if ( idItem == nullptr )
        {
            return;
        }
        const QString dialId = idItem->text().trimmed();
        if ( dialId.isEmpty() )
        {
            toastWarn(tr("Invalid dial id"));
            return;
        }
        setWidgetsEnabled({tasking_.cancelDialButton, tasking_.dialTable}, false);
        toastInfo(tr("Canceling dial %1...").arg(dialId));
        const uint64_t epoch = ctrl->ConnectionEpoch();
        struct Result {
            uint64_t epoch{0};
            QString dialId;
            bool ok{false};
            QString error;
            kelpieui::v1::CancelDialResponse resp;
        };
        runAsync<Result>(
            this,
            [ctrl, epoch, dialId]() {
                Result res;
                res.epoch = epoch;
                res.dialId = dialId;
                QString error;
                kelpieui::v1::CancelDialResponse resp;
                res.ok = ctrl->CancelDial(dialId, &resp, error);
                res.error = error;
                res.resp = resp;
                return res;
            },
            [this](const Result& res) {
                setWidgetsEnabled({tasking_.cancelDialButton, tasking_.dialTable}, true);
                auto* ctrl = controller();
                if ( ctrl == nullptr || ctrl->ConnectionEpoch() != res.epoch )
                {
                    return;
                }
                if ( !res.ok )
                {
                    toastError(tr("Cancel dial failed: %1").arg(res.error));
                    if ( shell_.opsStatusLabel ) { shell_.opsStatusLabel->setText(tr("Cancel failed: %1").arg(res.error));
}
                    return;
                }
                toastInfo(tr("Dial %1 canceled=%2").arg(res.dialId).arg(res.resp.canceled()));
                if ( shell_.opsStatusLabel ) { shell_.opsStatusLabel->setText(tr("Dial %1 canceled").arg(res.dialId));
}
                refreshDials();
            });
    }

    void KelpiePanel::startSshSession()
    {
        auto* ctrl = controller();
        if ( ctrl == nullptr )
        {
            return;
        }
        if ( currentNodeUuid_.isEmpty() )
        {
            toastWarn(tr("Select a node first"));
            return;
        }
        const QString targetUuid = currentNodeUuid_;
        const QString server = (tasking_.sshServerInput != nullptr) ? tasking_.sshServerInput->text().trimmed() : QString();
        if ( server.isEmpty() )
        {
            toastWarn(tr("SSH server address required"));
            return;
        }
        const QString user = (tasking_.sshUserInput != nullptr) ? tasking_.sshUserInput->text() : QString();
        const QString pass = (tasking_.sshPassInput != nullptr) ? tasking_.sshPassInput->text() : QString();
        const auto method = static_cast<kelpieui::v1::SshTunnelAuthMethod>((tasking_.sshAuthCombo != nullptr) ? tasking_.sshAuthCombo->currentData().toInt() : 0);
        if ( method == kelpieui::v1::SSH_TUNNEL_AUTH_METHOD_CERT )
        {
            toastWarn(tr("SSH session currently supports password auth only. Use SSH tunnel for certificate auth."));
            return;
        }

        setWidgetsEnabled({tasking_.startSshSessionButton, tasking_.startSshTunnelButton, tasking_.sshTable, tasking_.sshServerInput, tasking_.sshUserInput, tasking_.sshPassInput, tasking_.sshAuthCombo}, false);
        toastInfo(tr("Starting SSH session to %1...").arg(server));
        const uint64_t epoch = ctrl->ConnectionEpoch();

        struct Result {
            uint64_t epoch{0};
            QString targetUuid;
            QString server;
            bool ok{false};
            QString error;
            std::shared_ptr<StockmanNamespace::ProxyStreamHandle> handle;
        };

        runAsync<Result>(
            this,
            [ctrl, epoch, targetUuid, server, user, pass]() {
                Result res;
                res.epoch = epoch;
                res.targetUuid = targetUuid;
                res.server = server;
                QString error;
                auto handle = ctrl->StartSshSession(targetUuid, server, user, pass, error);
                res.ok = (handle != nullptr);
                res.error = error;
                res.handle = std::move(handle);
                return res;
            },
            [this](const Result& res) {
                setWidgetsEnabled({tasking_.startSshSessionButton, tasking_.startSshTunnelButton, tasking_.sshTable, tasking_.sshServerInput, tasking_.sshUserInput, tasking_.sshPassInput, tasking_.sshAuthCombo}, true);
                auto* ctrl = controller();
                if ( ctrl == nullptr || ctrl->ConnectionEpoch() != res.epoch )
                {
                    if ( res.handle )
                    {
                        closeHandleAsync(res.handle);
                    }
                    return;
                }
                if ( !res.ok )
                {
                    toastError(tr("SSH session failed: %1").arg(res.error));
                    return;
                }

                if ( res.handle )
                {
                    const QString sid = res.handle->SessionId();
                    tasking_.sshStreams.push_back(res.handle);
                    connect(res.handle.get(), &ProxyStreamHandle::Closed, this, [this, sid](const QString&) {
                        tasking_.sshStreams.erase(
                            std::remove_if(tasking_.sshStreams.begin(),
                                           tasking_.sshStreams.end(),
                                           [&sid](const std::shared_ptr<StockmanNamespace::ProxyStreamHandle>& h) {
                                               return !h || h->SessionId() == sid;
                                           }),
                            tasking_.sshStreams.end());
                    });
                }
                toastInfo(tr("SSH session started to %1").arg(res.server));
                if ( ctrl ) { ctrl->RequestSnapshotRefresh();
}
            });
    }

    void KelpiePanel::startSshTunnel()
    {
        auto* ctrl = controller();
        if ( ctrl == nullptr )
        {
            return;
        }
        if ( currentNodeUuid_.isEmpty() )
        {
            toastWarn(tr("Select a node first"));
            return;
        }
        const QString targetUuid = currentNodeUuid_;
        const QString server = (tasking_.sshServerInput != nullptr) ? tasking_.sshServerInput->text().trimmed() : QString();
        const QString agentPort = (tasking_.sshTunnelPortInput != nullptr) ? tasking_.sshTunnelPortInput->text().trimmed() : QString();
        if ( server.isEmpty() || agentPort.isEmpty() )
        {
            toastWarn(tr("SSH server and agent port are required"));
            return;
        }
        const QString user = (tasking_.sshUserInput != nullptr) ? tasking_.sshUserInput->text() : QString();
        const QString passOrKey = (tasking_.sshPassInput != nullptr) ? tasking_.sshPassInput->text() : QString();
        const auto method = static_cast<kelpieui::v1::SshTunnelAuthMethod>((tasking_.sshAuthCombo != nullptr) ? tasking_.sshAuthCombo->currentData().toInt() : 0);
        const QString password = method == kelpieui::v1::SSH_TUNNEL_AUTH_METHOD_CERT ? QString() : passOrKey;
        const QByteArray privateKey = method == kelpieui::v1::SSH_TUNNEL_AUTH_METHOD_CERT ? passOrKey.toUtf8() : QByteArray();

        setWidgetsEnabled({tasking_.startSshSessionButton,
                    tasking_.startSshTunnelButton,
                    tasking_.sshTable,
                    tasking_.sshServerInput,
                    tasking_.sshTunnelPortInput,
                    tasking_.sshUserInput,
                    tasking_.sshPassInput,
                    tasking_.sshAuthCombo},
                   false);
        toastInfo(tr("Starting SSH tunnel to %1...").arg(server));
        const uint64_t epoch = ctrl->ConnectionEpoch();

        struct Result {
            uint64_t epoch{0};
            QString server;
            QString agentPort;
            bool ok{false};
            QString error;
        };

        runAsync<Result>(
            this,
            [ctrl, epoch, targetUuid, server, agentPort, method, user, password, privateKey]() {
                Result res;
                res.epoch = epoch;
                res.server = server;
                res.agentPort = agentPort;
                QString error;
                res.ok = ctrl->StartSshTunnel(targetUuid, server, agentPort, method, user, password, privateKey, error);
                res.error = error;
                return res;
            },
            [this](const Result& res) {
                setWidgetsEnabled({tasking_.startSshSessionButton,
                            tasking_.startSshTunnelButton,
                            tasking_.sshTable,
                            tasking_.sshServerInput,
                            tasking_.sshTunnelPortInput,
                            tasking_.sshUserInput,
                            tasking_.sshPassInput,
                            tasking_.sshAuthCombo},
                           true);
                auto* ctrl = controller();
                if ( ctrl == nullptr || ctrl->ConnectionEpoch() != res.epoch )
                {
                    return;
                }
                if ( !res.ok )
                {
                    toastError(tr("SSH tunnel failed: %1").arg(res.error));
                    return;
                }
                toastInfo(tr("SSH tunnel started: %1/%2").arg(res.server, res.agentPort));
                ctrl->RequestSnapshotRefresh();
            });
    }

    void KelpiePanel::refreshSsh()
    {
        if ( tasking_.sshTable == nullptr )
        {
            return;
        }
        tasking_.sshTable->setEnabled(false);
        tasking_.sshTable->setRowCount(0);
        const auto* st = state();
        auto snapshot = (st != nullptr) ? st->Snapshot() : kelpieui::v1::Snapshot{};
        int row = 0;
        for (const auto& stream : snapshot.streams())
        {
            const QString kind = QString::fromStdString(stream.kind());
            const QString lowerKind = kind.toLower();
            if ( !lowerKind.contains(QStringLiteral("ssh")) )
            {
                continue;
            }
            const auto& meta = stream.metadata();
            auto metaValue = [&meta](const char* key) -> QString {
                auto it = meta.find(key);
                if ( it == meta.end() )
                {
                    return {};
                }
                return QString::fromStdString(it->second);
            };
            QString endpoint = metaValue("addr");
            if ( endpoint.isEmpty() )
            {
                endpoint = metaValue("server_addr");
            }
            if ( endpoint.isEmpty() )
            {
                endpoint = QStringLiteral("-");
            }
            QString status = stream.outbound() ? tr("active") : tr("incoming");
            const QString last = QString::fromStdString(stream.last_activity());
            if ( !last.isEmpty() )
            {
                status = tr("%1 @ %2").arg(status, last);
            }
            tasking_.sshTable->insertRow(row);
            tasking_.sshTable->setItem(row, 0, new QTableWidgetItem(lowerKind.contains(QStringLiteral("tunnel")) ? tr("Tunnel") : tr("Session")));
            tasking_.sshTable->setItem(row, 1, new QTableWidgetItem(QString::fromStdString(stream.target_uuid())));
            tasking_.sshTable->setItem(row, 2, new QTableWidgetItem(endpoint));
            tasking_.sshTable->setItem(row, 3, new QTableWidgetItem(status));
            ++row;
        }
        if ( row == 0 )
        {
            tasking_.sshTable->setRowCount(1);
            auto* placeholder = new QTableWidgetItem(tr("No active SSH streams"));
            placeholder->setFlags(Qt::NoItemFlags);
            tasking_.sshTable->setItem(0, 0, placeholder);
        }
        tasking_.sshTable->setEnabled(true);
    }
}
