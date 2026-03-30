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

                if ( streamDiagTable_ != nullptr )
                {
                    streamDiagTable_->setRowCount(streamCount);
                    for ( int i = 0; i < streamCount; ++i )
                    {
                        const auto& s = res.response.streams(i);
                        streamDiagTable_->setItem(i, 0, new QTableWidgetItem(QString::number(s.stream_id())));
                        streamDiagTable_->setItem(i, 1, new QTableWidgetItem(QString::fromStdString(s.target_uuid())));
                        streamDiagTable_->setItem(i, 2, new QTableWidgetItem(QString::fromStdString(s.kind())));
                        streamDiagTable_->setItem(i, 3, new QTableWidgetItem(QString::number(s.pending())));
                        streamDiagTable_->setItem(i, 4, new QTableWidgetItem(QString::number(s.inflight())));
                        streamDiagTable_->setItem(i,
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

        if ( sleepSecondsInput_ != nullptr ) { sleepSecondsInput_->setEnabled(false);
}
        if ( workSecondsInput_ != nullptr ) { workSecondsInput_->setEnabled(false);
}
        if ( jitterInput_ != nullptr ) { jitterInput_->setEnabled(false);
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
                if ( sleepSecondsInput_ ) { sleepSecondsInput_->setEnabled(true);
}
                if ( workSecondsInput_ ) { workSecondsInput_->setEnabled(true);
}
                if ( jitterInput_ ) { jitterInput_->setEnabled(true);
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
                    if ( sleepSecondsInput_ ) { sleepSecondsInput_->clear();
}
                    if ( workSecondsInput_ ) { workSecondsInput_->clear();
}
                    if ( jitterInput_ ) { jitterInput_->clear();
}
                    return;
                }
                if ( sleepSecondsInput_ )
                {
                    sleepSecondsInput_->setText(res.sleepSeconds ? QString::number(*res.sleepSeconds) : QString());
                }
                if ( workSecondsInput_ )
                {
                    workSecondsInput_->setText(res.workSeconds ? QString::number(*res.workSeconds) : QString());
                }
                if ( jitterInput_ )
                {
                    jitterInput_->setText(res.jitterPercent ? QString::number(*res.jitterPercent) : QString());
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
        int s = (sleepSecondsInput_ != nullptr) ? sleepSecondsInput_->text().toInt(&ok) : 0;
        if ( ok ) { sleep = s;
}
        int w = (workSecondsInput_ != nullptr) ? workSecondsInput_->text().toInt(&ok) : 0;
        if ( ok ) { work = w;
}
        double j = (jitterInput_ != nullptr) ? jitterInput_->text().toDouble(&ok) : 0.0;
        if ( ok ) { jitter = j;
}
        const QString targetUuid = currentNodeUuid_;
        setWidgetsEnabled({updateSleepButton_, sleepSecondsInput_, workSecondsInput_, jitterInput_}, false);
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
                setWidgetsEnabled({updateSleepButton_, sleepSecondsInput_, workSecondsInput_, jitterInput_}, true);
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
        if ( (ctrl == nullptr) || (dialTable_ == nullptr) )
        {
            return;
        }
        dialTable_->setEnabled(false);
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
                if ( dialTable_ ) { dialTable_->setEnabled(true);
}

                auto* ctrl = controller();
                if ( ctrl == nullptr || ctrl->ConnectionEpoch() != res.epoch )
                {
                    return;
                }
                if ( !res.ok )
                {
                    toastError(tr("Dial list failed: %1").arg(res.error));
                    if ( dialTable_ ) { dialTable_->setRowCount(0);
}
                    return;
                }
                dialTable_->setRowCount(static_cast<int>(res.dials.size()));
                for (size_t i = 0; i < res.dials.size(); ++i)
                {
                    const auto& d = res.dials[i];
                    dialTable_->setItem(static_cast<int>(i), 0, new QTableWidgetItem(QString::fromStdString(d.dial_id())));
                    dialTable_->setItem(static_cast<int>(i), 1, new QTableWidgetItem(QString::fromStdString(d.target_uuid())));
                    dialTable_->setItem(static_cast<int>(i), 2, new QTableWidgetItem(QString::fromStdString(d.address())));
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
                    dialTable_->setItem(static_cast<int>(i), 3, new QTableWidgetItem(stateText));
                    QString reason = QString::fromStdString(d.reason());
                    if ( reason.isEmpty() )
                    {
                        reason = QString::fromStdString(d.error());
                    }
                    dialTable_->setItem(static_cast<int>(i), 4, new QTableWidgetItem(reason));
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
        const QString addr = (dialAddressInput_ != nullptr) ? dialAddressInput_->text() : QString();
        if ( addr.isEmpty() )
        {
            toastWarn(tr("Dial address required"));
            return;
        }
        const QString reason = (dialReasonInput_ != nullptr) ? dialReasonInput_->text() : QString();
        const QString targetUuid = currentNodeUuid_;
        setWidgetsEnabled({startDialButton_, dialAddressInput_, dialReasonInput_}, false);
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
                setWidgetsEnabled({startDialButton_, dialAddressInput_, dialReasonInput_}, true);
                auto* ctrl = controller();
                if ( ctrl == nullptr || ctrl->ConnectionEpoch() != res.epoch )
                {
                    return;
                }
                if ( !res.ok )
                {
                    toastError(tr("Start dial failed: %1").arg(res.error));
                    if ( opsStatusLabel_ ) { opsStatusLabel_->setText(tr("Dial failed: %1").arg(res.error));
}
                    return;
                }
                const QString id = QString::fromStdString(res.resp.dial_id());
                toastInfo(tr("Dial %1 accepted=%2").arg(id).arg(res.resp.accepted()));
                if ( opsStatusLabel_ ) { opsStatusLabel_->setText(tr("Dial %1 accepted").arg(id));
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
        auto sel = (dialTable_ != nullptr) ? dialTable_->currentRow() : -1;
        if ( sel < 0 )
        {
            toastWarn(tr("Select a dial row first"));
            return;
        }
        auto* idItem = dialTable_->item(sel, 0);
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
        setWidgetsEnabled({cancelDialButton_, dialTable_}, false);
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
                setWidgetsEnabled({cancelDialButton_, dialTable_}, true);
                auto* ctrl = controller();
                if ( ctrl == nullptr || ctrl->ConnectionEpoch() != res.epoch )
                {
                    return;
                }
                if ( !res.ok )
                {
                    toastError(tr("Cancel dial failed: %1").arg(res.error));
                    if ( opsStatusLabel_ ) { opsStatusLabel_->setText(tr("Cancel failed: %1").arg(res.error));
}
                    return;
                }
                toastInfo(tr("Dial %1 canceled=%2").arg(res.dialId).arg(res.resp.canceled()));
                if ( opsStatusLabel_ ) { opsStatusLabel_->setText(tr("Dial %1 canceled").arg(res.dialId));
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
        const QString server = (sshServerInput_ != nullptr) ? sshServerInput_->text().trimmed() : QString();
        if ( server.isEmpty() )
        {
            toastWarn(tr("SSH server address required"));
            return;
        }
        const QString user = (sshUserInput_ != nullptr) ? sshUserInput_->text() : QString();
        const QString pass = (sshPassInput_ != nullptr) ? sshPassInput_->text() : QString();
        const auto method = static_cast<kelpieui::v1::SshTunnelAuthMethod>((sshAuthCombo_ != nullptr) ? sshAuthCombo_->currentData().toInt() : 0);
        if ( method == kelpieui::v1::SSH_TUNNEL_AUTH_METHOD_CERT )
        {
            toastWarn(tr("SSH session currently supports password auth only. Use SSH tunnel for certificate auth."));
            return;
        }

        setWidgetsEnabled({startSshSessionButton_, startSshTunnelButton_, sshTable_, sshServerInput_, sshUserInput_, sshPassInput_, sshAuthCombo_}, false);
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
                setWidgetsEnabled({startSshSessionButton_, startSshTunnelButton_, sshTable_, sshServerInput_, sshUserInput_, sshPassInput_, sshAuthCombo_}, true);
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
                    sshStreams_.push_back(res.handle);
                    connect(res.handle.get(), &ProxyStreamHandle::Closed, this, [this, sid](const QString&) {
                        sshStreams_.erase(
                            std::remove_if(sshStreams_.begin(),
                                           sshStreams_.end(),
                                           [&sid](const std::shared_ptr<StockmanNamespace::ProxyStreamHandle>& h) {
                                               return !h || h->SessionId() == sid;
                                           }),
                            sshStreams_.end());
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
        const QString server = (sshServerInput_ != nullptr) ? sshServerInput_->text().trimmed() : QString();
        const QString agentPort = (sshTunnelPortInput_ != nullptr) ? sshTunnelPortInput_->text().trimmed() : QString();
        if ( server.isEmpty() || agentPort.isEmpty() )
        {
            toastWarn(tr("SSH server and agent port are required"));
            return;
        }
        const QString user = (sshUserInput_ != nullptr) ? sshUserInput_->text() : QString();
        const QString passOrKey = (sshPassInput_ != nullptr) ? sshPassInput_->text() : QString();
        const auto method = static_cast<kelpieui::v1::SshTunnelAuthMethod>((sshAuthCombo_ != nullptr) ? sshAuthCombo_->currentData().toInt() : 0);
        const QString password = method == kelpieui::v1::SSH_TUNNEL_AUTH_METHOD_CERT ? QString() : passOrKey;
        const QByteArray privateKey = method == kelpieui::v1::SSH_TUNNEL_AUTH_METHOD_CERT ? passOrKey.toUtf8() : QByteArray();

        setWidgetsEnabled({startSshSessionButton_,
                    startSshTunnelButton_,
                    sshTable_,
                    sshServerInput_,
                    sshTunnelPortInput_,
                    sshUserInput_,
                    sshPassInput_,
                    sshAuthCombo_},
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
                setWidgetsEnabled({startSshSessionButton_,
                            startSshTunnelButton_,
                            sshTable_,
                            sshServerInput_,
                            sshTunnelPortInput_,
                            sshUserInput_,
                            sshPassInput_,
                            sshAuthCombo_},
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
        if ( sshTable_ == nullptr )
        {
            return;
        }
        sshTable_->setEnabled(false);
        sshTable_->setRowCount(0);
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
            sshTable_->insertRow(row);
            sshTable_->setItem(row, 0, new QTableWidgetItem(lowerKind.contains(QStringLiteral("tunnel")) ? tr("Tunnel") : tr("Session")));
            sshTable_->setItem(row, 1, new QTableWidgetItem(QString::fromStdString(stream.target_uuid())));
            sshTable_->setItem(row, 2, new QTableWidgetItem(endpoint));
            sshTable_->setItem(row, 3, new QTableWidgetItem(status));
            ++row;
        }
        if ( row == 0 )
        {
            sshTable_->setRowCount(1);
            auto* placeholder = new QTableWidgetItem(tr("No active SSH streams"));
            placeholder->setFlags(Qt::NoItemFlags);
            sshTable_->setItem(0, 0, placeholder);
        }
        sshTable_->setEnabled(true);
    }
}
