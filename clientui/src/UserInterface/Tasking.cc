#include <UserInterface/KelpiePanel.hpp>
#include <UserInterface/Pages/ShellPage.hpp>
#include <UserInterface/Pages/TaskingPage.hpp>

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

        if ( taskingPage_ != nullptr && taskingPage_->sleepSecondsInput != nullptr ) { taskingPage_->sleepSecondsInput->setEnabled(false);
}
        if ( taskingPage_ != nullptr && taskingPage_->workSecondsInput != nullptr ) { taskingPage_->workSecondsInput->setEnabled(false);
}
        if ( taskingPage_ != nullptr && taskingPage_->jitterInput != nullptr ) { taskingPage_->jitterInput->setEnabled(false);
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

        runAsyncGuarded<Result>(
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
            [this]() {
                if ( taskingPage_ != nullptr )
                {
                    if ( taskingPage_->sleepSecondsInput ) { taskingPage_->sleepSecondsInput->setEnabled(true);
}
                    if ( taskingPage_->workSecondsInput ) { taskingPage_->workSecondsInput->setEnabled(true);
}
                    if ( taskingPage_->jitterInput ) { taskingPage_->jitterInput->setEnabled(true);
}
                }
            },
            [this](const Result& res) {
                auto* currentCtrl = controller();
                return (currentCtrl != nullptr) && (currentCtrl->ConnectionEpoch() == res.epoch) && (res.targetUuid == currentNodeUuid_);
            },
            [this](const Result& res) {
                toastError(tr("Sleep profiles failed: %1").arg(res.error));
            },
            [this](const Result& res) {
                if ( !res.found )
                {
                    if ( taskingPage_->sleepSecondsInput ) { taskingPage_->sleepSecondsInput->clear();
}
                    if ( taskingPage_->workSecondsInput ) { taskingPage_->workSecondsInput->clear();
}
                    if ( taskingPage_->jitterInput ) { taskingPage_->jitterInput->clear();
}
                    return;
                }
                if ( taskingPage_->sleepSecondsInput )
                {
                    taskingPage_->sleepSecondsInput->setText(res.sleepSeconds ? QString::number(*res.sleepSeconds) : QString());
                }
                if ( taskingPage_->workSecondsInput )
                {
                    taskingPage_->workSecondsInput->setText(res.workSeconds ? QString::number(*res.workSeconds) : QString());
                }
                if ( taskingPage_->jitterInput )
                {
                    taskingPage_->jitterInput->setText(res.jitterPercent ? QString::number(*res.jitterPercent) : QString());
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
        int s = (taskingPage_ != nullptr && taskingPage_->sleepSecondsInput != nullptr) ? taskingPage_->sleepSecondsInput->text().toInt(&ok) : 0;
        if ( ok ) { sleep = s;
}
        int w = (taskingPage_ != nullptr && taskingPage_->workSecondsInput != nullptr) ? taskingPage_->workSecondsInput->text().toInt(&ok) : 0;
        if ( ok ) { work = w;
}
        double j = (taskingPage_ != nullptr && taskingPage_->jitterInput != nullptr) ? taskingPage_->jitterInput->text().toDouble(&ok) : 0.0;
        if ( ok ) { jitter = j;
}
        const QString targetUuid = currentNodeUuid_;
        setWidgetsEnabled({taskingPage_->updateSleepButton, taskingPage_->sleepSecondsInput, taskingPage_->workSecondsInput, taskingPage_->jitterInput}, false);
        toastInfo(tr("Updating sleep profile for %1...").arg(targetUuid));
        const uint64_t epoch = ctrl->ConnectionEpoch();
        struct Result {
            uint64_t epoch{0};
            QString targetUuid;
            bool ok{false};
            QString error;
        };
        runAsyncGuarded<Result>(
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
            [this]() {
                setWidgetsEnabled({taskingPage_->updateSleepButton, taskingPage_->sleepSecondsInput, taskingPage_->workSecondsInput, taskingPage_->jitterInput}, true);
            },
            [this](const Result& res) {
                auto* currentCtrl = controller();
                return (currentCtrl != nullptr) && (currentCtrl->ConnectionEpoch() == res.epoch);
            },
            [this](const Result& res) {
                toastError(tr("Update sleep failed: %1").arg(res.error));
            },
            [this](const Result& res) {
                toastInfo(tr("Updated sleep for %1").arg(res.targetUuid));
                refreshSleep();
                controller()->RequestSnapshotRefresh();
            });
    }

    void KelpiePanel::refreshDials()
    {
        auto* ctrl = controller();
        if ( (ctrl == nullptr) || (taskingPage_ == nullptr) || (taskingPage_->dialTable == nullptr) )
        {
            return;
        }
        taskingPage_->dialTable->setEnabled(false);
        const uint64_t epoch = ctrl->ConnectionEpoch();

        struct Result {
            uint64_t epoch{0};
            bool ok{false};
            QString error;
            std::vector<kelpieui::v1::DialStatus> dials;
        };

        runAsyncGuarded<Result>(
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
            [this]() {
                if ( taskingPage_ != nullptr && taskingPage_->dialTable ) { taskingPage_->dialTable->setEnabled(true);
}
            },
            [this](const Result& res) {
                auto* currentCtrl = controller();
                return (currentCtrl != nullptr) && (currentCtrl->ConnectionEpoch() == res.epoch);
            },
            [this](const Result& res) {
                toastError(tr("Dial list failed: %1").arg(res.error));
                if ( taskingPage_ != nullptr && taskingPage_->dialTable ) { taskingPage_->dialTable->setRowCount(0);
}
            },
            [this](const Result& res) {
                taskingPage_->dialTable->setRowCount(static_cast<int>(res.dials.size()));
                for (size_t i = 0; i < res.dials.size(); ++i)
                {
                    const auto& d = res.dials[i];
                    taskingPage_->dialTable->setItem(static_cast<int>(i), 0, new QTableWidgetItem(QString::fromStdString(d.dial_id())));
                    taskingPage_->dialTable->setItem(static_cast<int>(i), 1, new QTableWidgetItem(QString::fromStdString(d.target_uuid())));
                    taskingPage_->dialTable->setItem(static_cast<int>(i), 2, new QTableWidgetItem(QString::fromStdString(d.address())));
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
                    taskingPage_->dialTable->setItem(static_cast<int>(i), 3, new QTableWidgetItem(stateText));
                    QString reason = QString::fromStdString(d.reason());
                    if ( reason.isEmpty() )
                    {
                        reason = QString::fromStdString(d.error());
                    }
                    taskingPage_->dialTable->setItem(static_cast<int>(i), 4, new QTableWidgetItem(reason));
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
        const QString addr = (taskingPage_ != nullptr && taskingPage_->dialAddressInput != nullptr) ? taskingPage_->dialAddressInput->text() : QString();
        if ( addr.isEmpty() )
        {
            toastWarn(tr("Dial address required"));
            return;
        }
        const QString reason = (taskingPage_ != nullptr && taskingPage_->dialReasonInput != nullptr) ? taskingPage_->dialReasonInput->text() : QString();
        const QString targetUuid = currentNodeUuid_;
        setWidgetsEnabled({taskingPage_->startDialButton, taskingPage_->dialAddressInput, taskingPage_->dialReasonInput}, false);
        toastInfo(tr("Starting dial for %1...").arg(targetUuid));

        const uint64_t epoch = ctrl->ConnectionEpoch();
        struct Result {
            uint64_t epoch{0};
            bool ok{false};
            QString error;
            kelpieui::v1::StartDialResponse resp;
        };
        runAsyncGuarded<Result>(
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
            [this]() {
                setWidgetsEnabled({taskingPage_->startDialButton, taskingPage_->dialAddressInput, taskingPage_->dialReasonInput}, true);
            },
            [this](const Result& res) {
                auto* currentCtrl = controller();
                return (currentCtrl != nullptr) && (currentCtrl->ConnectionEpoch() == res.epoch);
            },
            [this](const Result& res) {
                toastError(tr("Start dial failed: %1").arg(res.error));
                if ( shellPage_ != nullptr && shellPage_->opsStatusLabel ) { shellPage_->opsStatusLabel->setText(tr("Dial failed: %1").arg(res.error));
}
            },
            [this](const Result& res) {
                const QString id = QString::fromStdString(res.resp.dial_id());
                toastInfo(tr("Dial %1 accepted=%2").arg(id).arg(res.resp.accepted()));
                if ( shellPage_ != nullptr && shellPage_->opsStatusLabel ) { shellPage_->opsStatusLabel->setText(tr("Dial %1 accepted").arg(id));
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
        auto sel = (taskingPage_ != nullptr && taskingPage_->dialTable != nullptr) ? taskingPage_->dialTable->currentRow() : -1;
        if ( sel < 0 )
        {
            toastWarn(tr("Select a dial row first"));
            return;
        }
        auto* idItem = taskingPage_->dialTable->item(sel, 0);
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
        setWidgetsEnabled({taskingPage_->cancelDialButton, taskingPage_->dialTable}, false);
        toastInfo(tr("Canceling dial %1...").arg(dialId));
        const uint64_t epoch = ctrl->ConnectionEpoch();
        struct Result {
            uint64_t epoch{0};
            QString dialId;
            bool ok{false};
            QString error;
            kelpieui::v1::CancelDialResponse resp;
        };
        runAsyncGuarded<Result>(
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
            [this]() {
                setWidgetsEnabled({taskingPage_->cancelDialButton, taskingPage_->dialTable}, true);
            },
            [this](const Result& res) {
                auto* currentCtrl = controller();
                return (currentCtrl != nullptr) && (currentCtrl->ConnectionEpoch() == res.epoch);
            },
            [this](const Result& res) {
                toastError(tr("Cancel dial failed: %1").arg(res.error));
                if ( shellPage_ != nullptr && shellPage_->opsStatusLabel ) { shellPage_->opsStatusLabel->setText(tr("Cancel failed: %1").arg(res.error));
}
            },
            [this](const Result& res) {
                toastInfo(tr("Dial %1 canceled=%2").arg(res.dialId).arg(res.resp.canceled()));
                if ( shellPage_ != nullptr && shellPage_->opsStatusLabel ) { shellPage_->opsStatusLabel->setText(tr("Dial %1 canceled").arg(res.dialId));
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
        const QString server = (taskingPage_ != nullptr && taskingPage_->sshServerInput != nullptr) ? taskingPage_->sshServerInput->text().trimmed() : QString();
        if ( server.isEmpty() )
        {
            toastWarn(tr("SSH server address required"));
            return;
        }
        const QString user = (taskingPage_ != nullptr && taskingPage_->sshUserInput != nullptr) ? taskingPage_->sshUserInput->text() : QString();
        const QString pass = (taskingPage_ != nullptr && taskingPage_->sshPassInput != nullptr) ? taskingPage_->sshPassInput->text() : QString();
        const auto method = static_cast<kelpieui::v1::SshTunnelAuthMethod>((taskingPage_ != nullptr && taskingPage_->sshAuthCombo != nullptr) ? taskingPage_->sshAuthCombo->currentData().toInt() : 0);
        if ( method == kelpieui::v1::SSH_TUNNEL_AUTH_METHOD_CERT )
        {
            toastWarn(tr("SSH session currently supports password auth only. Use SSH tunnel for certificate auth."));
            return;
        }

        setWidgetsEnabled({taskingPage_->startSshSessionButton, taskingPage_->startSshTunnelButton, taskingPage_->sshTable, taskingPage_->sshServerInput, taskingPage_->sshUserInput, taskingPage_->sshPassInput, taskingPage_->sshAuthCombo}, false);
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

        runAsyncGuarded<Result>(
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
            [this]() {
                setWidgetsEnabled({taskingPage_->startSshSessionButton, taskingPage_->startSshTunnelButton, taskingPage_->sshTable, taskingPage_->sshServerInput, taskingPage_->sshUserInput, taskingPage_->sshPassInput, taskingPage_->sshAuthCombo}, true);
            },
            [this](const Result& res) {
                auto* currentCtrl = controller();
                const bool sameEpoch = (currentCtrl != nullptr) && (currentCtrl->ConnectionEpoch() == res.epoch);
                if ( !sameEpoch && res.handle )
                {
                    closeHandleAsync(res.handle);
                }
                return sameEpoch;
            },
            [this](const Result& res) {
                toastError(tr("SSH session failed: %1").arg(res.error));
            },
            [this](const Result& res) {
                if ( res.handle )
                {
                    const QString sid = res.handle->SessionId();
                    taskingPage_->sshStreams.push_back(res.handle);
                    connect(res.handle.get(), &ProxyStreamHandle::Closed, this, [this, sid](const QString&) {
                        taskingPage_->sshStreams.erase(
                            std::remove_if(taskingPage_->sshStreams.begin(),
                                           taskingPage_->sshStreams.end(),
                                           [&sid](const std::shared_ptr<StockmanNamespace::ProxyStreamHandle>& h) {
                                               return !h || h->SessionId() == sid;
                                           }),
                            taskingPage_->sshStreams.end());
                    });
                }
                toastInfo(tr("SSH session started to %1").arg(res.server));
                controller()->RequestSnapshotRefresh();
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
        const QString server = (taskingPage_ != nullptr && taskingPage_->sshServerInput != nullptr) ? taskingPage_->sshServerInput->text().trimmed() : QString();
        const QString agentPort = (taskingPage_ != nullptr && taskingPage_->sshTunnelPortInput != nullptr) ? taskingPage_->sshTunnelPortInput->text().trimmed() : QString();
        if ( server.isEmpty() || agentPort.isEmpty() )
        {
            toastWarn(tr("SSH server and agent port are required"));
            return;
        }
        const QString user = (taskingPage_ != nullptr && taskingPage_->sshUserInput != nullptr) ? taskingPage_->sshUserInput->text() : QString();
        const QString passOrKey = (taskingPage_ != nullptr && taskingPage_->sshPassInput != nullptr) ? taskingPage_->sshPassInput->text() : QString();
        const auto method = static_cast<kelpieui::v1::SshTunnelAuthMethod>((taskingPage_ != nullptr && taskingPage_->sshAuthCombo != nullptr) ? taskingPage_->sshAuthCombo->currentData().toInt() : 0);
        const QString password = method == kelpieui::v1::SSH_TUNNEL_AUTH_METHOD_CERT ? QString() : passOrKey;
        const QByteArray privateKey = method == kelpieui::v1::SSH_TUNNEL_AUTH_METHOD_CERT ? passOrKey.toUtf8() : QByteArray();

        setWidgetsEnabled({taskingPage_->startSshSessionButton,
                    taskingPage_->startSshTunnelButton,
                    taskingPage_->sshTable,
                    taskingPage_->sshServerInput,
                    taskingPage_->sshTunnelPortInput,
                    taskingPage_->sshUserInput,
                    taskingPage_->sshPassInput,
                    taskingPage_->sshAuthCombo},
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

        runAsyncGuarded<Result>(
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
            [this]() {
                setWidgetsEnabled({taskingPage_->startSshSessionButton,
                            taskingPage_->startSshTunnelButton,
                            taskingPage_->sshTable,
                            taskingPage_->sshServerInput,
                            taskingPage_->sshTunnelPortInput,
                            taskingPage_->sshUserInput,
                            taskingPage_->sshPassInput,
                            taskingPage_->sshAuthCombo},
                           true);
            },
            [this](const Result& res) {
                auto* currentCtrl = controller();
                return (currentCtrl != nullptr) && (currentCtrl->ConnectionEpoch() == res.epoch);
            },
            [this](const Result& res) {
                toastError(tr("SSH tunnel failed: %1").arg(res.error));
            },
            [this](const Result& res) {
                toastInfo(tr("SSH tunnel started: %1/%2").arg(res.server, res.agentPort));
                controller()->RequestSnapshotRefresh();
            });
    }

    void KelpiePanel::refreshSsh()
    {
        if ( taskingPage_ == nullptr || taskingPage_->sshTable == nullptr )
        {
            return;
        }
        taskingPage_->sshTable->setEnabled(false);
        taskingPage_->sshTable->setRowCount(0);
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
            taskingPage_->sshTable->insertRow(row);
            taskingPage_->sshTable->setItem(row, 0, new QTableWidgetItem(lowerKind.contains(QStringLiteral("tunnel")) ? tr("Tunnel") : tr("Session")));
            taskingPage_->sshTable->setItem(row, 1, new QTableWidgetItem(QString::fromStdString(stream.target_uuid())));
            taskingPage_->sshTable->setItem(row, 2, new QTableWidgetItem(endpoint));
            taskingPage_->sshTable->setItem(row, 3, new QTableWidgetItem(status));
            ++row;
        }
        if ( row == 0 )
        {
            taskingPage_->sshTable->setRowCount(1);
            auto* placeholder = new QTableWidgetItem(tr("No active SSH streams"));
            placeholder->setFlags(Qt::NoItemFlags);
            taskingPage_->sshTable->setItem(0, 0, placeholder);
        }
        taskingPage_->sshTable->setEnabled(true);
    }
}
