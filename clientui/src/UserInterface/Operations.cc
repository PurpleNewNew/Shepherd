#include <UserInterface/KelpiePanel.hpp>

#include "Internal.hpp"

#include <QDateTime>
#include <QTableWidgetItem>

#include <map>

namespace
{
QDateTime parseAuditDateTime(const QString& value)
{
    QDateTime parsed = QDateTime::fromString(value.trimmed(), Qt::ISODateWithMs);
    if ( parsed.isValid() )
    {
        return parsed;
    }
    return QDateTime::fromString(value.trimmed(), Qt::ISODate);
}

void setAuditRow(QTableWidget* table, int row, const kelpieui::v1::AuditLogEntry& entry)
{
    if ( table == nullptr || row < 0 )
    {
        return;
    }
    table->setItem(row, 0, new QTableWidgetItem(QString::fromStdString(entry.timestamp())));
    table->setItem(row, 1, new QTableWidgetItem(QString::fromStdString(entry.username())));
    table->setItem(row, 2, new QTableWidgetItem(QString::fromStdString(entry.role())));
    table->setItem(row, 3, new QTableWidgetItem(QString::fromStdString(entry.method())));
    table->setItem(row, 4, new QTableWidgetItem(QString::fromStdString(entry.target())));
    table->setItem(row, 5, new QTableWidgetItem(QString::fromStdString(entry.status())));
    table->setItem(row, 6, new QTableWidgetItem(QString::fromStdString(entry.error())));
}
}

namespace StockmanNamespace::UserInterface
{
	    void KelpiePanel::refreshSupplemental()
	    {
	        auto* ctrl = controller();
	        if ( ctrl == nullptr )
	        {
	            return;
	        }
	        if ( refreshSupplementalButton_ != nullptr ) { refreshSupplementalButton_->setEnabled(false);
}
	        if ( supplementalTable_ != nullptr ) { supplementalTable_->setEnabled(false);
}
	        if ( supplementalQualityTable_ != nullptr ) { supplementalQualityTable_->setEnabled(false);
}
	        const uint64_t epoch = ctrl->ConnectionEpoch();
	        const bool needQuality = supplementalQualityTable_ != nullptr;

	        struct Result {
	            uint64_t epoch{0};
	            bool eventsOk{false};
	            QString eventsErr;
	            std::vector<kelpieui::v1::SupplementalEvent> events;

	            bool haveStatus{false};
	            QString statusErr;
	            kelpieui::v1::SupplementalStatus status;

	            bool haveMetrics{false};
	            QString metricsErr;
	            kelpieui::v1::SupplementalMetrics metrics;

	            bool haveQuality{false};
	            QString qualityErr;
	            std::vector<kelpieui::v1::SupplementalQuality> qualities;
	        };

	        runAsync<Result>(
	            this,
	            [ctrl, epoch, needQuality]() {
	                Result res;
	                res.epoch = epoch;

	                res.haveStatus = ctrl->GetSupplementalStatus(&res.status, res.statusErr);
	                res.haveMetrics = ctrl->GetSupplementalMetrics(&res.metrics, res.metricsErr);

	                QString error;
	                res.eventsOk = ctrl->ListSupplementalEvents(50, &res.events, error);
	                res.eventsErr = error;

	                if ( needQuality )
	                {
	                    QString qErr;
	                    std::vector<kelpieui::v1::SupplementalQuality> qualities;
	                    if ( ctrl->ListSupplementalQuality(50, {}, &qualities, qErr) )
	                    {
	                        res.haveQuality = true;
	                        res.qualities = std::move(qualities);
	                    }
	                    else
	                    {
	                        res.qualityErr = qErr;
	                    }
	                }
	                return res;
	            },
	            [this](const Result& res) {
	                if ( refreshSupplementalButton_ ) { refreshSupplementalButton_->setEnabled(true);
}
	                if ( supplementalTable_ ) { supplementalTable_->setEnabled(true);
}
	                if ( supplementalQualityTable_ ) { supplementalQualityTable_->setEnabled(true);
}

	                auto* ctrl = controller();
	                if ( ctrl == nullptr || ctrl->ConnectionEpoch() != res.epoch )
	                {
	                    return;
	                }

	                if ( supplementalSummary_ )
	                {
	                    QString summary = tr("Supplemental events: %1 (latest seq %2)")
	                                          .arg(res.events.size())
	                                          .arg(res.events.empty() ? 0 : res.events.front().seq());
	                    if ( res.haveStatus )
	                    {
	                        summary += tr(" | enabled=%1 queue=%2 pending=%3 links=%4")
	                                       .arg(res.status.enabled() ? tr("yes") : tr("no"))
	                                       .arg(res.status.queue_length())
	                                       .arg(res.status.pending_actions())
	                                       .arg(res.status.active_links());
	                    }
	                    else if ( !res.statusErr.isEmpty() )
	                    {
	                        summary += tr(" | status=%1").arg(res.statusErr);
	                    }
	                    if ( res.haveMetrics )
	                    {
	                        summary += tr(" | dispatched=%1 ok=%2 fail=%3 drop=%4 recycle=%5 seq=%6")
	                                       .arg(res.metrics.dispatched())
	                                       .arg(res.metrics.success())
	                                       .arg(res.metrics.failures())
	                                       .arg(res.metrics.dropped())
	                                       .arg(res.metrics.recycled())
	                                       .arg(res.metrics.event_seq());
	                    }
	                    else if ( !res.metricsErr.isEmpty() )
	                    {
	                        summary += tr(" | metrics=%1").arg(res.metricsErr);
	                    }
	                    if ( !res.eventsOk && !res.eventsErr.isEmpty() )
	                    {
	                        summary += tr(" | events_error=%1").arg(res.eventsErr);
	                    }
	                    supplementalSummary_->setText(summary);
	                }

		                if ( !res.eventsOk )
		                {
		                    if ( !res.eventsErr.isEmpty() )
		                    {
		                        toastError(tr("Supplemental refresh failed: %1").arg(res.eventsErr));
		                        if ( opsStatusLabel_ ) { opsStatusLabel_->setText(tr("Supplemental error: %1").arg(res.eventsErr));
}
		                    }
	                    if ( supplementalTable_ ) { supplementalTable_->setRowCount(0);
}
	                }
	                else
	                {
	                    if ( supplementalTable_ )
	                    {
	                        supplementalTable_->setRowCount(0);
	                        for (const auto& evt : res.events)
	                        {
	                            appendSupplementalEvent(evt);
	                        }
	                    }
	                    applySupplementalFilter();
	                }

	                if ( supplementalQualityTable_ )
	                {
	                    supplementalQualityTable_->setRowCount(0);
	                    if ( res.haveQuality )
	                    {
	                        supplementalQualityTable_->setRowCount(static_cast<int>(res.qualities.size()));
	                        for (size_t i = 0; i < res.qualities.size(); ++i)
	                        {
	                            const auto& q = res.qualities[i];
	                            const int row = static_cast<int>(i);
	                            supplementalQualityTable_->setItem(row,
	                                                              0,
	                                                              new QTableWidgetItem(QString::fromStdString(q.node_uuid())));
	                            supplementalQualityTable_->setItem(
	                                row, 1, new QTableWidgetItem(QString::number(q.health_score(), 'f', 3)));
	                            supplementalQualityTable_->setItem(
	                                row, 2, new QTableWidgetItem(QString::number(q.latency_score(), 'f', 3)));
	                            supplementalQualityTable_->setItem(
	                                row, 3, new QTableWidgetItem(QString::number(q.failure_score(), 'f', 3)));
	                            supplementalQualityTable_->setItem(
	                                row, 4, new QTableWidgetItem(QString::number(q.queue_score(), 'f', 3)));
	                            supplementalQualityTable_->setItem(
	                                row, 5, new QTableWidgetItem(QString::number(q.staleness_score(), 'f', 3)));
	                            supplementalQualityTable_->setItem(row, 6, new QTableWidgetItem(QString::number(q.total_success())));
	                            supplementalQualityTable_->setItem(row, 7, new QTableWidgetItem(QString::number(q.total_failures())));
	                            supplementalQualityTable_->setItem(
	                                row, 8, new QTableWidgetItem(QString::fromStdString(q.last_heartbeat())));
	                        }
	                    }
		                    else if ( !res.qualityErr.isEmpty() )
		                    {
		                        toastError(tr("Supplemental quality failed: %1").arg(res.qualityErr));
		                    }
		                }
	            });
	    }

    void KelpiePanel::applySupplementalFilter()
    {
        if ( supplementalTable_ == nullptr )
        {
            return;
        }
        const QString filter = (supplementalFilter_ != nullptr) ? supplementalFilter_->text().trimmed() : QString();
        for (int row = 0; row < supplementalTable_->rowCount(); ++row)
        {
            if ( filter.isEmpty() )
            {
                supplementalTable_->setRowHidden(row, false);
                continue;
            }
            const auto colText = [&](int col) -> QString {
                auto* item = supplementalTable_->item(row, col);
                return item ? item->text() : QString();
            };
            const QString kind = colText(1);
            const QString action = colText(2);
            const QString source = colText(3);
            const QString target = colText(4);
            const QString detail = colText(5);
            const bool match = kind.contains(filter, Qt::CaseInsensitive) ||
                               action.contains(filter, Qt::CaseInsensitive) ||
                               source.contains(filter, Qt::CaseInsensitive) ||
                               target.contains(filter, Qt::CaseInsensitive) ||
                               detail.contains(filter, Qt::CaseInsensitive);
            supplementalTable_->setRowHidden(row, !match);
        }
    }

	    void KelpiePanel::refreshRepairs()
	    {
	        auto* ctrl = controller();
	        if ( (ctrl == nullptr) || (repairsTable_ == nullptr) )
	        {
	            return;
	        }
	        if ( refreshRepairsButton_ != nullptr ) { refreshRepairsButton_->setEnabled(false);
}
	        repairsTable_->setEnabled(false);
	        const uint64_t epoch = ctrl->ConnectionEpoch();

	        struct Result {
	            uint64_t epoch{0};
	            bool ok{false};
	            QString error;
	            std::vector<kelpieui::v1::RepairStatus> repairs;
	        };

	        runAsync<Result>(
	            this,
	            [ctrl, epoch]() {
	                Result res;
	                res.epoch = epoch;
	                QString error;
	                std::vector<kelpieui::v1::RepairStatus> repairs;
	                res.ok = ctrl->ListRepairs(&repairs, error);
	                res.error = error;
	                res.repairs = std::move(repairs);
	                return res;
	            },
	            [this](const Result& res) {
	                if ( refreshRepairsButton_ ) { refreshRepairsButton_->setEnabled(true);
}
	                if ( repairsTable_ ) { repairsTable_->setEnabled(true);
}

	                auto* ctrl = controller();
	                if ( ctrl == nullptr || ctrl->ConnectionEpoch() != res.epoch )
	                {
	                    return;
	                }
		                if ( !res.ok )
		                {
		                    toastError(tr("Repairs refresh failed: %1").arg(res.error));
		                    if ( repairsTable_ ) { repairsTable_->setRowCount(0);
}
		                    return;
	                }

	                repairsTable_->setRowCount(static_cast<int>(res.repairs.size()));
	                for (size_t i = 0; i < res.repairs.size(); ++i)
	                {
	                    const auto& r = res.repairs[i];
	                    const int row = static_cast<int>(i);
	                    repairsTable_->setItem(row, 0, new QTableWidgetItem(QString::fromStdString(r.target_uuid())));
	                    repairsTable_->setItem(row, 1, new QTableWidgetItem(QString::number(r.attempts())));
	                    repairsTable_->setItem(row, 2, new QTableWidgetItem(QString::fromStdString(r.next_attempt())));
	                    repairsTable_->setItem(row, 3, new QTableWidgetItem(r.broken() ? tr("yes") : tr("no")));
	                    repairsTable_->setItem(row, 4, new QTableWidgetItem(QString::fromStdString(r.last_error())));
	                    repairsTable_->setItem(row, 5, new QTableWidgetItem(QString::fromStdString(r.reason())));
	                }
	            });
	    }

	    void KelpiePanel::refreshAudit()
	    {
	        auto* ctrl = controller();
	        if ( (ctrl == nullptr) || (auditTable_ == nullptr) )
	        {
	            return;
	        }
	        if ( refreshAuditButton_ != nullptr ) { refreshAuditButton_->setEnabled(false);
}
	        auditTable_->setEnabled(false);
	        const uint64_t epoch = ctrl->ConnectionEpoch();
	        const QString user = (auditUserFilter_ != nullptr) ? auditUserFilter_->text().trimmed() : QString();
	        const QString method = (auditMethodFilter_ != nullptr) ? auditMethodFilter_->text().trimmed() : QString();
	        const QString fromStr = (auditFromInput_ != nullptr) ? auditFromInput_->text().trimmed() : QString();
	        const QString toStr = (auditToInput_ != nullptr) ? auditToInput_->text().trimmed() : QString();
	        const int limit = (auditLimitSpin_ != nullptr) ? auditLimitSpin_->value() : 100;

	        const QDateTime from = parseAuditDateTime(fromStr);
	        const QDateTime to = parseAuditDateTime(toStr);

	        struct Result {
	            uint64_t epoch{0};
	            bool ok{false};
	            QString error;
	            std::vector<kelpieui::v1::AuditLogEntry> entries;
	        };

	        runAsync<Result>(
	            this,
	            [ctrl, epoch, user, method, from, to, limit]() {
	                Result res;
	                res.epoch = epoch;
	                QString error;
	                std::vector<kelpieui::v1::AuditLogEntry> entries;
	                res.ok = ctrl->ListAuditLogs(user, method, from, to, limit, &entries, error);
	                res.error = error;
	                res.entries = std::move(entries);
	                return res;
	            },
	            [this](const Result& res) {
	                if ( refreshAuditButton_ ) { refreshAuditButton_->setEnabled(true);
}
	                if ( auditTable_ ) { auditTable_->setEnabled(true);
}

	                auto* ctrl = controller();
	                if ( ctrl == nullptr || ctrl->ConnectionEpoch() != res.epoch )
	                {
	                    return;
	                }
		                if ( !res.ok )
		                {
		                    toastError(tr("Audit refresh failed: %1").arg(res.error));
		                    if ( opsStatusLabel_ ) { opsStatusLabel_->setText(tr("Audit error: %1").arg(res.error));
}
		                    if ( auditTable_ ) { auditTable_->setRowCount(0);
}
	                    return;
	                }

	                auditTable_->setRowCount(static_cast<int>(res.entries.size()));
	                for (size_t i = 0; i < res.entries.size(); ++i)
	                {
	                    setAuditRow(auditTable_, static_cast<int>(i), res.entries[i]);
	                }
	            });
	    }

	    void KelpiePanel::appendAuditEntry(const kelpieui::v1::AuditLogEntry& entry)
	    {
	        if ( auditTable_ == nullptr )
	        {
	            return;
	        }

	        const QString userFilter = (auditUserFilter_ != nullptr) ? auditUserFilter_->text().trimmed() : QString();
	        if ( !userFilter.isEmpty() &&
	             !QString::fromStdString(entry.username()).contains(userFilter, Qt::CaseInsensitive) )
	        {
	            return;
	        }

	        const QString methodFilter = (auditMethodFilter_ != nullptr) ? auditMethodFilter_->text().trimmed() : QString();
	        if ( !methodFilter.isEmpty() &&
	             !QString::fromStdString(entry.method()).contains(methodFilter, Qt::CaseInsensitive) )
	        {
	            return;
	        }

	        const QDateTime from = parseAuditDateTime((auditFromInput_ != nullptr) ? auditFromInput_->text() : QString());
	        const QDateTime to = parseAuditDateTime((auditToInput_ != nullptr) ? auditToInput_->text() : QString());
	        const QDateTime timestamp = parseAuditDateTime(QString::fromStdString(entry.timestamp()));
	        if ( timestamp.isValid() && from.isValid() && timestamp < from )
	        {
	            return;
	        }
	        if ( timestamp.isValid() && to.isValid() && timestamp > to )
	        {
	            return;
	        }

	        auditTable_->insertRow(0);
	        setAuditRow(auditTable_, 0, entry);

	        const int limit = (auditLimitSpin_ != nullptr) ? auditLimitSpin_->value() : 100;
	        while ( limit > 0 && auditTable_->rowCount() > limit )
	        {
	            auditTable_->removeRow(auditTable_->rowCount() - 1);
	        }
	    }

	    void KelpiePanel::refreshMetrics()
	    {
	        auto* ctrl = controller();
	        if ( (ctrl == nullptr) || (metricsView_ == nullptr) )
	        {
	            return;
	        }
	        metricsView_->setPlainText(tr("Loading metrics..."));
	        metricsView_->setEnabled(false);
	        const uint64_t epoch = ctrl->ConnectionEpoch();
	        const QString targetUuid = currentNodeUuid_;

	        struct Result {
	            uint64_t epoch{0};

	            bool metricsOk{false};
	            QString metricsErr;
	            kelpieui::v1::GetMetricsResponse metrics;

	            bool routingOk{false};
	            QString routingErr;
	            kelpieui::v1::RoutingStrategy routingStrategy{kelpieui::v1::ROUTING_STRATEGY_UNSPECIFIED};

	            bool streamStatsOk{false};
	            QString streamStatsErr;
	            std::vector<kelpieui::v1::StreamStatInfo> streamStats;
	        };

	        runAsync<Result>(
	            this,
	            [ctrl, epoch]() {
	                Result res;
	                res.epoch = epoch;

	                QString error;
	                kelpieui::v1::GetMetricsResponse metrics;
	                res.metricsOk = ctrl->GetMetrics(&metrics, error);
	                res.metricsErr = error;
	                res.metrics = metrics;

	                QString routingErr;
	                kelpieui::v1::RoutingStrategy routingStrategy = kelpieui::v1::ROUTING_STRATEGY_UNSPECIFIED;
	                res.routingOk = ctrl->GetRoutingStrategy(&routingStrategy, routingErr);
	                res.routingErr = routingErr;
	                res.routingStrategy = routingStrategy;

	                QString statsErr;
	                std::vector<kelpieui::v1::StreamStatInfo> stats;
	                res.streamStatsOk = ctrl->StreamStats(&stats, statsErr);
	                res.streamStatsErr = statsErr;
	                res.streamStats = std::move(stats);

	                return res;
	            },
	            [this, targetUuid](const Result& res) {
	                if ( metricsView_ ) { metricsView_->setEnabled(true);
}

	                auto* ctrl = controller();
	                if ( ctrl == nullptr || ctrl->ConnectionEpoch() != res.epoch )
	                {
	                    return;
	                }

	                if ( !res.metricsOk )
	                {
	                    metricsView_->setPlainText(tr("Metrics unavailable: %1").arg(res.metricsErr));
	                    return;
	                }

	                if ( res.routingOk && routingStrategyBox_ )
	                {
	                    int idx = routingStrategyBox_->findData(res.routingStrategy);
	                    if ( idx >= 0 )
	                    {
	                        routingStrategyBox_->setCurrentIndex(idx);
	                    }
	                }

	                QString text;
	                text += tr("Router metrics:\n");
	                for (const auto& m : res.metrics.router_metrics())
	                {
	                    text += tr("  type %1: dispatched=%2 errors=%3 drops=%4\n")
	                                .arg(m.message_type())
	                                .arg(m.dispatched())
	                                .arg(m.errors())
	                                .arg(m.drops());
	                }
	                if ( res.routingOk )
	                {
	                    QString strategyLabel = tr("unknown");
	                    switch (res.routingStrategy)
	                    {
	                    case kelpieui::v1::ROUTING_STRATEGY_HOPS:
	                        strategyLabel = tr("Hops (BFS)");
	                        break;
	                    case kelpieui::v1::ROUTING_STRATEGY_WEIGHT:
	                        strategyLabel = tr("Weight (Dijkstra)");
	                        break;
	                    case kelpieui::v1::ROUTING_STRATEGY_LATENCY:
	                        strategyLabel = tr("Latency (ETTD)");
	                        break;
	                    default:
	                        break;
	                    }
	                    text += tr("\nRouting strategy: %1\n").arg(strategyLabel);
	                }
	                else
	                {
	                    text += tr("\nRouting strategy: unavailable (%1)\n").arg(res.routingErr);
	                }
	                text += tr("\nReconnect: attempts=%1 success=%2 failures=%3 last_error=%4\n")
	                            .arg(res.metrics.reconnect_metrics().attempts())
	                            .arg(res.metrics.reconnect_metrics().success())
	                            .arg(res.metrics.reconnect_metrics().failures())
	                            .arg(QString::fromStdString(res.metrics.reconnect_metrics().last_error()));
	                if ( res.metrics.has_dtn_metrics() )
	                {
	                    const auto& dtn = res.metrics.dtn_metrics();
	                    text += tr("\nDTN: enqueued=%1 delivered=%2 failed=%3 retried=%4 at %5\n")
	                                .arg(dtn.enqueued())
	                                .arg(dtn.delivered())
	                                .arg(dtn.failed())
	                                .arg(dtn.retried())
	                                .arg(QString::fromStdString(dtn.captured_at()));
	                    if ( dtn.has_global_queue() )
	                    {
	                        const auto& q = dtn.global_queue();
	                        text += tr("  Queue: total=%1 ready=%2 held=%3 drop=%4 expired=%5 oldest=%6\n")
	                                    .arg(q.total())
	                                    .arg(q.ready())
	                                    .arg(q.held())
	                                    .arg(q.dropped_total())
	                                    .arg(q.expired_total())
	                                    .arg(QString::fromStdString(q.oldest_bundle_id()));
	                    }
	                }

	                text += "\n[StreamStats]\n";
	                if ( res.streamStatsOk )
	                {
	                    for (const auto& st : res.streamStats)
	                    {
	                        text += tr("kind %1 opened=%2 closed=%3 active=%4 last=%5 (%6)\n")
	                                    .arg(QString::fromStdString(st.kind()))
	                                    .arg(st.opened())
	                                    .arg(st.closed())
	                                    .arg(st.active())
	                                    .arg(QString::fromStdString(st.last_reason()))
	                                    .arg(QString::fromStdString(st.last_closed()));
	                    }
	                }
	                else if ( !res.streamStatsErr.isEmpty() )
	                {
	                    text += tr("unavailable: %1\n").arg(res.streamStatsErr);
	                }

	                metricsView_->setPlainText(text);

	                if ( diagnosticsTable_ )
	                {
	                    diagnosticsTable_->setRowCount(0);
	                    if ( !targetUuid.isEmpty() && targetUuid == currentNodeUuid_ )
	                    {
	                        refreshDiagnosticsForNode(targetUuid);
	                    }
	                }
	                refreshStreamDiagnostics();
	            });
	    }

    void KelpiePanel::applyRoutingStrategy()
    {
        auto* ctrl = controller();
        if ( (ctrl == nullptr) || (routingStrategyBox_ == nullptr) )
        {
            return;
        }
        auto value = routingStrategyBox_->currentData();
        if ( !value.isValid() )
        {
            toastWarn(tr("Select a routing strategy first"));
            return;
        }
        const auto strategy = static_cast<kelpieui::v1::RoutingStrategy>(value.toInt());
        setWidgetsEnabled({applyRoutingButton_, routingStrategyBox_}, false);
        toastInfo(tr("Updating routing strategy..."));

        const uint64_t epoch = ctrl->ConnectionEpoch();
        struct Result {
            uint64_t epoch{0};
            bool ok{false};
            QString error;
        };
        runAsync<Result>(
            this,
            [ctrl, epoch, strategy]() {
                Result res;
                res.epoch = epoch;
                QString error;
                res.ok = ctrl->SetRoutingStrategy(strategy, error);
                res.error = error;
                return res;
            },
            [this](const Result& res) {
                setWidgetsEnabled({applyRoutingButton_, routingStrategyBox_}, true);
                auto* ctrl = controller();
                if ( ctrl == nullptr || ctrl->ConnectionEpoch() != res.epoch )
                {
                    return;
                }
                if ( !res.ok )
                {
                    toastError(tr("Set routing strategy failed: %1").arg(res.error));
                    return;
                }
                toastInfo(tr("Routing strategy updated"));
                refreshMetrics();
            });
    }

    static void appendDiagnosticRow(QTableWidget* table,
                                    const QString& target,
                                    const kelpieui::v1::SessionIssue& issue,
                                    const kelpieui::v1::SessionMetric* metric,
                                    const kelpieui::v1::SessionProcess* proc)
    {
        int row = table->rowCount();
        table->insertRow(row);
        table->setItem(row, 0, new QTableWidgetItem(target));
        table->setItem(row, 1, new QTableWidgetItem(QString::fromStdString(issue.code())));
        table->setItem(row, 2, new QTableWidgetItem(QString::fromStdString(issue.message())));
        if ( metric != nullptr )
        {
            table->setItem(row, 3, new QTableWidgetItem(QString::fromStdString(metric->name())));
            table->setItem(row, 4, new QTableWidgetItem(QString::fromStdString(metric->value())));
        }
        if ( proc != nullptr )
        {
            table->setItem(row, 5, new QTableWidgetItem(QString::fromStdString(proc->name())));
        }
    }

	    void KelpiePanel::refreshDiagnosticsForNode(const QString& targetUuid)
	    {
	        auto* ctrl = controller();
	        if ( (ctrl == nullptr) || (diagnosticsTable_ == nullptr) )
	        {
	            return;
	        }
	        const QString normalized = targetUuid.trimmed();
	        if ( normalized.isEmpty() )
	        {
	            return;
	        }

	        diagnosticsTable_->setEnabled(false);
	        diagnosticsTable_->setRowCount(0);
	        const uint64_t epoch = ctrl->ConnectionEpoch();

	        struct Result {
	            uint64_t epoch{0};
	            QString targetUuid;
	            bool ok{false};
	            QString error;
	            kelpieui::v1::SessionDiagnosticsResponse resp;
	        };

	        runAsync<Result>(
	            this,
	            [ctrl, epoch, normalized]() {
	                Result res;
	                res.epoch = epoch;
	                res.targetUuid = normalized;
	                QString error;
	                kelpieui::v1::SessionDiagnosticsResponse resp;
	                res.ok = ctrl->GetSessionDiagnostics(normalized, true, true, &resp, error);
	                res.error = error;
	                res.resp = resp;
	                return res;
	            },
	            [this](const Result& res) {
	                if ( diagnosticsTable_ ) { diagnosticsTable_->setEnabled(true);
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
		                    toastError(tr("Diagnostics failed for %1: %2").arg(res.targetUuid, res.error));
		                    return;
		                }

	                for (const auto& issue : res.resp.issues())
	                {
	                    appendDiagnosticRow(diagnosticsTable_, res.targetUuid, issue, nullptr, nullptr);
	                }
	                for (const auto& metric : res.resp.metrics())
	                {
	                    appendDiagnosticRow(diagnosticsTable_, res.targetUuid, kelpieui::v1::SessionIssue(), &metric, nullptr);
	                }
	                for (const auto& proc : res.resp.processes())
	                {
	                    appendDiagnosticRow(diagnosticsTable_, res.targetUuid, kelpieui::v1::SessionIssue(), nullptr, &proc);
	                }
	            });
	    }

	    void KelpiePanel::refreshStreamDiagnostics()
	    {
	        if ( streamDiagTable_ == nullptr )
	        {
	            return;
	        }
	        auto* ctrl = controller();
	        if ( ctrl == nullptr )
	        {
	            return;
	        }
	        if ( streamRefreshDebounce_ != nullptr && streamRefreshDebounce_->isActive() )
	        {
	            streamRefreshDebounce_->stop();
	        }
	        if ( streamDiagnosticsRefreshInFlight_ )
	        {
	            streamDiagnosticsRefreshPending_ = true;
	            return;
	        }
	        streamDiagnosticsRefreshInFlight_ = true;
	        streamDiagTable_->setEnabled(false);
	        streamDiagTable_->setRowCount(0);
	        const uint64_t epoch = ctrl->ConnectionEpoch();

	        struct Result {
	            uint64_t epoch{0};
	            bool ok{false};
	            QString error;
	            std::vector<kelpieui::v1::StreamDiag> streams;
	        };

	        runAsync<Result>(
	            this,
	            [ctrl, epoch]() {
	                Result res;
	                res.epoch = epoch;
	                QString error;
	                std::vector<kelpieui::v1::StreamDiag> streams;
	                res.ok = ctrl->StreamDiagnostics(&streams, error);
	                res.error = error;
	                res.streams = std::move(streams);
	                return res;
	            },
	            [this](const Result& res) {
	                auto finishRefresh = [this]() {
	                    streamDiagnosticsRefreshInFlight_ = false;
	                    if ( streamDiagnosticsRefreshPending_ )
	                    {
	                        streamDiagnosticsRefreshPending_ = false;
	                        if ( streamRefreshDebounce_ != nullptr )
	                        {
	                            streamRefreshDebounce_->start();
	                        }
	                        else
	                        {
	                            refreshStreamDiagnostics();
	                        }
	                    }
	                };

	                if ( streamDiagTable_ ) { streamDiagTable_->setEnabled(true);
}

	                auto* ctrl = controller();
	                if ( ctrl == nullptr || ctrl->ConnectionEpoch() != res.epoch )
	                {
	                    finishRefresh();
	                    return;
	                }
		                if ( !res.ok )
		                {
		                    toastError(tr("Stream diagnostics failed: %1").arg(res.error));
		                    finishRefresh();
		                    return;
		                }

	                streamDiagTable_->setRowCount(static_cast<int>(res.streams.size()));
	                for (size_t i = 0; i < res.streams.size(); ++i)
	                {
	                    const auto& s = res.streams[i];
	                    streamDiagTable_->setItem(static_cast<int>(i), 0, new QTableWidgetItem(QString::number(s.stream_id())));
	                    streamDiagTable_->setItem(static_cast<int>(i), 1, new QTableWidgetItem(QString::fromStdString(s.target_uuid())));
	                    streamDiagTable_->setItem(static_cast<int>(i), 2, new QTableWidgetItem(QString::fromStdString(s.kind())));
	                    streamDiagTable_->setItem(static_cast<int>(i), 3, new QTableWidgetItem(QString::number(s.pending())));
	                    streamDiagTable_->setItem(static_cast<int>(i), 4, new QTableWidgetItem(QString::number(s.inflight())));
	                    streamDiagTable_->setItem(static_cast<int>(i),
	                                              5,
	                                              new QTableWidgetItem(tr("%1 / %2")
	                                                                       .arg(QString::fromStdString(s.rto()),
	                                                                            QString::fromStdString(s.last_activity()))));
	                }
	                finishRefresh();
	            });
	    }

	    void KelpiePanel::refreshDtn()
	    {
	        auto* ctrl = controller();
	        if ( ctrl == nullptr )
	        {
	            return;
	        }
	        if ( (dtnStatsLabel_ == nullptr) || (dtnBundleTable_ == nullptr) )
	        {
	            return;
	        }

	        const QString targetUuid = currentNodeUuid_.trimmed();
	        if ( targetUuid.isEmpty() )
	        {
	            dtnStatsLabel_->setText(tr("DTN stats: select a node"));
	            dtnBundleTable_->setRowCount(0);
	            if ( diagnosticsTable_ != nullptr ) { diagnosticsTable_->setRowCount(0);
}
	            refreshDtnPolicy();
	            return;
	        }

	        dtnBundleTable_->setEnabled(false);
	        dtnStatsLabel_->setText(tr("DTN stats: loading..."));
	        const int limit = (dtnLimitSpin_ != nullptr) ? dtnLimitSpin_->value() : 50;
	        const uint64_t epoch = ctrl->ConnectionEpoch();

	        struct Result {
	            uint64_t epoch{0};
	            QString targetUuid;

	            bool statsOk{false};
	            QString statsErr;
	            kelpieui::v1::DtnQueueStats stats;

	            bool bundlesOk{false};
	            QString bundlesErr;
	            std::vector<kelpieui::v1::DtnBundle> bundles;
	        };

	        runAsync<Result>(
	            this,
	            [ctrl, epoch, targetUuid, limit]() {
	                Result res;
	                res.epoch = epoch;
	                res.targetUuid = targetUuid;

	                QString error;
	                kelpieui::v1::DtnQueueStats stats;
	                res.statsOk = ctrl->GetDtnQueueStats(targetUuid, &stats, error);
	                res.statsErr = error;
	                res.stats = stats;

	                error.clear();
	                std::vector<kelpieui::v1::DtnBundle> bundles;
	                res.bundlesOk = ctrl->ListDtnBundles(targetUuid, limit, &bundles, error);
	                res.bundlesErr = error;
	                res.bundles = std::move(bundles);

	                return res;
	            },
	            [this](const Result& res) {
	                if ( dtnBundleTable_ ) { dtnBundleTable_->setEnabled(true);
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

	                if ( !res.statsOk )
	                {
	                    dtnStatsLabel_->setText(tr("DTN stats error: %1").arg(res.statsErr));
	                }
	                else
	                {
	                    dtnStatsLabel_->setText(tr("DTN queue total=%1 ready=%2 held=%3 drop=%4 expired=%5 oldest=%6")
	                                                .arg(res.stats.total())
	                                                .arg(res.stats.ready())
	                                                .arg(res.stats.held())
	                                                .arg(res.stats.dropped_total())
	                                                .arg(res.stats.expired_total())
	                                                .arg(QString::fromStdString(res.stats.oldest_bundle_id())));
	                }

	                if ( res.bundlesOk )
	                {
	                    dtnBundleTable_->setRowCount(static_cast<int>(res.bundles.size()));
	                    for (size_t i = 0; i < res.bundles.size(); ++i)
	                    {
	                        const auto& b = res.bundles[i];
	                        dtnBundleTable_->setItem(static_cast<int>(i),
	                                                 0,
	                                                 new QTableWidgetItem(QString::fromStdString(b.bundle_id())));
	                        dtnBundleTable_->setItem(static_cast<int>(i),
	                                                 1,
	                                                 new QTableWidgetItem(QString::fromStdString(b.target_uuid())));
	                        dtnBundleTable_->setItem(static_cast<int>(i), 2, new QTableWidgetItem(QString::number(b.priority())));
	                        dtnBundleTable_->setItem(static_cast<int>(i), 3, new QTableWidgetItem(QString::number(b.attempts())));
	                        dtnBundleTable_->setItem(static_cast<int>(i), 4, new QTableWidgetItem(QString::fromStdString(b.age())));
	                        dtnBundleTable_->setItem(static_cast<int>(i),
	                                                 5,
	                                                 new QTableWidgetItem(QString::fromStdString(b.deliver_by())));
	                        dtnBundleTable_->setItem(static_cast<int>(i),
	                                                 6,
	                                                 new QTableWidgetItem(QString::fromStdString(b.preview())));
	                    }
	                }
	                else
	                {
	                    dtnBundleTable_->setRowCount(0);
		                    if ( !res.bundlesErr.isEmpty() )
		                    {
		                        toastError(tr("DTN bundles error: %1").arg(res.bundlesErr));
		                    }
		                }

	                if ( diagnosticsTable_ )
	                {
	                    diagnosticsTable_->setRowCount(0);
	                    refreshDiagnosticsForNode(res.targetUuid);
	                    refreshStreamDiagnostics();
	                }
	                refreshDtnPolicy();
	            });
	    }

	    void KelpiePanel::refreshDtnPolicy()
	    {
	        auto* ctrl = controller();
	        if ( (ctrl == nullptr) || (dtnPolicyTable_ == nullptr) )
	        {
	            return;
	        }
	        dtnPolicyTable_->setEnabled(false);
	        const uint64_t epoch = ctrl->ConnectionEpoch();

	        struct Result {
	            uint64_t epoch{0};
	            bool ok{false};
	            QString error;
	            std::map<std::string, std::string> entries;
	        };

	        runAsync<Result>(
	            this,
	            [ctrl, epoch]() {
	                Result res;
	                res.epoch = epoch;
	                QString error;
	                std::map<std::string, std::string> entries;
	                res.ok = ctrl->GetDtnPolicy(&entries, error);
	                res.error = error;
	                res.entries = std::move(entries);
	                return res;
	            },
	            [this](const Result& res) {
	                if ( dtnPolicyTable_ ) { dtnPolicyTable_->setEnabled(true);
}

	                auto* ctrl = controller();
	                if ( ctrl == nullptr || ctrl->ConnectionEpoch() != res.epoch )
	                {
	                    return;
	                }
		                if ( !res.ok )
		                {
		                    toastError(tr("DTN policy error: %1").arg(res.error));
		                    if ( dtnPolicyTable_ ) { dtnPolicyTable_->setRowCount(0);
}
		                    return;
	                }

	                dtnPolicyTable_->setRowCount(static_cast<int>(res.entries.size()));
	                int row = 0;
	                for (const auto& [key, value] : res.entries)
	                {
	                    dtnPolicyTable_->setItem(row, 0, new QTableWidgetItem(QString::fromStdString(key)));
	                    dtnPolicyTable_->setItem(row, 1, new QTableWidgetItem(QString::fromStdString(value)));
	                    ++row;
	                }
	                if ( dtnPolicyKeyInput_ && dtnPolicyKeyInput_->text().trimmed().isEmpty() )
	                {
	                    dtnPolicyKeyInput_->setText(QStringLiteral("max_inflight_per_target"));
	                }
	            });
	    }

    void KelpiePanel::applyDtnPolicy()
    {
        auto* ctrl = controller();
        if ( ctrl == nullptr )
        {
            return;
        }
        const QString key = (dtnPolicyKeyInput_ != nullptr) ? dtnPolicyKeyInput_->text().trimmed() : QString();
        const QString value = (dtnPolicyValueInput_ != nullptr) ? dtnPolicyValueInput_->text().trimmed() : QString();
        if ( key.isEmpty() || value.isEmpty() )
        {
            toastWarn(tr("DTN policy key/value required"));
            return;
        }
        setWidgetsEnabled({applyDtnPolicyButton_, dtnPolicyKeyInput_, dtnPolicyValueInput_, dtnPolicyTable_}, false);
        toastInfo(tr("Updating DTN policy..."));
        const uint64_t epoch = ctrl->ConnectionEpoch();
        struct Result {
            uint64_t epoch{0};
            QString key;
            QString value;
            bool ok{false};
            QString error;
            std::map<std::string, std::string> entries;
        };
        runAsync<Result>(
            this,
            [ctrl, epoch, key, value]() {
                Result res;
                res.epoch = epoch;
                res.key = key;
                res.value = value;
                QString error;
                std::map<std::string, std::string> entries;
                res.ok = ctrl->UpdateDtnPolicy(key, value, &entries, error);
                res.error = error;
                res.entries = std::move(entries);
                return res;
            },
            [this](const Result& res) {
                setWidgetsEnabled({applyDtnPolicyButton_, dtnPolicyKeyInput_, dtnPolicyValueInput_, dtnPolicyTable_}, true);
                auto* ctrl = controller();
                if ( ctrl == nullptr || ctrl->ConnectionEpoch() != res.epoch )
                {
                    return;
                }
                if ( !res.ok )
                {
                    toastError(tr("Update DTN policy failed: %1").arg(res.error));
                    return;
                }
                toastInfo(tr("DTN policy updated: %1=%2").arg(res.key, res.value));
                if ( dtnPolicyTable_ )
                {
                    dtnPolicyTable_->setRowCount(static_cast<int>(res.entries.size()));
                    int row = 0;
                    for (const auto& [k, v] : res.entries)
                    {
                        dtnPolicyTable_->setItem(row, 0, new QTableWidgetItem(QString::fromStdString(k)));
                        dtnPolicyTable_->setItem(row, 1, new QTableWidgetItem(QString::fromStdString(v)));
                        ++row;
                    }
                }
            });
    }

    void KelpiePanel::enqueueDtn()
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
        const QString payload = dtnPayloadInput_->text();
        if ( payload.isEmpty() )
        {
            toastWarn(tr("Payload is required"));
            return;
        }
        bool okTtl = false;
        const long long ttl = dtnTtlInput_->text().toLongLong(&okTtl);
        auto priority = static_cast<kelpieui::v1::DtnPriority>(dtnPriorityBox_->currentData().toInt());
        const QString targetUuid = currentNodeUuid_;
        const int64_t ttlSeconds = okTtl ? ttl : 0;
        setWidgetsEnabled({enqueueDtnButton_, dtnPayloadInput_, dtnTtlInput_, dtnPriorityBox_}, false);
        toastInfo(tr("Enqueueing DTN payload..."));

        const uint64_t epoch = ctrl->ConnectionEpoch();
        struct Result {
            uint64_t epoch{0};
            QString bundleId;
            bool ok{false};
            QString error;
        };
        runAsync<Result>(
            this,
            [ctrl, epoch, targetUuid, payload, priority, ttlSeconds]() {
                Result res;
                res.epoch = epoch;
                QString error;
                QString bundleId;
                res.ok = ctrl->EnqueueDtnPayload(targetUuid, payload, priority, ttlSeconds, &bundleId, error);
                res.error = error;
                res.bundleId = bundleId;
                return res;
            },
            [this](const Result& res) {
                setWidgetsEnabled({enqueueDtnButton_, dtnPayloadInput_, dtnTtlInput_, dtnPriorityBox_}, true);
                auto* ctrl = controller();
                if ( ctrl == nullptr || ctrl->ConnectionEpoch() != res.epoch )
                {
                    return;
                }
                if ( !res.ok )
                {
                    toastError(tr("Enqueue DTN failed: %1").arg(res.error));
                    return;
                }
                toastInfo(tr("Enqueued bundle %1").arg(res.bundleId));
                if ( dtnPayloadInput_ ) { dtnPayloadInput_->clear();
}
                if ( dtnTtlInput_ ) { dtnTtlInput_->clear();
}
                refreshDtn();
            });
    }

    void KelpiePanel::appendSupplementalEvent(const kelpieui::v1::SupplementalEvent& event)
    {
        supplementalTable_->insertRow(0);
        supplementalTable_->setItem(0, 0, new QTableWidgetItem(QString::number(event.seq())));
        supplementalTable_->setItem(0, 1, new QTableWidgetItem(QString::fromStdString(event.kind())));
        supplementalTable_->setItem(0, 2, new QTableWidgetItem(QString::fromStdString(event.action())));
        supplementalTable_->setItem(0, 3, new QTableWidgetItem(QString::fromStdString(event.source_uuid())));
        supplementalTable_->setItem(0, 4, new QTableWidgetItem(QString::fromStdString(event.target_uuid())));
        supplementalTable_->setItem(0, 5, new QTableWidgetItem(QString::fromStdString(event.detail())));
        if ( supplementalTable_->rowCount() > 200 )
        {
            supplementalTable_->removeRow(supplementalTable_->rowCount() - 1);
        }
    }
}
