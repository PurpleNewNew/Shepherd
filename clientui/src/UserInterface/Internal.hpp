#ifndef STOCKMAN_KELPIE_PANEL_INTERNAL_HPP
#define STOCKMAN_KELPIE_PANEL_INTERNAL_HPP

#include <QFutureWatcher>
#include <QObject>
#include <QString>
#include <QWidget>
#include <QtConcurrent/QtConcurrent>

#include <algorithm>
#include <initializer_list>
#include <utility>

#include "proto/kelpieui/v1/kelpieui.pb.h"

namespace
{
inline QString lootCategoryText(kelpieui::v1::LootCategory category)
{
    switch ( category )
    {
        case kelpieui::v1::LOOT_CATEGORY_FILE:
            return QObject::tr("File");
        case kelpieui::v1::LOOT_CATEGORY_SCREENSHOT:
            return QObject::tr("Screenshot");
        case kelpieui::v1::LOOT_CATEGORY_TICKET:
            return QObject::tr("Ticket");
        default:
            return QObject::tr("Other");
    }
}

inline QString sessionStatusText(const kelpieui::v1::SessionInfo& session)
{
    switch (session.status())
    {
    case kelpieui::v1::SESSION_STATUS_ACTIVE:
        return QObject::tr("Active");
    case kelpieui::v1::SESSION_STATUS_DEGRADED:
        return QObject::tr("Degraded");
    case kelpieui::v1::SESSION_STATUS_FAILED:
        return QObject::tr("Failed");
    case kelpieui::v1::SESSION_STATUS_MARKED_DEAD:
        return QObject::tr("Marked");
    case kelpieui::v1::SESSION_STATUS_REPAIRING:
        return QObject::tr("Repairing");
    default:
        return session.connected() ? QObject::tr("Connected") : QObject::tr("Offline");
    }
}

template <typename Result, typename WorkFn, typename DoneFn>
// NOLINTBEGIN(clang-analyzer-cplusplus.NewDeleteLeaks): Qt parent ownership is not modeled by analyzer.
inline void runAsync(QObject* owner, WorkFn&& work, DoneFn&& done)
{
    if ( owner == nullptr )
    {
        Result res = std::forward<WorkFn>(work)();
        done(std::move(res));
        return;
    }

    auto* watcher = new QFutureWatcher<Result>(owner);
    QObject::connect(watcher, &QFutureWatcher<Result>::finished, owner, [watcher, done = std::forward<DoneFn>(done)]() mutable {
        Result res = watcher->result();
        watcher->deleteLater();
        done(std::move(res));
    });
    watcher->setFuture(QtConcurrent::run(std::forward<WorkFn>(work)));
}
// NOLINTEND(clang-analyzer-cplusplus.NewDeleteLeaks)

template <typename Result, typename WorkFn, typename RestoreFn, typename ValidFn, typename FailureFn, typename SuccessFn>
inline void runAsyncGuarded(QObject* owner,
                            WorkFn&& work,
                            RestoreFn&& restoreUi,
                            ValidFn&& isStillValid,
                            FailureFn&& onFailure,
                            SuccessFn&& onSuccess)
{
    runAsync<Result>(
        owner,
        std::forward<WorkFn>(work),
        [restoreUi = std::forward<RestoreFn>(restoreUi),
         isStillValid = std::forward<ValidFn>(isStillValid),
         onFailure = std::forward<FailureFn>(onFailure),
         onSuccess = std::forward<SuccessFn>(onSuccess)](Result res) mutable {
            restoreUi();
            if ( !isStillValid(res) )
            {
                return;
            }
            if ( !res.ok )
            {
                onFailure(res);
                return;
            }
            onSuccess(res);
        });
}

template <typename HandlePtr>
inline void closeHandleAsync(HandlePtr handle)
{
    if ( !handle )
    {
        return;
    }
    [[maybe_unused]] auto closeFuture = QtConcurrent::run([handle = std::move(handle)]() mutable {
        handle->Close();
    });
}

inline void setWidgetsEnabled(const std::initializer_list<QWidget*>& widgets, bool enabled)
{
    for ( auto* w : widgets )
    {
        if ( w != nullptr )
        {
            w->setEnabled(enabled);
        }
    }
}

inline QString controllerListenerStatusText(kelpieui::v1::ControllerListenerStatus status)
{
    switch ( status )
    {
        case kelpieui::v1::CONTROLLER_LISTENER_STATUS_PENDING:
            return QObject::tr("Pending");
        case kelpieui::v1::CONTROLLER_LISTENER_STATUS_RUNNING:
            return QObject::tr("Running");
        case kelpieui::v1::CONTROLLER_LISTENER_STATUS_FAILED:
            return QObject::tr("Failed");
        case kelpieui::v1::CONTROLLER_LISTENER_STATUS_STOPPED:
            return QObject::tr("Stopped");
        default:
            return QObject::tr("Unknown");
    }
}

inline QString pivotListenerModeText(kelpieui::v1::PivotListenerMode mode)
{
    switch ( mode )
    {
        case kelpieui::v1::PIVOT_LISTENER_MODE_NORMAL:
            return QObject::tr("Normal");
        case kelpieui::v1::PIVOT_LISTENER_MODE_IPTABLES:
            return QObject::tr("Iptables");
        case kelpieui::v1::PIVOT_LISTENER_MODE_SOREUSE:
            return QObject::tr("SoReuse");
        default:
            return QObject::tr("Unspecified");
    }
}

inline QString joinTags(const google::protobuf::RepeatedPtrField<std::string>& tags)
{
    QStringList values;
    values.reserve(tags.size());
    for ( const auto& tag : tags )
    {
        values.push_back(QString::fromStdString(tag));
    }
    return values.join(QStringLiteral(","));
}

inline kelpieui::v1::PivotListenerMode parsePivotMode(const QString& text)
{
    const QString normalized = text.trimmed().toLower();
    if ( normalized == QStringLiteral("iptables") )
    {
        return kelpieui::v1::PIVOT_LISTENER_MODE_IPTABLES;
    }
    if ( normalized == QStringLiteral("soreuse") )
    {
        return kelpieui::v1::PIVOT_LISTENER_MODE_SOREUSE;
    }
    return kelpieui::v1::PIVOT_LISTENER_MODE_NORMAL;
}
} // namespace

#endif // STOCKMAN_KELPIE_PANEL_INTERNAL_HPP
