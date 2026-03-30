#ifndef STOCKMAN_KELPIE_STATE_HPP
#define STOCKMAN_KELPIE_STATE_HPP

#include <QObject>

#include <mutex>
#include <vector>

#include "proto/kelpieui/v1/kelpieui.pb.h"

namespace StockmanNamespace
{
    // Thread-safe snapshot/event store so Qt widgets can observe Kelpie state
    class KelpieState : public QObject
    {
        Q_OBJECT

    public:
        explicit KelpieState(QObject* parent = nullptr);

        [[nodiscard]] bool HasSnapshot() const;
        [[nodiscard]] kelpieui::v1::Snapshot Snapshot() const;

        void UpdateSnapshot(const kelpieui::v1::Snapshot& snapshot);
        void Clear();

        void EnqueueEvent(const kelpieui::v1::UiEvent& event);

    signals:
        void SnapshotUpdated(const kelpieui::v1::Snapshot& snapshot);
        void UiEventReceived(const kelpieui::v1::UiEvent& event);

    private:
        mutable std::mutex mutex_;
        kelpieui::v1::Snapshot snapshot_;
        bool hasSnapshot_{false};
    };
}

#endif // STOCKMAN_KELPIE_STATE_HPP
