#include <Stockman/KelpieState.hpp>

namespace StockmanNamespace
{
    KelpieState::KelpieState(QObject* parent)
        : QObject(parent)
    {}

    bool KelpieState::HasSnapshot() const
    {
        std::lock_guard<std::mutex> guard(mutex_);
        return hasSnapshot_;
    }

    kelpieui::v1::Snapshot KelpieState::Snapshot() const
    {
        std::lock_guard<std::mutex> guard(mutex_);
        return snapshot_;
    }

    void KelpieState::UpdateSnapshot(const kelpieui::v1::Snapshot& snapshot)
    {
        kelpieui::v1::Snapshot copy;
        {
            std::lock_guard<std::mutex> guard(mutex_);
            snapshot_ = snapshot;
            hasSnapshot_ = true;
            copy = snapshot_;
        }
        emit SnapshotUpdated(copy);
    }

    void KelpieState::Clear()
    {
        kelpieui::v1::Snapshot copy;
        {
            std::lock_guard<std::mutex> guard(mutex_);
            snapshot_.Clear();
            hasSnapshot_ = false;
            copy = snapshot_;
        }
        emit SnapshotUpdated(copy);
    }

    void KelpieState::EnqueueEvent(const kelpieui::v1::UiEvent& event)
    {
        kelpieui::v1::UiEvent copy = event;
        emit UiEventReceived(copy);
    }
}
