#ifndef STOCKMAN_UI_REGISTRY_HPP
#define STOCKMAN_UI_REGISTRY_HPP

#include <QHash>
#include <QWidget>
#include <QAction>
#include <QObject>
#include <QCoreApplication>
#include <QThread>
#include <QMetaObject>
#include <memory>

namespace StockmanNamespace { struct AppContext; }

namespace StockmanNamespace
{
class UiResourceRegistry {
public:
    UiResourceRegistry() = default;

    void RegisterWidget(QWidget* w, const QString& owner = {}) {
        if (w == nullptr) { return;
}
        if (!owner.isEmpty()) { widgetOwners_[w] = owner; }
        QObject::connect(w, &QObject::destroyed, [this, w]() { widgetOwners_.remove(w); });
    }

    void RegisterAction(QAction* a, const QString& owner = {}) {
        if (a == nullptr) { return;
}
        if (!owner.isEmpty()) { actionOwners_[a] = owner; }
        QObject::connect(a, &QObject::destroyed, [this, a]() { actionOwners_.remove(a); });
    }

    void RegisterObject(QObject* obj, const QString& owner = {}) {
        if (obj == nullptr) { return;
}
        if (auto* w = qobject_cast<QWidget*>(obj)) {
            RegisterWidget(w, owner);
            return;
        }
        if (auto* a = qobject_cast<QAction*>(obj)) {
            RegisterAction(a, owner);
            return;
        }
        if (!owner.isEmpty()) {
            otherOwners_[obj] = owner;
            QObject::connect(obj, &QObject::destroyed, [this, obj]() { otherOwners_.remove(obj); });
        }
    }

    QString ownerOf(QWidget* w) const { return widgetOwners_.value(w); }
    QString ownerOf(QAction* a) const { return actionOwners_.value(a); }
    QString ownerOf(QObject* o) const { return otherOwners_.value(o); }

    void CleanupAll() {
        for (auto it = widgetOwners_.begin(); it != widgetOwners_.end(); ++it) {
            if (it.key() != nullptr) { it.key()->deleteLater();
}
        }
        for (auto it = actionOwners_.begin(); it != actionOwners_.end(); ++it) {
            if (it.key() != nullptr) { it.key()->deleteLater();
}
        }
        for (auto it = otherOwners_.begin(); it != otherOwners_.end(); ++it) {
            if (it.key() != nullptr) { it.key()->deleteLater();
}
        }
        widgetOwners_.clear();
        actionOwners_.clear();
        otherOwners_.clear();
    }

private:
    QHash<QWidget*, QString> widgetOwners_;
    QHash<QAction*, QString> actionOwners_;
    QHash<QObject*, QString> otherOwners_;
};
} // namespace StockmanNamespace

// Run a callable on the Qt UI thread; if already on UI thread, runs immediately (blocking when cross-thread).
template <typename F>
void RunOnUi(F&& fn) {
    auto* app = QCoreApplication::instance();
    if (!app) { fn(); return; }
    if (QThread::currentThread() == app->thread()) {
        fn();
        return;
    }
    QMetaObject::invokeMethod(app, std::forward<F>(fn), Qt::BlockingQueuedConnection);
}

StockmanNamespace::UiResourceRegistry* UiRegistry(struct StockmanNamespace::AppContext* ctx);

#endif // STOCKMAN_UI_REGISTRY_HPP
