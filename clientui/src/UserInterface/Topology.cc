#include <UserInterface/KelpiePanel.hpp>

#include "Internal.hpp"

#include <QBrush>
#include <QColor>
#include <QFont>
#include <QGraphicsEllipseItem>
#include <QGraphicsLineItem>
#include <QGraphicsScene>
#include <QGraphicsSimpleTextItem>
#include <QGraphicsView>
#include <QPen>
#include <QTableWidgetItem>
#include <QTreeWidgetItemIterator>

#include <algorithm>
#include <cmath>
#include <functional>
#include <limits>

namespace
{
struct TopologyEdgeView {
    QString parent;
    QString child;
    bool supplemental{false};
};

QColor nodeStatusColor(const QString& status)
{
    const QString s = status.trimmed().toLower();
    if ( s.contains(QStringLiteral("active")) || s.contains(QStringLiteral("connected")) || s.contains(QStringLiteral("online")) )
    {
        return QColor(70, 200, 120);
    }
    if ( s.contains(QStringLiteral("degraded")) )
    {
        return QColor(245, 200, 90);
    }
    if ( s.contains(QStringLiteral("failed")) || s.contains(QStringLiteral("dead")) )
    {
        return QColor(230, 90, 90);
    }
    if ( s.contains(QStringLiteral("offline")) )
    {
        return QColor(120, 120, 120);
    }
    return QColor(110, 180, 255);
}

QSet<QString> collectVisibleNodes(const kelpieui::v1::GetTopologyResponse& topo, const QString& filter)
{
    QSet<QString> all;
    all.reserve(topo.nodes_size());
    for ( const auto& n : topo.nodes() )
    {
        const QString uuid = QString::fromStdString(n.uuid());
        if ( !uuid.isEmpty() )
        {
            all.insert(uuid);
        }
    }

    const QString needle = filter.trimmed().toLower();
    if ( needle.isEmpty() )
    {
        return all;
    }

    QSet<QString> matched;
    for ( const auto& n : topo.nodes() )
    {
        const QString uuid = QString::fromStdString(n.uuid());
        if ( uuid.isEmpty() )
        {
            continue;
        }
        const QString alias = QString::fromStdString(n.alias());
        const QString status = QString::fromStdString(n.status());
        const QString network = QString::fromStdString(n.network());
        if ( uuid.toLower().contains(needle) || alias.toLower().contains(needle) ||
             status.toLower().contains(needle) || network.toLower().contains(needle) )
        {
            matched.insert(uuid);
        }
    }

    if ( matched.isEmpty() )
    {
        return matched;
    }

    // Keep one-hop neighbors for context.
    for ( const auto& e : topo.edges() )
    {
        const QString parent = QString::fromStdString(e.parent_uuid());
        const QString child = QString::fromStdString(e.child_uuid());
        if ( matched.contains(parent) || matched.contains(child) )
        {
            matched.insert(parent);
            matched.insert(child);
        }
    }

    return matched;
}

std::vector<TopologyEdgeView> collectVisibleEdges(const kelpieui::v1::GetTopologyResponse& topo,
                                                  const QSet<QString>& visibleNodes,
                                                  bool showSupplemental)
{
    std::vector<TopologyEdgeView> edges;
    edges.reserve(topo.edges_size());
    for ( const auto& e : topo.edges() )
    {
        const QString parent = QString::fromStdString(e.parent_uuid());
        const QString child = QString::fromStdString(e.child_uuid());
        if ( parent.isEmpty() || child.isEmpty() )
        {
            continue;
        }
        if ( !visibleNodes.contains(parent) || !visibleNodes.contains(child) )
        {
            continue;
        }
        if ( e.supplemental() && !showSupplemental )
        {
            continue;
        }
        edges.push_back({parent, child, e.supplemental()});
    }
    return edges;
}

QHash<QString, QPointF> computeTreeLayout(const QSet<QString>& visibleNodes,
                                          const std::vector<TopologyEdgeView>& edges)
{
    QHash<QString, QVector<QString>> children;
    QSet<QString> hasParent;
    for ( const auto& edge : edges )
    {
        if ( edge.supplemental )
        {
            continue;
        }
        children[edge.parent].push_back(edge.child);
        hasParent.insert(edge.child);
    }

    for ( auto it = children.begin(); it != children.end(); ++it )
    {
        auto& list = it.value();
        std::sort(list.begin(), list.end());
    }

    QStringList roots;
    roots.reserve(visibleNodes.size());
    for ( const auto& uuid : visibleNodes )
    {
        if ( !hasParent.contains(uuid) )
        {
            roots.push_back(uuid);
        }
    }
    roots.sort();

    constexpr double kX = 160.0;
    constexpr double kY = 120.0;
    QHash<QString, QPointF> pos;
    QSet<QString> visiting;
    int nextLeaf = 0;

    std::function<void(const QString&, int)> assign = [&](const QString& uuid, int depth) {
        if ( uuid.isEmpty() )
        {
            return;
        }
        if ( visiting.contains(uuid) )
        {
            pos.insert(uuid, QPointF(nextLeaf * kX, depth * kY));
            ++nextLeaf;
            return;
        }
        if ( pos.contains(uuid) )
        {
            return;
        }
        visiting.insert(uuid);

        const auto kids = children.value(uuid);
        if ( kids.isEmpty() )
        {
            pos.insert(uuid, QPointF(nextLeaf * kX, depth * kY));
            ++nextLeaf;
            visiting.remove(uuid);
            return;
        }

        double sumX = 0.0;
        int count = 0;
        for ( const auto& child : kids )
        {
            assign(child, depth + 1);
            if ( pos.contains(child) )
            {
                sumX += pos.value(child).x();
                ++count;
            }
        }
        const double x = count > 0 ? (sumX / count) : (nextLeaf * kX);
        pos.insert(uuid, QPointF(x, depth * kY));
        visiting.remove(uuid);
    };

    for ( const auto& root : roots )
    {
        assign(root, 0);
        nextLeaf += 1;
    }

    // Place remaining nodes on an extra row.
    double maxY = 0.0;
    for ( auto it = pos.constBegin(); it != pos.constEnd(); ++it )
    {
        maxY = std::max(maxY, it.value().y());
    }
    const double extraY = maxY + kY;
    QStringList leftovers;
    leftovers.reserve(visibleNodes.size());
    for ( const auto& uuid : visibleNodes )
    {
        if ( !pos.contains(uuid) )
        {
            leftovers.push_back(uuid);
        }
    }
    leftovers.sort();
    for ( const auto& uuid : leftovers )
    {
        pos.insert(uuid, QPointF(nextLeaf * kX, extraY));
        ++nextLeaf;
    }

    for ( auto it = pos.begin(); it != pos.end(); ++it )
    {
        it.value() += QPointF(80.0, 60.0);
    }
    return pos;
}

QHash<QString, QPointF> computeForceLayout(const QSet<QString>& visibleNodes,
                                           const std::vector<TopologyEdgeView>& edges)
{
    QStringList uuids = visibleNodes.values();
    uuids.sort();

    QHash<QString, QPointF> pos;
    const qsizetype nodeCount = uuids.size();
    if ( nodeCount == 0 )
    {
        return pos;
    }

    // Seed positions on a circle.
    const double radius = std::max(220.0, 45.0 * std::sqrt(static_cast<double>(nodeCount)));
    const QPointF center(360.0, 260.0);
    for ( qsizetype i = 0; i < nodeCount; ++i )
    {
        constexpr double kPi = 3.14159265358979323846;
        const double angle = (2.0 * kPi * static_cast<double>(i)) / static_cast<double>(nodeCount);
        pos.insert(uuids[static_cast<int>(i)], center + QPointF(std::cos(angle) * radius, std::sin(angle) * radius));
    }

    if ( nodeCount > 180 )
    {
        return pos;
    }

    const double area = (900.0 * 700.0);
    const double k = std::sqrt(area / static_cast<double>(std::max<qsizetype>(1, nodeCount)));
    double temperature = 55.0;
    constexpr int kIterations = 70;

    for ( int iter = 0; iter < kIterations; ++iter )
    {
        QHash<QString, QPointF> disp;
        for ( const auto& uuid : uuids )
        {
            disp.insert(uuid, QPointF(0.0, 0.0));
        }

        for ( qsizetype i = 0; i < nodeCount; ++i )
        {
            for ( qsizetype j = i + 1; j < nodeCount; ++j )
            {
                const QString& aId = uuids[static_cast<int>(i)];
                const QString& bId = uuids[static_cast<int>(j)];
                QPointF delta = pos.value(aId) - pos.value(bId);
                double dist = std::hypot(delta.x(), delta.y());
                if ( dist < 0.01 )
                {
                    dist = 0.01;
                    delta = QPointF(0.01, 0.01);
                }
                const QPointF dir(delta.x() / dist, delta.y() / dist);
                const double force = (k * k) / dist;
                disp[aId] += dir * force;
                disp[bId] -= dir * force;
            }
        }

        for ( const auto& edge : edges )
        {
            QPointF delta = pos.value(edge.parent) - pos.value(edge.child);
            double dist = std::hypot(delta.x(), delta.y());
            if ( dist < 0.01 )
            {
                dist = 0.01;
                delta = QPointF(0.01, 0.01);
            }
            const QPointF dir(delta.x() / dist, delta.y() / dist);
            const double force = (dist * dist) / k;
            disp[edge.parent] -= dir * force;
            disp[edge.child] += dir * force;
        }

        for ( const auto& uuid : uuids )
        {
            QPointF delta = disp.value(uuid);
            const double mag = std::hypot(delta.x(), delta.y());
            if ( mag < 0.01 )
            {
                continue;
            }
            const double step = std::min(mag, temperature);
            pos[uuid] += QPointF((delta.x() / mag) * step, (delta.y() / mag) * step);
        }

        temperature *= 0.94;
    }

    // Move into positive view-space with margin.
    double minX = std::numeric_limits<double>::max();
    double minY = std::numeric_limits<double>::max();
    for ( auto it = pos.constBegin(); it != pos.constEnd(); ++it )
    {
        minX = std::min(minX, it.value().x());
        minY = std::min(minY, it.value().y());
    }
    const QPointF offset(80.0 - minX, 60.0 - minY);
    for ( auto it = pos.begin(); it != pos.end(); ++it )
    {
        it.value() += offset;
    }
    return pos;
}
}

namespace StockmanNamespace::UserInterface
{
    void KelpiePanel::fitTopologyGraph()
    {
        if ( (topology_.graphView == nullptr) || (topology_.scene == nullptr) )
        {
            return;
        }
        const QRectF rect = topology_.scene->itemsBoundingRect();
        if ( rect.isNull() )
        {
            return;
        }
        topology_.graphView->fitInView(rect.adjusted(-80, -80, 80, 80), Qt::KeepAspectRatio);
    }

    void KelpiePanel::renderTopologyGraph(const kelpieui::v1::GetTopologyResponse& topo)
    {
        if ( (topology_.scene == nullptr) || (topology_.graphView == nullptr) )
        {
            return;
        }

        topology_.scene->clear();
        if ( topo.nodes_size() == 0 )
        {
            auto* msg = topology_.scene->addSimpleText(tr("No topology data"));
            msg->setBrush(QBrush(QColor(220, 220, 220)));
            msg->setPos(20, 20);
            return;
        }

        QHash<QString, const kelpieui::v1::NodeInfo*> nodes;
        nodes.reserve(topo.nodes_size());
        for ( const auto& n : topo.nodes() )
        {
            const QString uuid = QString::fromStdString(n.uuid());
            if ( !uuid.isEmpty() )
            {
                nodes.insert(uuid, &n);
            }
        }

        const QString filter = (topology_.filterInput != nullptr) ? topology_.filterInput->text() : QString();
        const QSet<QString> visibleNodes = collectVisibleNodes(topo, filter);
        if ( visibleNodes.isEmpty() )
        {
            auto* msg = topology_.scene->addSimpleText(tr("No nodes matched filter"));
            msg->setBrush(QBrush(QColor(220, 220, 220)));
            msg->setPos(20, 20);
            return;
        }

        const bool showSupplemental =
            (topology_.showSupplementalCheck == nullptr) || topology_.showSupplementalCheck->isChecked();
        const auto edges = collectVisibleEdges(topo, visibleNodes, showSupplemental);

        const QString layout =
            (topology_.layoutBox != nullptr) ? topology_.layoutBox->currentData().toString() : QStringLiteral("tree");
        const QHash<QString, QPointF> pos =
            layout == QStringLiteral("force") ? computeForceLayout(visibleNodes, edges) : computeTreeLayout(visibleNodes, edges);

        // Draw edges first.
        for ( const auto& edge : edges )
        {
            if ( !pos.contains(edge.parent) || !pos.contains(edge.child) )
            {
                continue;
            }
            const QPointF a = pos.value(edge.parent);
            const QPointF b = pos.value(edge.child);
            QPen pen;
            if ( edge.supplemental )
            {
                pen = QPen(QColor(90, 160, 255), 1.6, Qt::DashLine);
            }
            else
            {
                pen = QPen(QColor(140, 140, 140), 2.2, Qt::SolidLine);
            }
	            auto* line = topology_.scene->addLine(QLineF(a, b), pen);
	            if ( line != nullptr )
	            {
	                line->setZValue(0);
	                line->setFlag(QGraphicsItem::ItemIsSelectable, true);
	                line->setData(1, QStringLiteral("edge"));
	                line->setData(2, edge.parent);
	                line->setData(3, edge.child);
	                line->setData(4, edge.supplemental);
                line->setToolTip(tr("%1 -> %2%3")
                                     .arg(edge.parent,
                                          edge.child,
                                          edge.supplemental ? tr(" (supplemental)") : QString()));
            }
        }

        // Draw nodes.
        constexpr double kR = 18.0;
        QFont labelFont;
        labelFont.setPointSize(10);

        QStringList drawOrder = visibleNodes.values();
        drawOrder.sort();
        for ( const auto& uuid : drawOrder )
        {
            const auto* n = nodes.value(uuid, nullptr);
            if ( n == nullptr || !pos.contains(uuid) )
            {
                continue;
            }

            const QPointF p = pos.value(uuid);
            const QString alias = QString::fromStdString(n->alias());
            const QString status = QString::fromStdString(n->status());
            QString label = alias.trimmed();
            if ( label.isEmpty() )
            {
                label = uuid.left(8);
            }

            const QColor fill = nodeStatusColor(status);
            QPen pen(QColor(30, 30, 30), 2.0);
            if ( uuid == currentNodeUuid_ )
            {
                pen.setWidthF(3.0);
                pen.setColor(QColor(255, 205, 80));
            }

            auto* nodeItem = topology_.scene->addEllipse(p.x() - kR, p.y() - kR, kR * 2, kR * 2, pen, QBrush(fill));
            if ( nodeItem != nullptr )
            {
                nodeItem->setZValue(2);
                nodeItem->setFlag(QGraphicsItem::ItemIsSelectable, true);
                nodeItem->setData(0, uuid);
                nodeItem->setData(1, QStringLiteral("node"));
                nodeItem->setToolTip(tr("UUID: %1\nAlias: %2\nStatus: %3\nDepth: %4\nNetwork: %5\nMemo: %6")
                                         .arg(uuid,
                                              alias,
                                              status,
                                              QString::number(n->depth()),
                                              QString::fromStdString(n->network()),
                                              QString::fromStdString(n->memo())));
            }

            auto* text = topology_.scene->addSimpleText(label, labelFont);
            if ( text != nullptr )
            {
                text->setBrush(QBrush(QColor(235, 235, 235)));
                text->setZValue(3);
                text->setPos(p.x() - (text->boundingRect().width() / 2.0), p.y() + kR + 4.0);
                text->setFlag(QGraphicsItem::ItemIsSelectable, true);
                text->setData(0, uuid);
                text->setData(1, QStringLiteral("label"));
            }
        }

        fitTopologyGraph();
    }

    void KelpiePanel::populateTopologyEdges(const kelpieui::v1::GetTopologyResponse& topo)
    {
        if ( topology_.edgesTable == nullptr )
        {
            return;
        }

        const QString filter = (topology_.filterInput != nullptr) ? topology_.filterInput->text() : QString();
        const QSet<QString> visibleNodes = collectVisibleNodes(topo, filter);
        const bool showSupplemental =
            (topology_.showSupplementalCheck == nullptr) || topology_.showSupplementalCheck->isChecked();
        const auto edges = collectVisibleEdges(topo, visibleNodes, showSupplemental);

        const auto edgeCount = edges.size();
        topology_.edgesTable->setRowCount(static_cast<int>(edgeCount));
        for ( std::size_t index = 0; index < edgeCount; ++index )
        {
            const int row = static_cast<int>(index);
            topology_.edgesTable->setItem(row, 0, new QTableWidgetItem(edges[index].parent));
            topology_.edgesTable->setItem(row, 1, new QTableWidgetItem(edges[index].child));
            topology_.edgesTable->setItem(row, 2, new QTableWidgetItem(edges[index].supplemental ? tr("yes") : tr("no")));
        }
    }

    void KelpiePanel::refreshTopologyView()
    {
        if ( topology_.refreshDebounce != nullptr && topology_.refreshDebounce->isActive() )
        {
            topology_.refreshDebounce->stop();
        }
        if ( topology_.snapshot.nodes_size() == 0 )
        {
            if ( topology_.edgesTable != nullptr )
            {
                topology_.edgesTable->setRowCount(0);
            }
            if ( topology_.scene != nullptr )
            {
                topology_.scene->clear();
                auto* msg = topology_.scene->addSimpleText(tr("No topology data"));
                msg->setBrush(QBrush(QColor(220, 220, 220)));
                msg->setPos(20, 20);
            }
            return;
        }

        renderTopologyGraph(topology_.snapshot);
        populateTopologyEdges(topology_.snapshot);
        applyTopologyHighlights();
    }

    void KelpiePanel::scheduleTopologyViewRefresh()
    {
        if ( topology_.refreshDebounce == nullptr )
        {
            refreshTopologyView();
            return;
        }
        topology_.refreshDebounce->start();
    }

    void KelpiePanel::setTopologyHighlightNode(const QString& uuid)
    {
        topology_.highlightNodeUuid = uuid;
        topology_.highlightParentUuid.clear();
        topology_.highlightChildUuid.clear();
        applyTopologyHighlights();
    }

    void KelpiePanel::setTopologyHighlightEdge(const QString& parentUuid, const QString& childUuid)
    {
        topology_.highlightParentUuid = parentUuid;
        topology_.highlightChildUuid = childUuid;
        topology_.highlightNodeUuid = !childUuid.isEmpty() ? childUuid : parentUuid;
        applyTopologyHighlights();
    }

    void KelpiePanel::applyTopologyHighlights()
    {
        const QString focusNode = topology_.highlightNodeUuid;
        const QString focusParent = topology_.highlightParentUuid;
        const QString focusChild = topology_.highlightChildUuid;

        if ( topology_.edgesTable != nullptr )
        {
            const QColor connectedBg(36, 58, 84);
            const QColor selectedBg(88, 72, 36);
            for ( int row = 0; row < topology_.edgesTable->rowCount(); ++row )
            {
                auto* parentItem = topology_.edgesTable->item(row, 0);
                auto* childItem = topology_.edgesTable->item(row, 1);
                const QString parent = (parentItem != nullptr) ? parentItem->text() : QString();
                const QString child = (childItem != nullptr) ? childItem->text() : QString();
                const bool isEdge = !focusParent.isEmpty() && parent == focusParent && child == focusChild;
                const bool isConnected = !focusNode.isEmpty() && (parent == focusNode || child == focusNode);
                const QBrush brush = isEdge ? QBrush(selectedBg) : (isConnected ? QBrush(connectedBg) : QBrush());
                for ( int col = 0; col < topology_.edgesTable->columnCount(); ++col )
                {
                    if ( auto* item = topology_.edgesTable->item(row, col) )
                    {
                        item->setBackground(brush);
                    }
                }
            }
        }

        if ( topology_.scene == nullptr )
        {
            return;
        }

        for ( auto* item : topology_.scene->items() )
        {
            if ( item == nullptr )
            {
                continue;
            }
            const QString kind = item->data(1).toString();
            if ( kind == QStringLiteral("edge") )
            {
                auto* line = qgraphicsitem_cast<QGraphicsLineItem*>(item);
                if ( line == nullptr )
                {
                    continue;
                }
                const QString parent = item->data(2).toString();
                const QString child = item->data(3).toString();
                const bool supplemental = item->data(4).toBool();
                const bool isEdge = !focusParent.isEmpty() && parent == focusParent && child == focusChild;
                const bool isConnected = !focusNode.isEmpty() && (parent == focusNode || child == focusNode);

                QPen pen;
                if ( isEdge )
                {
                    pen = QPen(QColor(255, 205, 80), 3.6, supplemental ? Qt::DashLine : Qt::SolidLine);
                }
                else if ( isConnected )
                {
                    pen = QPen(QColor(125, 190, 255), 3.0, supplemental ? Qt::DashLine : Qt::SolidLine);
                }
                else if ( supplemental )
                {
                    pen = QPen(QColor(90, 160, 255), 1.6, Qt::DashLine);
                }
                else
                {
                    pen = QPen(QColor(140, 140, 140), 2.2, Qt::SolidLine);
                }
                line->setPen(pen);
                continue;
            }

            if ( kind == QStringLiteral("node") )
            {
                auto* node = qgraphicsitem_cast<QGraphicsEllipseItem*>(item);
                if ( node == nullptr )
                {
                    continue;
                }
                const QString uuid = item->data(0).toString();
                const bool isHighlighted = !focusNode.isEmpty() && uuid == focusNode;
                const bool isEndpoint = !focusParent.isEmpty() && (uuid == focusParent || uuid == focusChild);

                QPen pen = node->pen();
                if ( isHighlighted || isEndpoint )
                {
                    pen.setWidthF(3.6);
                    pen.setColor(QColor(255, 205, 80));
                }
                else
                {
                    pen.setWidthF(uuid == currentNodeUuid_ ? 3.0 : 2.0);
                    pen.setColor(uuid == currentNodeUuid_ ? QColor(255, 205, 80) : QColor(30, 30, 30));
                }
                node->setPen(pen);
                continue;
            }

            if ( kind == QStringLiteral("label") )
            {
                auto* label = qgraphicsitem_cast<QGraphicsSimpleTextItem*>(item);
                if ( label == nullptr )
                {
                    continue;
                }
                const QString uuid = item->data(0).toString();
                const bool highlighted = (!focusNode.isEmpty() && uuid == focusNode) ||
                                         (!focusParent.isEmpty() && (uuid == focusParent || uuid == focusChild));
                label->setBrush(QBrush(highlighted ? QColor(255, 232, 170) : QColor(235, 235, 235)));
            }
        }
    }

    void KelpiePanel::locateTopologyNode()
    {
        if ( topology_.snapshot.nodes_size() == 0 )
        {
            return;
        }
        const QString query = (topology_.locateInput != nullptr) ? topology_.locateInput->text().trimmed() : QString();
        if ( query.isEmpty() )
        {
            return;
        }

        QString foundUuid;
        const QString lower = query.toLower();
        for ( const auto& node : topology_.snapshot.nodes() )
        {
            const QString uuid = QString::fromStdString(node.uuid());
            if ( uuid.compare(query, Qt::CaseInsensitive) == 0 )
            {
                foundUuid = uuid;
                break;
            }
        }
        if ( foundUuid.isEmpty() )
        {
            for ( const auto& node : topology_.snapshot.nodes() )
            {
                const QString uuid = QString::fromStdString(node.uuid());
                const QString alias = QString::fromStdString(node.alias());
                if ( uuid.toLower().contains(lower) || alias.toLower().contains(lower) )
                {
                    foundUuid = uuid;
                    break;
                }
            }
        }

        if ( foundUuid.isEmpty() )
        {
            toastWarn(tr("Topology locate: node not found"));
            return;
        }

        const QString filter = (topology_.filterInput != nullptr) ? topology_.filterInput->text().trimmed() : QString();
        if ( (topology_.filterInput != nullptr) && !filter.isEmpty() && !foundUuid.toLower().contains(filter.toLower()) )
        {
            topology_.filterInput->setText(foundUuid);
        }
        refreshTopologyView();

        setTopologyHighlightNode(foundUuid);
        selectNodeByUuid(foundUuid);

        if ( (topology_.scene != nullptr) && (topology_.graphView != nullptr) )
        {
            for ( auto* item : topology_.scene->items() )
            {
                if ( item == nullptr || item->data(1).toString() != QStringLiteral("node") )
                {
                    continue;
                }
                if ( item->data(0).toString() == foundUuid )
                {
                    topology_.scene->clearSelection();
                    item->setSelected(true);
                    topology_.graphView->centerOn(item);
                    break;
                }
            }
        }
    }

    void KelpiePanel::selectNodeByUuid(const QString& uuid)
    {
        if ( uuid.isEmpty() || (nodesTree_ == nullptr) )
        {
            return;
        }
        QTreeWidgetItemIterator it(nodesTree_);
        while ( (*it) != nullptr )
        {
            auto* item = *it;
            if ( (item != nullptr) && item->text(0) == uuid )
            {
                nodesTree_->setCurrentItem(item);
                item->setSelected(true);
                nodesTree_->scrollToItem(item);
                return;
            }
            ++it;
        }
    }

    void KelpiePanel::refreshTopology()
    {
        auto* ctrl = controller();
        if ( ctrl == nullptr || topology_.edgesTable == nullptr )
        {
            return;
        }

        if ( topology_.refreshButton != nullptr )
        {
            topology_.refreshButton->setEnabled(false);
        }
        if ( topology_.statusLabel != nullptr )
        {
            topology_.statusLabel->setText(tr("Topology: refreshing..."));
        }

        const uint64_t epoch = ctrl->ConnectionEpoch();
        const QString target = (topology_.targetInput != nullptr) ? topology_.targetInput->text().trimmed() : QString();
        const QString network = (topology_.networkInput != nullptr) ? topology_.networkInput->text().trimmed() : QString();

        struct Result {
            uint64_t epoch{0};
            bool ok{false};
            QString error;
            kelpieui::v1::GetTopologyResponse topo;
            bool listNodesOk{false};
            QString listNodesError;
            std::vector<kelpieui::v1::NodeInfo> nodes;
        };

        runAsync<Result>(
            this,
            [ctrl, target, network, epoch]() {
                Result res;
                res.epoch = epoch;
                QString error;
                kelpieui::v1::GetTopologyResponse topo;
                res.ok = ctrl->GetTopology(target, network, &topo, error);
                res.error = error;
                res.topo = topo;
                QString listNodesError;
                std::vector<kelpieui::v1::NodeInfo> nodes;
                res.listNodesOk = ctrl->ListNodes(target, network, &nodes, listNodesError);
                res.listNodesError = listNodesError;
                res.nodes = std::move(nodes);
                return res;
            },
            [this](const Result& res) {
                if ( topology_.refreshButton )
                {
                    topology_.refreshButton->setEnabled(true);
                }

                auto* ctrl = controller();
                if ( ctrl == nullptr || ctrl->ConnectionEpoch() != res.epoch )
                {
                    return;
                }
                if ( !res.ok )
                {
                    toastError(tr("Topology refresh failed: %1").arg(res.error));
                    if ( topology_.statusLabel )
                    {
                        topology_.statusLabel->setText(tr("Topology error: %1").arg(res.error));
                    }
                    topology_.snapshot.Clear();
                    topology_.highlightNodeUuid.clear();
                    topology_.highlightParentUuid.clear();
                    topology_.highlightChildUuid.clear();
                    if ( topology_.edgesTable )
                    {
                        topology_.edgesTable->setRowCount(0);
                    }
                    if ( topology_.scene )
                    {
                        topology_.scene->clear();
                        auto* msg = topology_.scene->addSimpleText(tr("Topology unavailable"));
                        msg->setBrush(QBrush(QColor(220, 220, 220)));
                        msg->setPos(20, 20);
                    }
                    return;
                }

                topology_.snapshot = res.topo;
                if ( topology_.statusLabel )
                {
                    const int listedNodes = res.listNodesOk ? static_cast<int>(res.nodes.size()) : res.topo.nodes_size();
                    topology_.statusLabel->setText(tr("Topology: nodes=%1 edges=%2 updated=%3")
                                                       .arg(listedNodes)
                                                       .arg(res.topo.edges_size())
                                                       .arg(QString::fromStdString(res.topo.last_updated())));
                }
                if ( !res.listNodesOk )
                {
                    toastWarn(tr("List nodes failed: %1").arg(res.listNodesError));
                }
                refreshTopologyView();
            });
    }
}
