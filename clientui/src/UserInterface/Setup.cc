#include <UserInterface/KelpiePanel.hpp>

#include <Util/Base.hpp>

namespace StockmanNamespace::UserInterface
{
    void KelpiePanel::setupUi()
    {
        auto* layout = new QVBoxLayout(this);
        layout->setContentsMargins(0, 0, 0, 0);

        workspaceTabs_ = new QTabWidget(this);
        workspaceTabs_->setObjectName(QStringLiteral("KelpieWorkspaceTabs"));
        workspaceTabs_->setDocumentMode(true);

        buildOverviewWorkspaceTab();
        buildStreamsWorkspaceTab();
        buildConsoleWorkspaceTab();
        buildChatWorkspaceTab();
        buildLootWorkspaceTab();
        buildShellWorkspaceTab();

        layout->addWidget(workspaceTabs_, 1);

        // Node-specific actions stay disabled until a target is selected.
        setNodeScopedActionsEnabled(false);
        if ( startSshSessionButton_ != nullptr )
        {
            startSshSessionButton_->setToolTip(tr("Legacy SSH session RPC may be disabled on current server build."));
        }

        wireStateActions();
        wireWorkspaceActions();
        wireTopologyInteractions();

        refreshDtnPolicy();
        refreshStatePage((stateTabs_ != nullptr) ? stateTabs_->currentWidget() : nullptr);

        StockmanNamespace::Util::NormalizeInteractiveControls(this);
    }
}
