#ifndef STOCKMAN_UTIL_UICONSTANTS_HPP
#define STOCKMAN_UTIL_UICONSTANTS_HPP

namespace StockmanNamespace::UiConstants
{
inline constexpr char kDocsUrl[]          = "https://codeberg.org/Agnoie/Shepherd/src/branch/main/README.md";
inline constexpr char kApiReferenceUrl[]  = "https://codeberg.org/Agnoie/Shepherd/src/branch/main/proto/kelpieui/v1/kelpieui.proto";
inline constexpr char kRepositoryUrl[]    = "https://codeberg.org/Agnoie/Shepherd";
inline constexpr char kDefaultHost[]      = "127.0.0.1";
inline constexpr char kDefaultPort[]      = "50061";
inline constexpr char kStyleDark[]        = ":/stylesheets/Dark";
inline constexpr char kStyleLight[]       = ":/stylesheets/Light";

// Common table headers
inline constexpr char kSessionColId[]        = "ID";
inline constexpr char kSessionColExternal[]  = "External";
inline constexpr char kSessionColInternal[]  = "Internal";
inline constexpr char kSessionColUser[]      = "User";
inline constexpr char kSessionColComputer[]  = "Computer";
inline constexpr char kSessionColOS[]        = "OS";
inline constexpr char kSessionColProcess[]   = "Process";
inline constexpr char kSessionColPid[]       = "PID";
inline constexpr char kSessionColLast[]      = "Last";
inline constexpr char kSessionColHealth[]    = "Health";
inline constexpr char kSessionColSleep[]     = "Sleep";

inline constexpr char kProxyColId[]          = "Proxy ID";
inline constexpr char kProxyColTarget[]      = "Target";
inline constexpr char kProxyColKind[]        = "Kind";
inline constexpr char kProxyColBind[]        = "Bind";
inline constexpr char kProxyColStatus[]      = "Remote/Status";

inline constexpr char kListenerColName[]     = "Name";
inline constexpr char kListenerColProto[]    = "Protocol";
inline constexpr char kListenerColHost[]     = "Host";
inline constexpr char kListenerColPortBind[] = "PortBind";
inline constexpr char kListenerColPortConn[] = "PortConn";
inline constexpr char kListenerColStatus[]   = "Status";

// gRPC connection defaults
inline constexpr int kGrpcDefaultTimeoutMs       = 5000;
inline constexpr int kGrpcKeepaliveTimeMs        = 15000;
inline constexpr int kGrpcKeepaliveTimeoutMs     = 10000;

// Dialog dimensions
inline constexpr int kConnectDialogMinWidth       = 520;
inline constexpr int kConnectDialogMargin         = 8;
inline constexpr int kConnectDialogHSpacing       = 10;
inline constexpr int kConnectDialogVSpacing       = 6;
inline constexpr int kConnectDialogHeaderHeight   = 32;
inline constexpr int kConnectDialogBtnMinWidth    = 110;
inline constexpr int kConnectDialogBtnMinHeight   = 30;
inline constexpr int kConnectDialogInputMinWidth  = 150;
inline constexpr int kConnectDialogSpacerWidth    = 40;
inline constexpr int kConnectDialogSpacerHeight   = 20;
inline constexpr int kConnectDialogListMaxWidth   = 170;

// Event queue limits
inline constexpr int kUiEventQueueMax          = 1000;
inline constexpr int kUiEventChatMax           = 200;

// Dialog titles/messages
inline constexpr char kDlgTitleLoot[]          = "Loot";
inline constexpr char kDlgTitleListener[]      = "Listener";
inline constexpr char kDlgMsgSaveFailed[]      = "Unable to write file: %1";
inline constexpr char kDlgMsgLootSaved[]       = "Saved to %1";
inline constexpr char kDlgMsgNeedSelection[]   = "Please select a row first.";
inline constexpr char kDlgMsgConnectKelpie[]   = "Connect to Kelpie to fetch loot content.";
inline constexpr char kDlgMsgKelpieNotConnected[] = "Kelpie gRPC session not connected.";
inline constexpr char kDlgMsgSnapshotUnavailable[] = "Kelpie snapshot unavailable; connect first.";
inline constexpr char kDlgMsgSnapshotEmpty[]   = "Kelpie snapshot contains no nodes.";
inline constexpr char kDlgMsgListenerNoId[]    = "Pivot listener has no identifier.";
inline constexpr char kDlgMsgSelectListener[]  = "Select one listener to edit";
inline constexpr char kDlgMsgSelectListenerRemove[] = "Select one listener to remove";
inline constexpr char kDlgMsgCreateListenerFailed[] = "Create listener failed: %1";
inline constexpr char kDlgMsgUpdateListenerFailed[] = "Update listener failed: %1";
inline constexpr char kDlgMsgDeleteListenerFailed[] = "Delete listener failed: %1";
inline constexpr char kDlgMsgListenerCreated[]  = "Pivot listener %1 created";
inline constexpr char kDlgMsgListenerUpdated[]  = "Pivot listener %1 status updated";
inline constexpr char kDlgMsgListenerDeleted[]  = "Pivot listener %1 deleted";
inline constexpr char kDlgMsgDeleteListenerConfirm[] = "Delete listener %1?";

// Proxy messages
inline constexpr char kDlgMsgSelectNode[]        = "Select a node first";
inline constexpr char kDlgMsgForwardArgs[]       = "Forward bind and remote are required";
inline constexpr char kDlgMsgBackwardArgs[]      = "Remote and local port are required";
inline constexpr char kDlgMsgStartForwardFailed[] = "Start forward proxy failed: %1";
inline constexpr char kDlgMsgStartBackwardFailed[] = "Start backward proxy failed: %1";
inline constexpr char kDlgMsgStartForwardOk[]     = "Started forward proxy %1";
inline constexpr char kDlgMsgStartBackwardOk[]    = "Started backward proxy %1";
inline constexpr char kDlgMsgSelectProxyRow[]     = "Select a proxy row first";
inline constexpr char kDlgMsgStopProxyFailed[]    = "Stop proxy failed: %1";
inline constexpr char kDlgMsgStopProxyOk[]        = "Stopped proxy %1 (%2)";

// Connect dialog messages
inline constexpr char kDlgTitleConnect[]      = "Kelpie gRPC";
inline constexpr char kDlgMsgNameEmpty[]      = "Name is empty";
inline constexpr char kDlgMsgHostEmpty[]      = "Host is empty";
inline constexpr char kDlgMsgPortEmpty[]      = "Port is empty";
inline constexpr char kDlgMsgProfileExists[]  = "Profile name already exists";
inline constexpr char kDlgMsgTlsMissingCa[]   = "TLS is enabled but CA certificate path is empty; please provide CA or disable TLS.";
inline constexpr char kDlgMsgConnectSuccess[] = "Connected to %1";
inline constexpr char kDlgMsgConnectFailure[] = "Unable to connect to Kelpie gRPC backend";
} // namespace StockmanNamespace::UiConstants

#endif // STOCKMAN_UTIL_UICONSTANTS_HPP
