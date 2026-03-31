cmake_minimum_required( VERSION 3.15 )
project( Stockman )

##
## cmake sets
##
set( QT_VERSION 6 )
set( CMAKE_CXX_STANDARD 20 )
set( CMAKE_AUTOMOC ON )
set( CMAKE_AUTORCC ON )
set( CMAKE_AUTOUIC ON )
set( REQUIRED_LIBS Core Gui Widgets Network WebSockets Sql Concurrent )
set( REQUIRED_LIBS_QUALIFIED Qt6::Core Qt6::Gui Qt6::Widgets Qt6::Network Qt6::WebSockets Qt6::Sql Qt6::Concurrent )
set( APP_ICON_RESOURCE_WINDOWS "data/Stockman.qrc" )
set( CMAKE_RUNTIME_OUTPUT_DIRECTORY .. )
set( CMAKE_INCLUDE_CURRENT_DIR ON )

# VSCode / CMake Tools 等 IDE 在配置时可能不会继承 shell 里的 Homebrew 环境，
# 导致 find_package(gRPC/Protobuf/fmt/...) 找不到 /opt/homebrew 下的 CMake config。
# 这里对 macOS 做一次显式兜底，优先 Apple Silicon 的 /opt/homebrew。
if(APPLE)
    if (EXISTS "/opt/homebrew")
        list(PREPEND CMAKE_PREFIX_PATH "/opt/homebrew")
        if (NOT DEFINED gRPC_DIR AND EXISTS "/opt/homebrew/lib/cmake/grpc")
            set(gRPC_DIR "/opt/homebrew/lib/cmake/grpc")
        endif()
    elseif (EXISTS "/usr/local")
        list(PREPEND CMAKE_PREFIX_PATH "/usr/local")
        if (NOT DEFINED gRPC_DIR AND EXISTS "/usr/local/lib/cmake/grpc")
            set(gRPC_DIR "/usr/local/lib/cmake/grpc")
        endif()
    endif()
endif()

##
## import includes and external libraries
##
include_directories( include )
include_directories( generated )
include_directories( generated/proto )
include_directories( ${Protobuf_INCLUDE_DIRS} )
include_directories( external/spdlog/include )
include_directories( external/json/include )
include_directories( external/toml )

##
## find qt / protobuf / grpc packages
##
find_package( Qt${QT_VERSION} COMPONENTS ${REQUIRED_LIBS} REQUIRED )

# Force CMake to use the system libprotobuf toolchain (3.21.x)
# instead of any newer protoc binary that might be on $PATH.
if (EXISTS "/usr/bin/protoc")
    set(Protobuf_PROTOC_EXECUTABLE "/usr/bin/protoc")
endif()
# gRPC 的 CMake 包会以 CONFIG 模式依赖 Protobuf；这里也用 CONFIG 模式以避免
# FindProtobuf(module) 与 protobuf-config(config) 混用导致的 target 重定义冲突。
find_package( Protobuf CONFIG REQUIRED )
find_package( gRPC CONFIG REQUIRED )
find_package( Threads REQUIRED )
find_package( fmt CONFIG REQUIRED )

set(STOCKMAN_CORE_INCLUDE
        # Core Kelpie / DB / service
        include/Stockman/DBManager/DBManager.hpp
        include/Stockman/KelpieState.hpp
        include/Stockman/Grpc/Client.hpp
        include/Stockman/KelpieController.hpp
        include/Stockman/Stockman.hpp
        include/Stockman/CmdLine.hpp

        # Dialogs
        include/UserInterface/Dialogs/About.hpp
        include/UserInterface/Dialogs/Connect.hpp

        include/UserInterface/StockmanUI.hpp
        include/UserInterface/KelpiePanel.hpp
        include/Util/ThemeManager.hpp
        include/Util/UiConstants.hpp
)

set(STOCKMAN_INCLUDE ${STOCKMAN_CORE_INCLUDE})

# src for UI
set(
        STOCKMAN_UI

        src/UserInterface/StockmanUi.cc
        src/global.cc

        # Dialogs
        src/UserInterface/Dialogs/About.cc
        src/UserInterface/Dialogs/Connect.cc

)

set(
        STOCKMAN_UTIL

        src/Util/ColorText.cpp
        src/Util/Base64.cpp
        src/Util/Base.cpp
        src/Util/ThemeManager.cpp
)

set(
        GRPC_PROTO_SRCS
        generated/proto/kelpieui/v1/kelpieui.pb.cc
        generated/proto/kelpieui/v1/kelpieui.grpc.pb.cc
        generated/dataplane/v1/dataplane.pb.cc
        generated/dataplane/v1/dataplane.grpc.pb.cc
)

	set(
	        STOCKMAN_GRPC_SRC
	        src/Stockman/Grpc/Client.cc
	        src/Stockman/KelpieController.cc
	        src/Stockman/KelpieState.cc
	        src/UserInterface/KelpiePanel.cc
	        src/UserInterface/Chat.cc
	        src/UserInterface/Loot.cc
	        src/UserInterface/Operations.cc
	        src/UserInterface/Listeners.cc
	        src/UserInterface/Proxy.cc
	        src/UserInterface/Network.cc
	        src/UserInterface/Setup.cc
	        src/UserInterface/State.cc
	        src/UserInterface/Workspace.cc
	        src/UserInterface/Wiring.cc
	        src/UserInterface/Transfer.cc
	        src/UserInterface/Socks.cc
	        src/UserInterface/Shell.cc
	        src/UserInterface/Tasking.cc
	        src/UserInterface/Topology.cc
	)

# src for Stockman
set(STOCKMAN_CORE_SOURCE
        src/Main.cc

        # Core services & DB
        src/Stockman/DBManager/DBManager.cc
        src/Stockman/DBManager/Kelpie.cc
        src/Stockman/Stockman.cc
)

set(STOCKMAN_SOURCE
        ${STOCKMAN_CORE_SOURCE}
        ${STOCKMAN_UI}
        ${STOCKMAN_GRPC_SRC}
        ${GRPC_PROTO_SRCS}
)

add_executable( ${PROJECT_NAME} ${STOCKMAN_INCLUDE} ${STOCKMAN_SOURCE} ${STOCKMAN_UTIL} ${APP_ICON_RESOURCE_WINDOWS} data/Stockman.rc )
add_definitions( -DQT_NO_DEBUG_OUTPUT )

target_link_libraries(
    ${PROJECT_NAME}
    ${REQUIRED_LIBS_QUALIFIED}
    gRPC::grpc++
    gRPC::grpc
    protobuf::libprotobuf
    Threads::Threads
    fmt::fmt
    # spdlog::spdlog
    # spdlog::spdlog_header_only
)
