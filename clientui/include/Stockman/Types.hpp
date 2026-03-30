#ifndef STOCKMAN_TYPES_HPP
#define STOCKMAN_TYPES_HPP

#include <cstdint>
#include <string>
#include <map>
#include <any>

// 基础类型与通用别名，集中定义便于替换/精简。
namespace StockmanNamespace
{
    using u32 = uint32_t;
    using u64 = uint64_t;

    // windows 习惯兼容
    using PCHAR   = char*;
    using BYTE    = char;
    using PVOID   = void*;
    using LPVOID  = void*;
    using UINT_PTR = unsigned long int;

    using MapStrStr = std::map<std::string, std::string>;
    using MapStrAny = std::map<std::string, std::any>;

    struct RegisteredCommand
    {
        std::string Agent;
        std::string Module;
        std::string Command;
        std::string Help;
        u32         Behaviour;
        std::string Usage;
        std::string Example;
        void*       Function;
        std::string Path;
    };

    struct RegisteredModule
    {
        std::string Agent;
        std::string Name;
        std::string Description;
        std::string Behavior;
        std::string Usage;
        std::string Example;
    };

    namespace Util {
        std::string base64_encode(const char* buf, unsigned int bufLen);
        std::string gen_random(int len);
    }
}

#endif // STOCKMAN_TYPES_HPP
