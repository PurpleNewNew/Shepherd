#ifndef STOCKMAN_BASE64_H
#define STOCKMAN_BASE64_H

#include <string>

namespace StockmanNamespace::Util {
std::string base64_encode(const char* buf, unsigned int bufLen);
}

#endif
