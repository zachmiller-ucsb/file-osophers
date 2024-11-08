#ifndef MACROS_H_
#define MACROS_H_

#include "glog/logging.h"

namespace internal {
template <typename T>
T CheckNotNullopt(const char* file, int line, const char* names,
                  std::optional<T>&& t) {
  if (t == std::nullopt) {
    google::LogMessageFatal(file, line, std::make_unique<std::string>(names));
  }
  return std::forward<T>(*t);
}
}  // namespace internal

#define CHECK_NOTNULLOPT(val)                   \
  internal::CheckNotNullopt(__FILE__, __LINE__, \
                            "'" #val "' Must be non nullopt", (val))

#endif  // MACROS_H_