//
// Created by clark010 on 2017/5/25.
//

#include <string>
#include <include/rocksdb/env.h>
#include <thread>
#include <port/port.h>
#include <set>
#include <map>

#include "util/mutexlock.h"
#include "util/string_util.h"
#include "db/filename.h"

#ifndef ROCKSDB_CHECKPOINT_FILE_CACHE_H
#define ROCKSDB_CHECKPOINT_FILE_CACHE_H

#endif //ROCKSDB_CHECKPOINT_FILE_CACHE_H

namespace rocksdb {

class Env;
class Logger

class CheckpointFileCache {
public:
  CheckpointFileCache(Env *env, std::string &checkpoint_dir, std::shared_ptr<Logger> info_log);

  ~CheckpointFileCache();

  virtual std::vector<std::string> getUnreferencedFiles(std::vector<std::string>& files);
  
  void BackgroundRefresher();
  
  void RefreshCache();

private:
  Env* env_;
  
  std::string& checkpoint_dir_;
  
  port::Mutex mu_;
  
  port::CondVar cv_;
  
  bool closing_;
  
  long last_modified_time_;

  std::unique_ptr<std::thread> bg_thread_;
  
  std::set<std::string> cache_;
  std::map<std::string, std::set<std::string>> checkpoints_;
  
  std::shared_ptr<Logger> info_log_;
  
  static const uint64_t kMicrosInSecond = 1000 * 1000LL;
};
}