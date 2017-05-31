//  Copyright (c) 2015, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#pragma once

#include <string>
#include <thread>
#include <set>
#include <map>

#include <port/port.h>
#include <include/rocksdb/env.h>

#include "util/mutexlock.h"
#include "util/string_util.h"
#include "db/filename.h"


namespace rocksdb {

class Env;
class Logger

class CheckpointFileCache {
public:
  CheckpointFileCache(Env *env, std::string checkpoint_dir, std::shared_ptr<Logger> info_log);

  ~CheckpointFileCache();

  std::vector<std::string> getUnreferencedFiles(std::vector<std::string>& files);
  
  void BackgroundRefresher();
  
  void RefreshCache();

private:
  Env* env_;
  
  port::Mutex mu_;
  
  port::CondVar cv_;
  
  std::string checkpoint_dir_;
  
  bool closing_;
  
  long last_modified_time_;
  
  std::shared_ptr<Logger> info_log_;
  
  std::unique_ptr<std::thread> bg_thread_;
  
  std::set<std::string> cache_;
  std::map<std::string, std::set<std::string>> checkpoints_;
  
  static const uint64_t kMicrosInSecond = 1000 * 1000LL;
};
}