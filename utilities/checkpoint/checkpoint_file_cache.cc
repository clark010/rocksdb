//  Copyright (c) 2015, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#include <iostream>
#include "utilities/checkpoint/checkpoint_file_cache.h"

namespace rocksdb {

CheckpointFileCache::CheckpointFileCache(Env *env,
                                         DbPath db_path,
                                         std::shared_ptr<Logger> info_log)
  :env_(env),
   cv_(&mu_),
   db_path_(db_path),
   closing_(false),
   last_modified_time_(0),
   info_log_(info_log) {
  
  checkpoint_dir_ = db_path_.path + "/checkpoint";
  bg_thread_.reset(new std::thread(&CheckpointFileCache::BackgroundRefresher, this));
};

void CheckpointFileCache::BackgroundRefresher() {
  while(true) {
    if (closing_) {
      return;
    }
  
    RefreshCache();
  
    env_->SleepForMicroseconds(kMicrosInSecond*60);
  }
}

std::vector<std::string> CheckpointFileCache::getUnreferencedFiles(std::vector<std::string> &files) {
  
  std::vector<std::string> unref_files;
  
  //MutexLock l(&mu_);
  
  bool freshed = false;
  for (auto file : files) {
    std::cout << "check file deletable:" << file << std::endl;
    if (!freshed && cache_.find(file) == cache_.end()) {
      //mu_.Unlock();
      
      CheckpointFileCache::RefreshCache();
      std::cout << "current cache size:" << cache_.size() << std::endl;
      freshed = true;
      //mu_.Lock();
    }
    
    if (cache_.find(file) != cache_.end()) {
      continue;
    }
    unref_files.push_back(file);
  }
  
  return unref_files;
}

void CheckpointFileCache::RefreshCache() {
  TEST_SYNC_POINT("[CachekpointFileCache]CachekpointFileCache::BackgroundRefresher");
  
  std::cout << "[CachekpointFileCache]refresh cache" << std::endl;
  
  MutexLock l(&mu_);
  
  Status s;
  
  // 1. check checkpoint root dir modify time is changed
  uint64_t modified_time;
  s = env_->GetFileModificationTime(checkpoint_dir_, &modified_time);
  if (!s.ok()) {
    Header(info_log_, "get checkpoint root dir modified time failed");
    return;
  }
  
  if (modified_time <= last_modified_time_) {
    return;
  }
  
  // 2. list checkpoint root dir and find all changed dir which modified time > last_modifed_time
  std::vector<std::string> chk_sub_dirs;
  s = env_->GetChildren(checkpoint_dir_, &chk_sub_dirs);
  if (!s.ok()) {
    Header(info_log_, "list checkpoint root dir failed");
    return;
  }
  
  if (chk_sub_dirs.size() == 0) {
    return;
  }
  
  std::map<std::string, std::set<std::string>> knows;
  std::set<std::string> all_ref_files_;
  
  // 3. update the cache
  for (std::string dir : chk_sub_dirs) {
    s = env_->GetFileModificationTime(checkpoint_dir_ + "/" + dir + "/data.manifest", &modified_time);
    if (!s.ok()) {
      Header(info_log_, "get checkpoint-%s CURRENT modified time failed", dir.c_str());
      continue;
    }
  
    //TODO: if we should add a new struct to store dir info for every checkpoint
  
    if (modified_time > last_modified_time_) {
      std::string content;
      ReadFileToString(env_, checkpoint_dir_ + "/" + dir + "/data.manifest", &content);
      
      std::vector<std::string> ref_files = StringSplit(content, '\n');
      if (ref_files.size() <  2) {
        Header(info_log_, "checkpoint-%s data.manifest pattern error", dir.c_str());
        continue;
      }
  
      std::set<std::string> buf_ref_files_;
      for (auto file : ref_files) {
        Slice rest(file);
        if (!rest.starts_with("MANIFEST")) {
          //cache_.insert(file);
          all_ref_files_.insert(file);
          buf_ref_files_.insert(file);
        }
      }
      
      knows.insert(std::make_pair(dir, buf_ref_files_));
    } else {
      std::set<std::string> cset = checkpoints_.find(dir)->second;
      knows.insert(std::make_pair(dir, cset)); //TODO: bug?
      all_ref_files_.insert(cset.begin(), cset.end());
    }
  }
  
  checkpoints_.clear();  //TODO: in c++, if we should release all old set?
  checkpoints_.insert(knows.begin(), knows.end());
  
  cache_.clear();
  cache_.insert(all_ref_files_.begin(), all_ref_files_.end());
}

CheckpointFileCache::~CheckpointFileCache() {
  {
    MutexLock l(&mu_);
    closing_ = true;
    cv_.SignalAll();
  }
  
  if (bg_thread_) {
    bg_thread_->join();
  }
}

}