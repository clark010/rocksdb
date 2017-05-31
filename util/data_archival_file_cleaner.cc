//  Copyright (c) 2015, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#include "util/data_archival_file_cleaner.h"

#include <iostream>
#include <algorithm>

namespace rocksdb {

DataArchivalFileCleaner::DataArchivalFileCleaner(Env *env, const std::vector<DbPath> *db_paths,
                                                 std::shared_ptr<Logger> info_log)
  : env_(env),
    cv_(&mu_),
    closing_(false),
    info_log_(info_log) {

  db_paths_ = db_paths;
  chk_file_cache_.reset(new CheckpointFileCache(env, ((*db_paths).at(0)), info_log));
  bg_thread_.reset(new std::thread(&DataArchivalFileCleaner::BackgroundCleaner, this));
};

void DataArchivalFileCleaner::BackgroundCleaner() {
  TEST_SYNC_POINT("[DataArchivalFileCleaner]DataArchivalFileCleaner::BackgroundCleaner");

  Header(info_log_, "start %s thread", "DataArchivalFileCleaner");
  std::cout << "[DataArchivalFileCleaner]Start DataArchivalFileCleaner thread" << std::endl;

  while(true) {
    MutexLock l(&mu_);

    // get all deletable files into queue
    std::queue<std::string> deletable_files;
    RequestDeletableFiles(deletable_files);

    /* TODO: when some thread put file into the queue
    while (deletable_files.empty() && !closing_) {
      cv_.Wait();
    }
    */

    if (closing_) {
      //std::cout << "[DataArchivalFileCleaner]:Cleaner is closing, return" << std::endl;
      return;
    }

    while (!deletable_files.empty() && !closing_) {
      const std::string deletable_file = deletable_files.front();
      deletable_files.pop();

      mu_.Unlock();

      Status s = env_->DeleteFile(deletable_file);
      if (s.ok()) {
        Header(info_log_, "[DataArchivalFileCleaner]Delete file:%s", deletable_file.c_str());
        std::cout << "[DataArchivalFileCleaner]Delete file:" << deletable_file << std::endl;
      } else {
        std::cout << "[DataArchivalFileCleaner]Failed Delete file:" << deletable_file
                  << ", error:" << s.ToString()
                  << std::endl;
        Header(info_log_, "[DataArchivalFileCleaner]Failed delete file:%, error info: %s",
               deletable_file.c_str(), s.ToString().c_str());
      }

      mu_.Lock();
    }

    env_->SleepForMicroseconds(kMicrosInSecond*2);
  }
}

//TODO: request ArchivalFileCache to get files to delete
void DataArchivalFileCleaner::RequestDeletableFiles(std::queue<std::string>& deletable_files) {
  Header(info_log_, "[DataArchivalFileCleaner]Request deletable files");
  std::cout << "[DataArchivalFileCleaner]Request deletable files" << std::endl;

  for (auto p : *db_paths_) {
    std::vector<std::string> arc_files;
    std::string arc_dir = DataArchivalDirectory(p.path);
    Status s = env_->GetChildren(arc_dir, &arc_files);
    if (!s.ok()) {
        Header(info_log_, "[DataArchivalFileCleaner]list archive dir failed");
        return;
    }

    std::vector<std::string> chk_sub_dirs;
    std::string chk_dir = CheckpointDirectory(p.path);
    s = env_->GetChildren(chk_dir, &chk_sub_dirs);
    if (!s.ok()) {
      Header(info_log_, "[DataArchivalFileCleaner]list checkpoint dir failed");
      return;
    }

    std::queue<std::string> checkpoint_ref_files;
    std::vector<std::string> unref_files = chk_file_cache_.get()->getUnreferencedFiles(arc_files);
    for (auto f : unref_files) {
      deletable_files.push(f);
      std::cout << "add deletable file:" << f << std::endl;
    }
  
    //std::set<std::string> checkpoint_ref_files;
    
    /*
    for (auto sdir : chk_sub_dirs) {
      if (sdir == "." || sdir == "..") {
        continue;
      }
      std::string chk_manifest = chk_dir + "/" + sdir + "/data.manifest"; //TODO: use const var for data.manifest
      std::string content;
      s = ReadFileToString(env_, chk_manifest, &content);
      if (!s.ok()) {
        Header(info_log_, "[DataArchivalFileCleaner]read checkpoint-%s manifest failed", sdir.c_str());
        return;
      }
    
      std::vector<std::string> ref_files_ = StringSplit(content, '\n');
      checkpoint_ref_files.insert(checkpoint_ref_files.end(), ref_files_.begin(), ref_files_.end());
      ref_files_.clear();
    }
    
    
    uint64_t number;
    FileType type;
    Slice slice;
    for (auto file : arc_files) {
      if (ParseFileName(file, &number, slice, &type)) {
        if (std::find(checkpoint_ref_files.begin(), checkpoint_ref_files.end(), file) == checkpoint_ref_files.end()) {
          deletable_files.push(arc_dir + "/" + file);
          Header(info_log_, "[DataArchivalFileCleaner]Add deletable file:%s", file.c_str());
          std::cout << "[DataArchivalFileCleaner]Add deletable file:"
                    << file
                    << std::endl;
        } else {
          std::cout << "[DataArchivalFileCleaner]Not deletable file:"
                    << file
                    << std::endl;
        }
      }
    }
     */
  
  }
}

DataArchivalFileCleaner::~DataArchivalFileCleaner() {
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
