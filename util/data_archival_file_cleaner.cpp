//  Copyright (c) 2015, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#include <vector>
#include "util/data_archival_file_cleaner.h"

namespace rocksdb {


DataArchivalFileCleaner::DataArchivalFileCleaner(Env *env, std::vector<DbPath>* db_paths,
                                                 std::shared_ptr<Logger> info_log)
    :env_(env),
     cv_(&mu_),
     closing_(false),
     info_log_(info_log) {

    db_paths_ = db_paths;
    bg_thread_.reset(new std::thread(&DataArchivalFileCleaner::BackgroundCleaner));
};

void DataArchivalFileCleaner::BackgroundCleaner() {
    TEST_SYNC_POINT("DataArchivalFileCleaner::BackgroundCleaner");

    Header(info_log_, "start %s thread", "DataArchivalFileCleaner");

    //std::vector<std::string> deletable_files;

    while(true) {
        MutexLock l(&mu_);

        // get all deletable files into queue
        RequestDeletableFiles();

        /* TODO: when some thread put file into the queue
        while (deletable_files.empty() && !closing_) {
            cv_.Wait();
        }
        */

        if (closing_) {
            return;
        }

        while (!deletable_files_.empty() && !closing_) {
            std::string deletable_file = deletable_files_.front();
            deletable_files_.pop();

            mu_.Unlock();

            Status s = env_->DeleteFile(deletable_file);
            if (s.ok()) {
                Header(info_log_, "[DataArchivalFileCleaner]Delete file:%s", deletable_file);
            } else {
                Header(info_log_, "[DataArchivalFileCleaner]Failed delete file:%", deletable_file);
            }

            mu_.Lock();
        }

        Header(info_log_, "cleaner start sleep 2s");
        cv_.TimedWait(env_->NowMicros() + kMicrosInSecond * 2);
        Header(info_log_, "cleaner end sleep");
    }
}

//TODO: request ArchivalFileCache to get files to delete
void DataArchivalFileCleaner::RequestDeletableFiles() {
    Header(info_log_, "request deletable files");
    //std::vector<std::string> archival_files;

    for (auto p : *db_paths_) {
        std::vector<std::string> t_files;
        env_->GetChildren(p.path, &t_files);

        //filter undeletable file
        for (auto file : t_files) {
            deletable_files_.push(file);
            Header(info_log_, "add deletable file:%s", file);
        }
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
