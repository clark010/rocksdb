//  Copyright (c) 2015, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

//#pragma once
#ifndef DATA_ARCHIVAL_FILE_CLEANER_H
#define DATA_ARCHIVAL_FILE_CLEANER_H
#endif /*DATA_ARCHIVAL_FILE_CLEANER_H*/

#include <string>
#include <thread>
#include <queue>

#include "port/port.h"

#include <include/rocksdb/env.h>
#include "sync_point.h"
#include "mutexlock.h"

namespace rocksdb {

class Env;
class Logger;

class DataArchivalFileCleaner {
    public:
        DataArchivalFileCleaner(Env *env, const std::vector<DbPath>* dbPath , std::shared_ptr<Logger> info_log);

        ~DataArchivalFileCleaner();

        void BackgroundCleaner();

        void RequestDeletableFiles();

    private:
        Env *env_;

        port::Mutex mu_;

        port::CondVar cv_;

        bool closing_;

        const std::vector<DbPath>* db_paths_;

        // Queue of files  which referenced by compaction
        //std::queue<std::string> ref_files;

        // Queue of files which can be delete by backgroup clean thread
        std::queue<std::string> deletable_files_;

        std::unique_ptr<std::thread> bg_thread_;

        std::shared_ptr<Logger> info_log_;

        static const uint64_t kMicrosInSecond = 1000 * 1000LL;
};
}
