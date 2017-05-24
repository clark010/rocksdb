//  Copyright (c) 2013, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
// Copyright (c) 2012 Facebook.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#ifndef ROCKSDB_LITE

#include "rocksdb/utilities/checkpoint.h"

#ifndef __STDC_FORMAT_MACROS
#define __STDC_FORMAT_MACROS
#endif

#include <inttypes.h>
#include <algorithm>
#include <string>
#include <sstream>
#include <iterator>
#include <util/string_util.h>
#include "db/filename.h"
#include "db/wal_manager.h"
#include "rocksdb/db.h"
#include "rocksdb/env.h"
#include "rocksdb/transaction_log.h"
#include "util/file_util.h"
#include "port/port.h"

namespace rocksdb {

class CheckpointImpl : public Checkpoint {
 public:
  // Creates a Checkpoint object to be used for creating openable sbapshots
  explicit CheckpointImpl(DB* db) : db_(db) {}

  // Builds an openable snapshot of RocksDB on the same disk, which
  // accepts an output directory on the same disk, and under the directory
  // (1) hard-linked SST files pointing to existing live SST files
  // SST files will be copied if output directory is on a different filesystem
  // (2) a copied manifest files and other files
  // The directory should not already exist and will be created by this API.
  // The directory will be an absolute path
  using Checkpoint::CreateCheckpoint;
  virtual Status CreateCheckpoint(const std::string& checkpoint_dir) override;
  virtual Status CreateInternalCheckpoint(const std::string& checkpoint_name) override;
  virtual Status RestoreInternalCheckpoint(const std::string& checkpoint_name) override;

 private:
  DB* db_;
};

Status Checkpoint::Create(DB* db, Checkpoint** checkpoint_ptr) {
  *checkpoint_ptr = new CheckpointImpl(db);
  return Status::OK();
}

Status Checkpoint::CreateCheckpoint(const std::string& checkpoint_dir) {
  return Status::NotSupported("");
}

Status Checkpoint::CreateInternalCheckpoint(const std::string& checkpoint_name) {
  return Status::NotSupported("");
}

Status Checkpoint::RestoreInternalCheckpoint(const std::string& checkpoint_name) {
  return Status::NotSupported("");
}

Status CheckpointImpl::RestoreInternalCheckpoint(const std::string& checkpoint_name) {
  //TODO:
  //  1. HDFSEnv not support file lock
  //  2. only one db path and one column family is supported

  if (db_->GetDBOptions().db_paths.size() > 1) {
    return Status::NotSupported("More than one db path is not supported in internal checkpoint");
  }

  DbPath db_path = db_->GetDBOptions().db_paths.front();
  std::string checkpoint_dir = CheckpointDirectory(db_path.path) + "/" + checkpoint_name;
  // 1. check the checkpoint_name instance exists under checkpoint path
  Status s = db_->GetEnv()->FileExists(checkpoint_dir);
  if (!s.ok()) {
    Log(db_->GetDBOptions().info_log, "Cannot find the checkpoint dir %s/%s",
        CheckpointDirectory(db_path.path).c_str(), checkpoint_name.c_str());
    return s;
  }

  // 2. read data.manifest
  std::string content;
  ReadFileToString(db_->GetEnv(), checkpoint_dir, &content);

  std::vector<std::string> ref_files = StringSplit(content, '\n');

  // 3. restore all sst file to  data dir
  // 3.a check all sst file is exist, and find all archive sst file(exclude current data sst)
  std::string archival_dir = DataArchivalDirectory(db_path.path);
  std::vector<std::string> files_need_move_back;
  for (auto file : ref_files) {
    if (file.rfind("MANIFEST") == 0) { //TODO: how impl start with?
      s = CopyFile(db_->GetEnv(), checkpoint_dir + "/" + file, db_path.path + "/" + file, 0);
      if (!s.ok()) {
        Log(db_->GetDBOptions().info_log, "Copy ref manifest:%s failed for checkpoint-%s",
            file.c_str(), checkpoint_name.c_str());
        return s;
      }
    } else {
      s = db_->GetEnv()->FileExists(archival_dir + "/" + file);
      if (!s.ok()) {
        s = db_->GetEnv()->FileExists(db_path.path + "/" + file);
        if (!s.ok()) {
          Log(db_->GetDBOptions().info_log, "Cannot find ref sst-%s for checkpoint-%s",
              file.c_str(), checkpoint_name.c_str());
          return s;
        }
        continue;
      }
      files_need_move_back.push_back(file);
    }
  }

  // 3.b move back all sst file
  for (auto file : files_need_move_back) {
    s = db_->GetEnv()->RenameFile(archival_dir + "/" + file, db_path.path + "/" + file);
    if (!s.ok()) {
      Log(db_->GetDBOptions().info_log, "Moved ref sst-%s failed for checkpoint-%s",
          file.c_str(), checkpoint_name.c_str());
      return s;
    }
  }

  // 4. copy CURRENT etc
  s = CopyFile(db_->GetEnv(), checkpoint_dir + "/CURRENT", db_path.path + "/CURRENT", 0);
  if (!s.ok()) {
    Log(db_->GetDBOptions().info_log, "Copy CURRENT failed for checkpoint-%s", checkpoint_name.c_str());
    return s;
  }

  return s;
}

//TODO: Only one db path and one family
Status CheckpointImpl::CreateInternalCheckpoint(const std::string &checkpoint_name) {
  // TODO: only support one db path
  if (db_->GetDBOptions().db_paths.size() > 1) {
    return Status::NotSupported("More than one DB paths are not supported in CreateInternalCheckpoint");
  }

  // TODO: check family size, if maily more than one then return;


  Status s;
  std::vector<std::string> live_files;
  uint64_t manifest_file_size = 0;
  uint64_t sequence_number = db_->GetLatestSequenceNumber();
  VectorLogPtr live_wal_files;

  std::string checkpoint_dir = db_->GetDBOptions().db_paths.front().path + "/checkpoint/" + checkpoint_name;
  s = db_->GetEnv()->FileExists(checkpoint_dir);
  if (s.ok()) {
    return Status::InvalidArgument("Directory exists");
  } else if (!s.IsNotFound()) {
    assert(s.IsIOError());
    return s;
  }

  s = db_->DisableFileDeletions();
  if (s.ok()) {
    // this will return live_files prefixed with "/"
    s = db_->GetLiveFiles(live_files, &manifest_file_size, true);
  }
  // if we have more than one column family, we need to also get WAL files
  if (s.ok()) {
    s = db_->GetSortedWalFiles(live_wal_files);
  }
  if (!s.ok()) {
    db_->EnableFileDeletions(false);
    return s;
  }

  size_t wal_size = live_wal_files.size();
  Log(db_->GetOptions().info_log,
      "Started the snapshot process -- creating snapshot in directory %s",
      checkpoint_dir.c_str());

  std::string full_private_path = checkpoint_dir + ".tmp";

  // create snapshot directory
  s = db_->GetEnv()->CreateDir(full_private_path);

  // copy/hard link live_files
  // write the live_files into data.manifest
  unique_ptr<WritableFile> manifest_file;
  EnvOptions soptions;
  std::string manifest_fname = full_private_path + "/data.manifest";
  s = db_->GetEnv()->NewWritableFile(manifest_fname, &manifest_file, soptions);
  if (!s.ok()) {
    //TODO: log
    return s;
  }

  for (size_t i = 0; s.ok() && i < live_files.size(); ++i) {
    uint64_t number;
    FileType type;
    bool ok = ParseFileName(live_files[i], &number, &type);
    if (!ok) {
      s = Status::Corruption("Can't parse file name. This is very bad");
      break;
    }

    // we should only get sst, manifest and current files here
    assert(type == kTableFile || type == kDescriptorFile ||
           type == kCurrentFile);
    assert(live_files[i].size() > 0 && live_files[i][0] == '/');
    std::string src_fname = live_files[i];

    if ((type == kTableFile)) {
      //TODO: write the fname into data.manifest
      s = manifest_file->Append(src_fname.substr(1) + "\n");
    } else {
      //TODO: copy the manifest or current files to the checkpoint dir
      Log(db_->GetOptions().info_log, "Copying %s", src_fname.c_str());
      s = CopyFile(db_->GetEnv(), db_->GetName() + src_fname,
                   full_private_path + src_fname,
                   (type == kDescriptorFile) ? manifest_file_size : 0);

      if (type != kCurrentFile) {
        manifest_file->Append(src_fname.substr(1) + "\n");
      }
    }
    if (!s.ok()) {
      break;
    }
    Log(db_->GetDBOptions().info_log, "checkpoint file %s", (db_->GetName() + src_fname).c_str());
  }
  Log(db_->GetOptions().info_log, "Number of log files %" ROCKSDB_PRIszt,
      live_wal_files.size());

  if (s.ok()) {
    //TODO: for multi column family the implemetion is not good
    // Link WAL files. Copy exact size of last one because it is the only one
    // that has changes after the last flush.
    for (size_t i = 0; s.ok() && i < wal_size; ++i) {
      if ((live_wal_files[i]->Type() == kAliveLogFile) &&
          (live_wal_files[i]->StartSequence() >= sequence_number)) {
        Log(db_->GetOptions().info_log, "Copying %s",
            live_wal_files[i]->PathName().c_str());
        s = CopyFile(db_->GetEnv(),
                     db_->GetOptions().wal_dir + live_wal_files[i]->PathName(),
                     full_private_path + live_wal_files[i]->PathName(),
                     live_wal_files[i]->SizeFileBytes()); // TODO: if should use 0 that can copy everything?
      }
    }
  }

  // we copied all the files, enable file deletions
  db_->EnableFileDeletions(false);

  if (s.ok()) {
    // move tmp private backup to real snapshot directory
    s = db_->GetEnv()->RenameFile(full_private_path, checkpoint_dir);
  }
  if (s.ok()) {
    unique_ptr<Directory> checkpoint_directory;
    db_->GetEnv()->NewDirectory(checkpoint_dir, &checkpoint_directory);
    if (checkpoint_directory != nullptr) {
      s = checkpoint_directory->Fsync();
    }
  }

  if (!s.ok()) {
    // clean all the files we might have created
    Log(db_->GetOptions().info_log, "Snapshot failed -- %s",
        s.ToString().c_str());
    // we have to delete the dir and all its children
    std::vector<std::string> subchildren;
    db_->GetEnv()->GetChildren(full_private_path, &subchildren);
    for (auto& subchild : subchildren) {
      Status s1 = db_->GetEnv()->DeleteFile(full_private_path + subchild);
      if (s1.ok()) {
        Log(db_->GetOptions().info_log, "Deleted %s",
            (full_private_path + subchild).c_str());
      }
    }
    // finally delete the private dir
    Status s1 = db_->GetEnv()->DeleteDir(full_private_path);
    Log(db_->GetOptions().info_log, "Deleted dir %s -- %s",
        full_private_path.c_str(), s1.ToString().c_str());
    return s;
  }

  // here we know that we succeeded and installed the new snapshot
  Log(db_->GetOptions().info_log, "Snapshot DONE. All is good");
  Log(db_->GetOptions().info_log, "Snapshot sequence number: %" PRIu64,
      sequence_number);

  return s;
}

// Builds an openable snapshot of RocksDB
Status CheckpointImpl::CreateCheckpoint(const std::string& checkpoint_dir) {
  Status s;
  std::vector<std::string> live_files;
  uint64_t manifest_file_size = 0;
  uint64_t sequence_number = db_->GetLatestSequenceNumber();
  bool same_fs = true;
  VectorLogPtr live_wal_files;

  s = db_->GetEnv()->FileExists(checkpoint_dir);
  if (s.ok()) {
    return Status::InvalidArgument("Directory exists");
  } else if (!s.IsNotFound()) {
    assert(s.IsIOError());
    return s;
  }

  s = db_->DisableFileDeletions();
  if (s.ok()) {
    // this will return live_files prefixed with "/"
    s = db_->GetLiveFiles(live_files, &manifest_file_size, true);
  }
  // if we have more than one column family, we need to also get WAL files
  if (s.ok()) {
    s = db_->GetSortedWalFiles(live_wal_files);
  }
  if (!s.ok()) {
    db_->EnableFileDeletions(false);
    return s;
  }

  size_t wal_size = live_wal_files.size();
  Log(db_->GetOptions().info_log,
      "Started the snapshot process -- creating snapshot in directory %s",
      checkpoint_dir.c_str());

  std::string full_private_path = checkpoint_dir + ".tmp";

  // create snapshot directory
  s = db_->GetEnv()->CreateDir(full_private_path);

  // copy/hard link live_files
  for (size_t i = 0; s.ok() && i < live_files.size(); ++i) {
    uint64_t number;
    FileType type;
    bool ok = ParseFileName(live_files[i], &number, &type);
    if (!ok) {
      s = Status::Corruption("Can't parse file name. This is very bad");
      break;
    }
    // we should only get sst, manifest and current files here
    assert(type == kTableFile || type == kDescriptorFile ||
           type == kCurrentFile);
    assert(live_files[i].size() > 0 && live_files[i][0] == '/');
    std::string src_fname = live_files[i];

    // rules:
    // * if it's kTableFile, then it's shared
    // * if it's kDescriptorFile, limit the size to manifest_file_size
    // * always copy if cross-device link
    if ((type == kTableFile) && same_fs) {
      Log(db_->GetOptions().info_log, "Hard Linking %s", src_fname.c_str());
      s = db_->GetEnv()->LinkFile(db_->GetName() + src_fname,
                                  full_private_path + src_fname);
      if (s.IsNotSupported()) {
        same_fs = false;
        s = Status::OK();
      }
    }
    if ((type != kTableFile) || (!same_fs)) {
      Log(db_->GetOptions().info_log, "Copying %s", src_fname.c_str());
      s = CopyFile(db_->GetEnv(), db_->GetName() + src_fname,
                   full_private_path + src_fname,
                   (type == kDescriptorFile) ? manifest_file_size : 0);
    }
  }
  Log(db_->GetOptions().info_log, "Number of log files %" ROCKSDB_PRIszt,
      live_wal_files.size());

  // Link WAL files. Copy exact size of last one because it is the only one
  // that has changes after the last flush.
  for (size_t i = 0; s.ok() && i < wal_size; ++i) {
    if ((live_wal_files[i]->Type() == kAliveLogFile) &&
        (live_wal_files[i]->StartSequence() >= sequence_number)) {
      if (i + 1 == wal_size) {
        Log(db_->GetOptions().info_log, "Copying %s",
            live_wal_files[i]->PathName().c_str());
        s = CopyFile(db_->GetEnv(),
                     db_->GetOptions().wal_dir + live_wal_files[i]->PathName(),
                     full_private_path + live_wal_files[i]->PathName(),
                     live_wal_files[i]->SizeFileBytes());
        break;
      }
      if (same_fs) {
        // we only care about live log files
        Log(db_->GetOptions().info_log, "Hard Linking %s",
            live_wal_files[i]->PathName().c_str());
        s = db_->GetEnv()->LinkFile(
            db_->GetOptions().wal_dir + live_wal_files[i]->PathName(),
            full_private_path + live_wal_files[i]->PathName());
        if (s.IsNotSupported()) {
          same_fs = false;
          s = Status::OK();
        }
      }
      if (!same_fs) {
        Log(db_->GetOptions().info_log, "Copying %s",
            live_wal_files[i]->PathName().c_str());
        s = CopyFile(db_->GetEnv(),
                     db_->GetOptions().wal_dir + live_wal_files[i]->PathName(),
                     full_private_path + live_wal_files[i]->PathName(), 0);
      }
    }
  }

  // we copied all the files, enable file deletions
  db_->EnableFileDeletions(false);

  if (s.ok()) {
    // move tmp private backup to real snapshot directory
    s = db_->GetEnv()->RenameFile(full_private_path, checkpoint_dir);
  }
  if (s.ok()) {
    unique_ptr<Directory> checkpoint_directory;
    db_->GetEnv()->NewDirectory(checkpoint_dir, &checkpoint_directory);
    if (checkpoint_directory != nullptr) {
      s = checkpoint_directory->Fsync();
    }
  }

  if (!s.ok()) {
    // clean all the files we might have created
    Log(db_->GetOptions().info_log, "Snapshot failed -- %s",
        s.ToString().c_str());
    // we have to delete the dir and all its children
    std::vector<std::string> subchildren;
    db_->GetEnv()->GetChildren(full_private_path, &subchildren);
    for (auto& subchild : subchildren) {
      Status s1 = db_->GetEnv()->DeleteFile(full_private_path + subchild);
      if (s1.ok()) {
        Log(db_->GetOptions().info_log, "Deleted %s",
            (full_private_path + subchild).c_str());
      }
    }
    // finally delete the private dir
    Status s1 = db_->GetEnv()->DeleteDir(full_private_path);
    Log(db_->GetOptions().info_log, "Deleted dir %s -- %s",
        full_private_path.c_str(), s1.ToString().c_str());
    return s;
  }

  // here we know that we succeeded and installed the new snapshot
  Log(db_->GetOptions().info_log, "Snapshot DONE. All is good");
  Log(db_->GetOptions().info_log, "Snapshot sequence number: %" PRIu64,
      sequence_number);

  return s;
}

/*
template<typename Out>
extern void split(const std::string &s, char delim, Out result) {
  std::stringstream ss;
  ss.str(s);
  std::string item;
  while (std::getline(ss, item, delim)) {
    *(result++) = item;
  }
}

extern std::vector<std::string> split(const std::string &s, char delim) {
  std::vector<std::string> elems;
  split(s, delim, std::back_inserter(elems));
  return elems;
}
*/

}  // namespace rocksdb

#endif  // ROCKSDB_LITE
