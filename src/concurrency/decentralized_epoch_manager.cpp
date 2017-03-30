//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// centralized_epoch_manager.cpp
//
// Identification: src/concurrency/centralized_epoch_manager.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "concurrency/decentralized_epoch_manager.h"


namespace peloton {
namespace concurrency {


  // enter epoch with thread id
  cid_t DecentralizedEpochManager::EnterEpoch(const size_t thread_id, const bool is_snapshot_read) {

    PL_ASSERT(local_epochs_.find(thread_id) != local_epochs_.end());

    if (is_snapshot_read == true) {

      local_epochs_.at(thread_id)->EnterEpoch(snapshot_global_epoch_, is_snapshot_read);

      return (snapshot_global_epoch_ << 32) | 0x0;

    } else {

      while (true) {
        uint64_t epoch_id = GetCurrentGlobalEpoch();

        // enter the corresponding local epoch.
        bool rt = local_epochs_.at(thread_id)->EnterEpoch(epoch_id, is_snapshot_read);

        // if successfully entered local epoch
        if (rt == true) {
      
          uint32_t next_txn_id = GetNextTransactionId();

          return (epoch_id << 32) | next_txn_id;
        }
      }

    }
  }

  void DecentralizedEpochManager::ExitEpoch(const size_t thread_id, const eid_t epoch_id) {

    PL_ASSERT(local_epochs_.find(thread_id) != local_epochs_.end());

    // exit from the corresponding local epoch.
    local_epochs_.at(thread_id)->ExitEpoch(epoch_id);
 
  }


  eid_t DecentralizedEpochManager::GetExpiredEpochId() {
    eid_t global_expired_eid = MAX_EID;
    
    // for all the local epoch contexts, obtain the minimum max committed epoch id.
    for (auto &local_epoch_itr : local_epochs_) {
      
      eid_t local_expired_eid = local_epoch_itr.second->GetExpiredEpochId(current_global_epoch_);
      
      if (local_expired_eid < global_expired_eid) {
        global_expired_eid = local_expired_eid;
      }
    }

    // if we observe that global_expired_eid is larger than snapshot_global_epoch,
    // then it means the current thread's progress is too slow.
    // we should directly update it to global_expired_eid + 1.
    if (global_expired_eid != MAX_EID && 
        global_expired_eid >= snapshot_global_epoch_) {
      snapshot_global_epoch_ = global_expired_eid + 1;
    }

    return global_expired_eid;
  }

}
}