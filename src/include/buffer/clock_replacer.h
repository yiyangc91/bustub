//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// clock_replacer.h
//
// Identification: src/include/buffer/clock_replacer.h
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <atomic>
#include <mutex>  // NOLINT
#include <optional>
#include <shared_mutex>
#include <unordered_map>
#include <utility>
#include <vector>

#include "buffer/replacer.h"
#include "common/config.h"

namespace bustub {

struct ClockFrame {
  bool pinned = false;
  bool referenced = false;
};

/**
 * ClockReplacer implements the clock replacement policy, which approximates the Least Recently Used policy.
 */
class ClockReplacer : public Replacer {
 public:
  /**
   * Create a new ClockReplacer.
   * @param num_pages the maximum number of pages the ClockReplacer will be required to store
   */
  explicit ClockReplacer(size_t num_pages);

  /**
   * Destroys the ClockReplacer.
   */
  ~ClockReplacer() override;

  bool Victim(frame_id_t *frame_id) override;

  void Pin(frame_id_t frame_id) override;

  void Unpin(frame_id_t frame_id) override;

  size_t Size() override;

 private:
  void incrementClockHand() {
    if (++clockHand_ == elements_.size()) {
      clockHand_ = 0;
    }
  }

  std::vector<std::optional<ClockFrame>> elements_;
  std::vector<std::shared_mutex> locks_;
  std::atomic_size_t clockSize_;

  std::mutex clockMutex_;
  std::size_t clockHand_;
};

}  // namespace bustub
