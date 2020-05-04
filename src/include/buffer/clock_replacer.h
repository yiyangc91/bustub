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
#include <vector>

#include "buffer/replacer.h"
#include "common/config.h"

namespace bustub {

struct ClockFrame {
  ClockFrame() : pinned(false), referenced(false) {}
  ClockFrame(bool pinned, bool referenced) : pinned(pinned), referenced(referenced) {}
  std::atomic_bool pinned;
  std::atomic_bool referenced;
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
  // These methods assume the clockMutex is obtained.

  /*
   * Prepares free space at the hand by bubbling free space (or doing nothing).
   * This updates the free space list and erases the free space. Callers should
   * ensure the space is used otherwise we'll leak space.
   */
  void prepareToInsertAtHand();
  void bubbleClockElement(size_t start, size_t end);
  size_t binarySearchFreeSlots(size_t val, size_t begin, size_t end);
  void setUnpinFrameAttributes(frame_id_t frame_id);
  void setPinFrameAttributes(frame_id_t frame_id);

  void insertFreeList(size_t val);
  void removeFreeList(size_t val);

  // exclusive access gives us write access to everything
  // shared access allows us to write to frames - this is OKAY because
  // - reference bit is only ever toggled on by shared writers
  // - pinning and unpinning is a single atomic boolean
  std::shared_mutex clockMutex_;

  // ASSUMPTION: whenever we victimize something, we usually want to fill
  // the space immediately. So instead of using a list and dealing with memory
  // allocations, our clock is just fragmented. We expect to insert a frame into
  // the removed position immediately.
  // This does mean we need to deal with the case where this assumption falls
  // flat by maintaining a vector of empty slots so we can bubble the empty slots
  // into the correct position.
  size_t clockHand_;
  std::atomic_size_t victimizable_;
  std::vector<std::optional<frame_id_t>> clock_;

  // Sorted list of empty slots. This should generally be quite small.
  // Effectively a sorted stack except no allocs
  std::vector<size_t> freeList_;

  std::unordered_map<frame_id_t, ClockFrame> frames_;
};

}  // namespace bustub
