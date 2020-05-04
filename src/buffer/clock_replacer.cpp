//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// clock_replacer.cpp
//
// Identification: src/buffer/clock_replacer.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/clock_replacer.h"

#include <algorithm>
#include <thread>

#include "common/logger.h"
#include "common/macros.h"

namespace bustub {

ClockReplacer::ClockReplacer(size_t num_pages) : clockHand_(0), victimizable_(0), clock_(num_pages) {
  freeList_.reserve(num_pages);
  for (size_t i = 0; i < num_pages; ++i) {
    // Everything is free!
    freeList_.push_back(i);
  }
}

ClockReplacer::~ClockReplacer() = default;

bool ClockReplacer::Victim(frame_id_t *frame_id) {
  LOG_DEBUG("Victim()");
  // this write lock gives us exclusive access to everything
  std::lock_guard<std::shared_mutex> lock(clockMutex_);

  // find our victim
  size_t killerHand = clockHand_ + 1;
  if (killerHand == clock_.size()) {
    killerHand = 0;
  }
  while (victimizable_.load()) {
    if (clock_[killerHand]) {
      auto curFrameId = *clock_[killerHand];

      if (!frames_[curFrameId].pinned.load()) {
        if (!frames_[curFrameId].referenced.load()) {
          LOG_DEBUG("clock hand %zu (frame %d) - victimizing", killerHand, curFrameId);
          *frame_id = curFrameId;

          // remove the element from the clock
          --victimizable_;
          clock_[killerHand].reset();
          insertFreeList(killerHand);

          // remove the element from the frames
          frames_.erase(curFrameId);

          clockHand_ = killerHand;
          return true;
        }

        LOG_DEBUG("clock hand %zu (frame %d) is referenced - skipping and toggling reference to false", killerHand,
                  curFrameId);
        frames_[curFrameId].referenced.store(false);
      } else {
        LOG_DEBUG("clock hand %zu (frame %d) is pinned - skipping", killerHand, curFrameId);
      }
    } else {
      LOG_DEBUG("clock hand %zu is empty - skipping", killerHand);
    }

    ++killerHand;
    if (killerHand == clock_.size()) {
      killerHand = 0;
    }
  }

  *frame_id = 0;
  return false;
}

void ClockReplacer::Pin(frame_id_t frame_id) {
  LOG_DEBUG("Pin() frame %d", frame_id);
  {
    std::shared_lock<std::shared_mutex> lock(clockMutex_);
    if (frames_.find(frame_id) != frames_.end()) {
      setPinFrameAttributes(frame_id);
      return;
    }
  }

  // Frame doesn't exist, so we acquire a write lock and check again.
  {
    LOG_DEBUG("frame not found: %d - inserting pinned", frame_id);
    std::lock_guard<std::shared_mutex> lock(clockMutex_);
    if (frames_.find(frame_id) != frames_.end()) {
      setPinFrameAttributes(frame_id);
      return;
    }

    // frame doesn't exist, add it and push it into the clock
    prepareToInsertAtHand();
    BUSTUB_ASSERT(!clock_[clockHand_], "clock hand should point to empty slot");
    frames_.try_emplace(frame_id, true /*pinned*/, false /*referenced*/);
    clock_[clockHand_] = frame_id;
  }
}

void ClockReplacer::Unpin(frame_id_t frame_id) {
  LOG_DEBUG("Unpin() frame %d", frame_id);
  {
    std::shared_lock<std::shared_mutex> lock(clockMutex_);
    if (frames_.find(frame_id) != frames_.end()) {
      setUnpinFrameAttributes(frame_id);
      return;
    }
  }

  // frame doesn't exist here, so we acquire a write lock and check again
  // because we might need to update it
  {
    std::lock_guard<std::shared_mutex> lock(clockMutex_);
    if (frames_.find(frame_id) != frames_.end()) {
      setUnpinFrameAttributes(frame_id);
      return;
    }

    // frame doesn't exist, add it and push it into the clock
    LOG_DEBUG("frame not found: %d - inserting unpinned", frame_id);
    prepareToInsertAtHand();
    BUSTUB_ASSERT(!clock_[clockHand_], "clock hand should point to empty slot");
    frames_.try_emplace(frame_id, false /*pinned*/, false /*referenced*/);
    clock_[clockHand_] = frame_id;
    ++victimizable_;
  }
}

size_t ClockReplacer::Size() { return victimizable_; }

void ClockReplacer::setUnpinFrameAttributes(frame_id_t frame_id) {
  bool pinned = frames_[frame_id].pinned.exchange(false);
  if (pinned) {
    LOG_DEBUG("frame %d is now unpinned from being pinned", frame_id);
    frames_[frame_id].referenced.store(true);
    ++victimizable_;
  }
}

void ClockReplacer::setPinFrameAttributes(frame_id_t frame_id) {
  bool pinned = frames_[frame_id].pinned.exchange(true);
  if (!pinned) {
    LOG_DEBUG("frame %d is now pinned from being unpinned", frame_id);
    --victimizable_;
  }
}

void ClockReplacer::prepareToInsertAtHand() {
  BUSTUB_ASSERT(!freeList_.empty(), "free list is empty");
  BUSTUB_ASSERT(clockHand_ < clock_.size(), "clock hand should be valid");

  LOG_DEBUG("Prepping replacer to insert at hand %zu", clockHand_);

  if (!clock_[clockHand_]) {
    LOG_DEBUG("Inserting at %zu as there is nothing there", clockHand_);
    removeFreeList(clockHand_);
    return;
  }

  auto pos = binarySearchFreeSlots(clockHand_, 0, freeList_.size());
  if (pos + 1 == freeList_.size()) {
    LOG_DEBUG("Bubbling closest free slot at %zu", freeList_[pos]);
    bubbleClockElement(freeList_[pos], clockHand_);
    freeList_.erase(freeList_.begin() + pos);
    return;
  }

  // figure out the closest bubble
  size_t p1Dist = clockHand_ > freeList_[pos] ? clockHand_ - freeList_[pos] : freeList_[pos] - clockHand_;
  size_t p2Dist = clockHand_ > freeList_[pos + 1] ? clockHand_ - freeList_[pos + 1] : freeList_[pos + 1] - clockHand_;
  if (p1Dist < p2Dist) {
    LOG_DEBUG("Bubbling closest free slot at %zu", freeList_[pos]);
    bubbleClockElement(freeList_[pos], clockHand_);
    freeList_.erase(freeList_.begin() + pos);
  } else {
    LOG_DEBUG("Bubbling closest free slot at %zu", freeList_[pos + 1]);
    bubbleClockElement(freeList_[pos + 1], clockHand_);
    freeList_.erase(freeList_.begin() + pos + 1);
  }
}

size_t ClockReplacer::binarySearchFreeSlots(size_t val, size_t begin, size_t end) {
  if (begin == end) {
    return begin;
  }
  size_t mid = begin + (end - begin) / 2;
  if (freeList_[mid] == val) {
    return mid;
  }
  if (freeList_[mid] < val) {
    size_t pos = binarySearchFreeSlots(val, mid + 1, end);
    if (pos == freeList_.size() || freeList_[pos] != val) {
      return mid;
    }
    return pos;
  }
  return binarySearchFreeSlots(val, begin, mid);
}

void ClockReplacer::bubbleClockElement(size_t start, size_t end) {
  if (start > end) {
    LOG_DEBUG("Bubbling free element downwards from %zu to %zu", start, end);
    // The point is to get the free slot one slot above the clock hand
    for (size_t i = start; i != end + 1; --i) {
      LOG_DEBUG("Swapping %zu and %zu", i, i - 1);
      std::swap(clock_[i], clock_[i - 1]);
    }
    LOG_DEBUG("Incrementing clock hand at %zu", clockHand_);
    ++clockHand_;
    if (clockHand_ == clock_.size()) {
      clockHand_ = 0;
    }
  } else {
    LOG_DEBUG("Bubbling free element upwards from %zu to %zu", start, end);
    for (size_t i = start; i != end; ++i) {
      LOG_DEBUG("Swapping %zu and %zu", i, i + 1);
      std::swap(clock_[i], clock_[i + 1]);
    }
  }
}

void ClockReplacer::insertFreeList(size_t val) {
  BUSTUB_ASSERT(freeList_.size() != clock_.size(), "over-extending free list");
  if (freeList_.empty()) {
    freeList_.push_back(val);
    return;
  }
  auto insertionPosition = binarySearchFreeSlots(val, 0, freeList_.size());
  auto it = freeList_.begin() + insertionPosition + 1;
  freeList_.insert(it, val);
}

void ClockReplacer::removeFreeList(size_t val) {
  auto found = binarySearchFreeSlots(val, 0, freeList_.size());
  BUSTUB_ASSERT(freeList_[found] == val, "tried to remove non existing element");
  auto it = freeList_.begin() + found;
  freeList_.erase(it);
}

}  // namespace bustub
