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
//
//
// The implementation here is NOT a clock replacer.
// It's my own bizzare invention where new elements in the clock are inserted
// prior to the hand, and empty slots are shuffled around a vector.
//
// The actual clock replacer is pretty trivial to implement with a single
// circular buffer.

#include "buffer/clock_replacer.h"

#include <algorithm>

#include "common/logger.h"
#include "common/macros.h"

namespace bustub {

ClockReplacer::ClockReplacer(size_t num_pages)
    : elements_(num_pages), locks_(num_pages), clockSize_(0), clockHand_(0) {}

ClockReplacer::~ClockReplacer() = default;

bool ClockReplacer::Victim(frame_id_t *frame_id) {
  LOG_DEBUG("Victim()");
  auto lock = std::lock_guard{clockMutex_};

  for (; Size() != 0; incrementClockHand()) {
    auto &elem = elements_[clockHand_];
    {
      auto sharedLock = std::shared_lock{locks_[clockHand_]};
      if (!elem.has_value()) {
        continue;
      }

      if (elem->pinned) {
        continue;
      }
    }

    {
      auto elemLock = std::unique_lock{locks_[clockHand_]};
      if (elem->referenced) {
				LOG_DEBUG("Unreferencing element %zu", clockHand_);
        elem->referenced = false;
        continue;
      }

			LOG_DEBUG("Victimizing element %zu", clockHand_);
      elem.reset();
      *frame_id = static_cast<frame_id_t>(clockHand_);
      --clockSize_;
      return true;
    }
  }

	LOG_DEBUG("Failed to victimize anything");
  *frame_id = -1;
  return false;
}

void ClockReplacer::Pin(frame_id_t frame_id) {
  LOG_DEBUG("Pin() frame %d", frame_id);
  auto lock = std::unique_lock{locks_[frame_id]};
  auto &elem = elements_[frame_id];
  if (elem.has_value()) {
    if (!elem->pinned) {
      elem->pinned = true;
      --clockSize_;
    }
  } else {
    elem = ClockFrame{true};
  }
}

void ClockReplacer::Unpin(frame_id_t frame_id) {
  LOG_DEBUG("Unpin() frame %d", frame_id);
  auto lock = std::unique_lock{locks_[frame_id]};
  auto &elem = elements_[frame_id];
  if (elem.has_value()) {
    if (elem->pinned) {
      elem->referenced = true;
      elem->pinned = false;
      ++clockSize_;
    }
  } else {
    elem = ClockFrame{};
    ++clockSize_;
  }
}

size_t ClockReplacer::Size() { return clockSize_; }

}  // namespace bustub
