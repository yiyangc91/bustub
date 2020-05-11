//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager.h"

#include <list>
#include <unordered_map>

#include "common/logger.h"

namespace bustub {

BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager, LogManager *log_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager), log_manager_(log_manager) {
  // We allocate a consecutive memory space for the buffer pool.
  pages_ = new Page[pool_size_];
  replacer_ = new ClockReplacer(pool_size);

  // Initially, every frame is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.push(static_cast<int>(i));
    pages_[i].page_id_ = INVALID_PAGE_ID;
    pages_[i].pin_count_ = 0;
    pages_[i].is_dirty_ = false;
  }
}

BufferPoolManager::~BufferPoolManager() {
  delete[] pages_;
  delete replacer_;
}

Page *BufferPoolManager::FetchPageImpl(page_id_t page_id) {
  std::lock_guard<std::mutex> lock(latch_);
  LOG_DEBUG("FetchPageImpl(%d)", page_id);

  auto frameIdIterator = page_table_.find(page_id);
  frame_id_t frameId;
  if (frameIdIterator == page_table_.end()) {
    LOG_DEBUG("FetchPageImpl(%d): Page Id not found in page table", page_id);
    if (free_list_.empty() && !victimizeFrame()) {
      return nullptr;
    }

    frameId = free_list_.top();
    free_list_.pop();

    // Pull the page into the empty frame
    pages_[frameId].page_id_ = page_id;
    disk_manager_->ReadPage(page_id, pages_[frameId].data_);
    pages_[frameId].pin_count_ = 1;
    LOG_DEBUG("FetchPageImpl(%d): Read page from disk into frame %d", page_id, frameId);
  } else {
    frameId = frameIdIterator->second;
    LOG_DEBUG("FetchPageImpl(%d): Page Id found at frame %d", page_id, frameId);
    pages_[frameId].WLatch();
    ++pages_[frameId].pin_count_;
    pages_[frameId].WUnlatch();
  }

  // Add to the page table and pin on the replacer
  page_table_.insert({page_id, frameId});
  replacer_->Pin(frameId);

  return &(pages_[frameId]);
}

bool BufferPoolManager::UnpinPageImpl(page_id_t page_id, bool is_dirty) {
  std::lock_guard<std::mutex> lock(latch_);
  LOG_DEBUG("UnpinPageImpl(%d, %d)", page_id, is_dirty);

  auto frameIdIterator = page_table_.find(page_id);
  if (frameIdIterator == page_table_.end()) {
    LOG_DEBUG("UnpinPageImpl(%d, %d): Page Id not found in page table", page_id, is_dirty);
    return false;
  }

  auto frameId = frameIdIterator->second;
  pages_[frameId].WLatch();
  pages_[frameId].is_dirty_ = pages_[frameId].is_dirty_ || is_dirty;
  if (pages_[frameId].pin_count_ == 0) {
    LOG_DEBUG("UnpinPageImpl(%d, %d): Page Id already has a pin count of zero", page_id, is_dirty);
    pages_[frameId].WUnlatch();
    return false;
  }

  --pages_[frameId].pin_count_;
  LOG_DEBUG("UnpinPageImpl(%d, %d): Decrementing pin count to %d", page_id, is_dirty, pages_[frameId].pin_count_);
  if (pages_[frameId].pin_count_ == 0) {
    replacer_->Unpin(frameId);
  }
  pages_[frameId].WUnlatch();
  return true;
}

bool BufferPoolManager::FlushPageImpl(page_id_t page_id) {
  std::lock_guard<std::mutex> lock(latch_);
  LOG_DEBUG("FlushPageImpl(%d)", page_id);

  auto frameIdIterator = page_table_.find(page_id);
  if (frameIdIterator == page_table_.end()) {
    LOG_DEBUG("FlushPageImpl(%d): Page Id not found in page table", page_id);
    return false;
  }

  auto frameId = frameIdIterator->second;
  pages_[frameId].WLatch();
  disk_manager_->WritePage(page_id, pages_[frameId].data_);
  pages_[frameId].is_dirty_ = false;
  pages_[frameId].WUnlatch();
  return true;
}

Page *BufferPoolManager::NewPageImpl(page_id_t *page_id) {
  LOG_DEBUG("NewPageImpl(%p)", page_id);
  std::lock_guard<std::mutex> lock(latch_);

  if (free_list_.empty() && !victimizeFrame()) {
    LOG_DEBUG("NewPageImpl(%p): No space remains and nothing to victimize", page_id);
    *page_id = 0;
    return nullptr;
  }
  frame_id_t frame = free_list_.top();
  BUSTUB_ASSERT(pages_[frame].pin_count_ == 0, "freed frame should not be pinned");
  free_list_.pop();

  // Allocate a new page and clean up page metadata.
  auto allocatedPageId = disk_manager_->AllocatePage();
  pages_[frame].page_id_ = allocatedPageId;
  ++pages_[frame].pin_count_;

  // Add entry into the page table, replacers, and so on
  page_table_.insert({allocatedPageId, frame});
  replacer_->Pin(frame);

  LOG_DEBUG("NewPageImpl(%p): Allocated a page %d at frame %d", page_id, *page_id, frame);
  *page_id = allocatedPageId;
  return &(pages_[frame]);
}

bool BufferPoolManager::DeletePageImpl(page_id_t page_id) {
  LOG_DEBUG("DeletePageImpl(%d)", page_id);
  std::lock_guard<std::mutex> lock(latch_);

  auto frameIdIterator = page_table_.find(page_id);
  if (frameIdIterator == page_table_.end()) {
    LOG_DEBUG("DeletePageImpl(%d): Page Id not found in page table", page_id);
    // Deallocate the page anyway?
    disk_manager_->DeallocatePage(page_id);
    return true;
  }

  // Skip pinned pages.
  auto frameId = frameIdIterator->second;
  pages_[frameId].RLatch();
  if (pages_[frameId].pin_count_ != 0) {
    LOG_DEBUG("DeletePageImpl(%d): Page is still pinned - cannot delete", page_id);
    pages_[frameId].RUnlatch();
    return false;
  }
  pages_[frameId].RUnlatch();

  // No page lock required  - pin count is zero, no other users
  // If we're going to be deleting the page, may as well skip writes
  pages_[frameId].is_dirty_ = false;
  LOG_DEBUG("DeletePageImpl(%d): Wiping page", page_id);
  wipePage(frameIdIterator);
  disk_manager_->DeallocatePage(page_id);
  return true;
}

void BufferPoolManager::FlushAllPagesImpl() {
  LOG_DEBUG("FlushAllPagesImpl()");
  std::lock_guard<std::mutex> lock(latch_);

  for (size_t i = 0; i < pool_size_; ++i) {
    if (pages_[i].page_id_ == INVALID_PAGE_ID) {
      continue;
    }
    pages_[i].WLatch();
    disk_manager_->WritePage(pages_[i].page_id_, pages_[i].data_);
    pages_[i].is_dirty_ = false;
    pages_[i].WUnlatch();
  }
}

bool BufferPoolManager::victimizeFrame() {
  frame_id_t frameId;
  if (!replacer_->Victim(&frameId)) {
    return false;
  }

  // Okay clean up time
  // If it's in the replacer, the page must exist
  auto it = page_table_.find(pages_[frameId].page_id_);
  BUSTUB_ASSERT(it != page_table_.end(), "replaced page should exist");

  // Yet again, no lock required here as a victimized frame has a pin count of 0
  wipePage(it);
  return true;
}

void BufferPoolManager::wipePage(std::unordered_map<page_id_t, frame_id_t>::const_iterator pageTableIterator) {
  auto frameId = pageTableIterator->second;
  if (pages_[frameId].is_dirty_) {
    // We do not need to acquire the lock on the page because the pin count is
    // zero which means we are the only thing accessing the page right now.
    disk_manager_->WritePage(pages_[frameId].page_id_, pages_[frameId].data_);
  }
  pages_[frameId].ResetMemory();
  pages_[frameId].page_id_ = INVALID_PAGE_ID;
  pages_[frameId].pin_count_ = 0;
  pages_[frameId].is_dirty_ = false;
  page_table_.erase(pageTableIterator);
  free_list_.push(frameId);
}

}  // namespace bustub
