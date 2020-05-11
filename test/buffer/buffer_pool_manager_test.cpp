//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager_test.cpp
//
// Identification: test/buffer/buffer_pool_manager_test.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager.h"

#include <vector>
#include <cstdio>
#include <memory>
#include <string>

#include "gtest/gtest.h"

namespace bustub {

// NOLINTNEXTLINE
// Check whether pages containing terminal characters can be recovered
TEST(BufferPoolManagerTest, BinaryDataTest) {
  const std::string db_name = "test.db";
  const size_t buffer_pool_size = 10;

  auto *disk_manager = new DiskManager(db_name);
  auto *bpm = new BufferPoolManager(buffer_pool_size, disk_manager);

  page_id_t page_id_temp;
  auto *page0 = bpm->NewPage(&page_id_temp);

  // Scenario: The buffer pool is empty. We should be able to create a new page.
  ASSERT_NE(nullptr, page0);
  EXPECT_EQ(0, page_id_temp);

  char random_binary_data[PAGE_SIZE];
  // Generate random binary data
  unsigned int seed = 15645;
  for (char &i : random_binary_data) {
    i = static_cast<char>(rand_r(&seed) % 256);
  }

  // Insert terminal characters both in the middle and at end
  random_binary_data[PAGE_SIZE / 2] = '\0';
  random_binary_data[PAGE_SIZE - 1] = '\0';

  // Scenario: Once we have a page, we should be able to read and write content.
  std::strncpy(page0->GetData(), random_binary_data, PAGE_SIZE);
  EXPECT_EQ(0, std::strcmp(page0->GetData(), random_binary_data));

  // Scenario: We should be able to create new pages until we fill up the buffer pool.
  for (size_t i = 1; i < buffer_pool_size; ++i) {
    EXPECT_NE(nullptr, bpm->NewPage(&page_id_temp));
  }

  // Scenario: Once the buffer pool is full, we should not be able to create any new pages.
  for (size_t i = buffer_pool_size; i < buffer_pool_size * 2; ++i) {
    EXPECT_EQ(nullptr, bpm->NewPage(&page_id_temp));
  }

  // Scenario: After unpinning pages {0, 1, 2, 3, 4} and pinning another 4 new pages,
  // there would still be one cache frame left for reading page 0.
  for (int i = 0; i < 5; ++i) {
    EXPECT_EQ(true, bpm->UnpinPage(i, true));
    bpm->FlushPage(i);
  }
  for (int i = 0; i < 5; ++i) {
    EXPECT_NE(nullptr, bpm->NewPage(&page_id_temp));
    bpm->UnpinPage(page_id_temp, false);
  }
  // Scenario: We should be able to fetch the data we wrote a while ago.
  page0 = bpm->FetchPage(0);
  EXPECT_EQ(0, strcmp(page0->GetData(), random_binary_data));
  EXPECT_EQ(true, bpm->UnpinPage(0, true));

  // Shutdown the disk manager and remove the temporary file we created.
  disk_manager->ShutDown();
  remove("test.db");

  delete bpm;
  delete disk_manager;
}

// NOLINTNEXTLINE
TEST(BufferPoolManagerTest, SampleTest) {
  const std::string db_name = "test.db";
  const size_t buffer_pool_size = 10;

  auto *disk_manager = new DiskManager(db_name);
  auto *bpm = new BufferPoolManager(buffer_pool_size, disk_manager);

  page_id_t page_id_temp;
  auto *page0 = bpm->NewPage(&page_id_temp);

  // Scenario: The buffer pool is empty. We should be able to create a new page.
  ASSERT_NE(nullptr, page0);
  EXPECT_EQ(0, page_id_temp);

  // Scenario: Once we have a page, we should be able to read and write content.
  snprintf(page0->GetData(), PAGE_SIZE, "Hello");
  EXPECT_EQ(0, strcmp(page0->GetData(), "Hello"));

  // Scenario: We should be able to create new pages until we fill up the buffer pool.
  for (size_t i = 1; i < buffer_pool_size; ++i) {
    EXPECT_NE(nullptr, bpm->NewPage(&page_id_temp));
  }

  // Scenario: Once the buffer pool is full, we should not be able to create any new pages.
  for (size_t i = buffer_pool_size; i < buffer_pool_size * 2; ++i) {
    EXPECT_EQ(nullptr, bpm->NewPage(&page_id_temp));
  }

  // Scenario: After unpinning pages {0, 1, 2, 3, 4} and pinning another 4 new pages,
  // there would still be one buffer page left for reading page 0.
  for (int i = 0; i < 5; ++i) {
    EXPECT_EQ(true, bpm->UnpinPage(i, true));
  }
  for (int i = 0; i < 4; ++i) {
    EXPECT_NE(nullptr, bpm->NewPage(&page_id_temp));
  }

  // Scenario: We should be able to fetch the data we wrote a while ago.
  page0 = bpm->FetchPage(0);
  EXPECT_EQ(0, strcmp(page0->GetData(), "Hello"));

  // Scenario: If we unpin page 0 and then make a new page, all the buffer pages should
  // now be pinned. Fetching page 0 should fail.
  EXPECT_EQ(true, bpm->UnpinPage(0, true));
  EXPECT_NE(nullptr, bpm->NewPage(&page_id_temp));
  EXPECT_EQ(nullptr, bpm->FetchPage(0));

  // Shutdown the disk manager and remove the temporary file we created.
  disk_manager->ShutDown();
  remove("test.db");

  delete bpm;
  delete disk_manager;
}

TEST(BufferPoolManagerTest, MultiPinUnpinTest) {
  const std::string db_name = "test.db";
  const size_t buffer_pool_size = 1; // BP of size 1

  auto disk_manager = std::make_unique<DiskManager>(db_name);
  auto bpm = std::make_unique<BufferPoolManager>(buffer_pool_size, disk_manager.get());

  page_id_t pageId0;
  page_id_t pageId1;
  auto *page0 = bpm->NewPage(&pageId0);

  // Scenario: The buffer pool is empty. We should be able to create a new page.
  ASSERT_NE(nullptr, page0);
  EXPECT_EQ(0, pageId0);

	// Unpin the page and bring in a new one
  snprintf(page0->GetData(), PAGE_SIZE, "Page0 data");
  EXPECT_EQ(0, strcmp(page0->GetData(), "Page0 data"));
	bpm->UnpinPage(pageId0, true);

	// Create page 1
  auto *page1 = bpm->NewPage(&pageId1);
  snprintf(page1->GetData(), PAGE_SIZE, "Page1 data");
  EXPECT_EQ(0, strcmp(page1->GetData(), "Page1 data"));

	// Pin the page multiple times. It should not be replaced.
	bpm->UnpinPage(pageId1, true); // 0
	bpm->UnpinPage(pageId1, false); // 0
	bpm->FetchPage(pageId1); // 1
	bpm->FetchPage(pageId1); // 2
	bpm->UnpinPage(pageId1, false); // 0

	// This should fail
	EXPECT_EQ(nullptr, bpm->FetchPage(pageId0));
	bpm->UnpinPage(pageId1, false); // 0
	EXPECT_NE(nullptr, bpm->FetchPage(pageId0));

  // Shutdown the disk manager and remove the temporary file we created.
  disk_manager->ShutDown();
  remove("test.db");
}

TEST(BufferPoolManagerTest, DeletePinnedPage) {
  const std::string db_name = "test.db";
  const size_t buffer_pool_size = 1; // BP of size 1

  auto disk_manager = std::make_unique<DiskManager>(db_name);
  auto bpm = std::make_unique<BufferPoolManager>(buffer_pool_size, disk_manager.get());

  page_id_t pageId0;
  auto *page0 = bpm->NewPage(&pageId0);
  ASSERT_NE(nullptr, page0);
  EXPECT_EQ(0, pageId0);
  snprintf(page0->GetData(), PAGE_SIZE, "Hello");
  EXPECT_EQ(0, strcmp(page0->GetData(), "Hello"));

	// Try to delete the page
	ASSERT_FALSE(bpm->DeletePage(pageId0));
	ASSERT_NE(nullptr, bpm->FetchPage(pageId0));

	// Actually delete the page
	bpm->UnpinPage(pageId0, true);
	bpm->UnpinPage(pageId0, false);
	ASSERT_TRUE(bpm->DeletePage(pageId0));

  // Shutdown the disk manager and remove the temporary file we created.
  disk_manager->ShutDown();
  remove("test.db");
}

TEST(BufferPoolManagerTest, DeletePinnedPage2) {
  const std::string db_name = "test.db";
  const size_t buffer_pool_size = 1; // BP of size 1

  auto disk_manager = std::make_unique<DiskManager>(db_name);
  auto bpm = std::make_unique<BufferPoolManager>(buffer_pool_size, disk_manager.get());

  page_id_t pageId0;
  auto *page0 = bpm->NewPage(&pageId0);
  ASSERT_NE(nullptr, page0);
  EXPECT_EQ(0, pageId0);
  snprintf(page0->GetData(), PAGE_SIZE, "Hello");
  EXPECT_EQ(0, strcmp(page0->GetData(), "Hello"));
	bpm->UnpinPage(pageId0, true); // 0

	// Try to delete the page
	bpm->FetchPage(pageId0); // 1
	ASSERT_FALSE(bpm->DeletePage(pageId0));
	auto* ptr = bpm->FetchPage(pageId0); // 2
	ASSERT_NE(nullptr, ptr);
  EXPECT_EQ(0, strcmp(ptr->GetData(), "Hello"));

	// Actually delete the page
	bpm->UnpinPage(pageId0, false); // 1
	bpm->UnpinPage(pageId0, true); // 0
	ASSERT_TRUE(bpm->DeletePage(pageId0));

  // Shutdown the disk manager and remove the temporary file we created.
  disk_manager->ShutDown();
  remove("test.db");
}

TEST(BufferPoolManagerTest, NonDirtyPagesAreNotFlushed) {
  const std::string db_name = "test.db";
  const size_t buffer_pool_size = 1; // BP of size 1

  auto disk_manager = std::make_unique<DiskManager>(db_name);
  auto bpm = std::make_unique<BufferPoolManager>(buffer_pool_size, disk_manager.get());

  page_id_t pageId0;
  auto *page0 = bpm->NewPage(&pageId0);
  ASSERT_NE(nullptr, page0);
  EXPECT_EQ(0, pageId0);
  snprintf(page0->GetData(), PAGE_SIZE, "Hello");
  EXPECT_EQ(0, strcmp(page0->GetData(), "Hello"));

	// Deliberately mark the page as not dirty
	bpm->UnpinPage(pageId0, false);
	
	// Replace the page
  page_id_t pageId1;
  bpm->NewPage(&pageId1);
	bpm->UnpinPage(pageId1, false);

	// Fetch the original page
	page0 = bpm->FetchPage(pageId0);
  EXPECT_NE(0, strcmp(page0->GetData(), "Hello"));

  // Shutdown the disk manager and remove the temporary file we created.
  disk_manager->ShutDown();
  remove("test.db");
}

TEST(BufferPoolManagerTest, DirtyTakesPrecendence) {
  const std::string db_name = "test.db";
  const size_t buffer_pool_size = 1; // BP of size 1

  auto disk_manager = std::make_unique<DiskManager>(db_name);
  auto bpm = std::make_unique<BufferPoolManager>(buffer_pool_size, disk_manager.get());

  page_id_t pageId0;
  auto *page0 = bpm->NewPage(&pageId0);
  ASSERT_NE(nullptr, page0);
  EXPECT_EQ(0, pageId0);
  snprintf(page0->GetData(), PAGE_SIZE, "Hello");
  EXPECT_EQ(0, strcmp(page0->GetData(), "Hello"));

	// Deliberately mark the page as not dirty
	bpm->FetchPage(pageId0);
	bpm->FetchPage(pageId0);
	bpm->FetchPage(pageId0);
	bpm->UnpinPage(pageId0, false);
	bpm->UnpinPage(pageId0, true);
	bpm->UnpinPage(pageId0, false);
	bpm->UnpinPage(pageId0, false);
	
	// Replace the page
  page_id_t pageId1;
  bpm->NewPage(&pageId1);
	bpm->UnpinPage(pageId1, false);

	// Fetch the original page
	page0 = bpm->FetchPage(pageId0);
  EXPECT_EQ(0, strcmp(page0->GetData(), "Hello"));

  // Shutdown the disk manager and remove the temporary file we created.
  disk_manager->ShutDown();
  remove("test.db");
}

TEST(BufferPoolManagerTest, FlushUndirtiesThePage) {
  const std::string db_name = "test.db";
  const size_t buffer_pool_size = 1; // BP of size 1

  auto disk_manager = std::make_unique<DiskManager>(db_name);
  auto bpm = std::make_unique<BufferPoolManager>(buffer_pool_size, disk_manager.get());

  page_id_t pageId0;
  auto *page0 = bpm->NewPage(&pageId0);
  ASSERT_NE(nullptr, page0);
  EXPECT_EQ(0, pageId0);
  snprintf(page0->GetData(), PAGE_SIZE, "Hello");
  EXPECT_EQ(0, strcmp(page0->GetData(), "Hello"));

	// Mark the page as dirty and flush the page manually
	bpm->FetchPage(pageId0);
	bpm->UnpinPage(pageId0, true);
	bpm->FlushPage(pageId0);

	// This data is deliberately not marked as dirty
  snprintf(page0->GetData(), PAGE_SIZE, "World");
  EXPECT_EQ(0, strcmp(page0->GetData(), "World"));
	bpm->UnpinPage(pageId0, false);
	
	// Replace the page
  page_id_t pageId1;
  bpm->NewPage(&pageId1);
	bpm->UnpinPage(pageId1, false);

	// Fetch the original page
	page0 = bpm->FetchPage(pageId0);
  EXPECT_EQ(0, strcmp(page0->GetData(), "Hello"));

  // Shutdown the disk manager and remove the temporary file we created.
  disk_manager->ShutDown();
  remove("test.db");
}

TEST(BufferPoolManagerTest, FlushAllUndirtiesThePage) {
  const std::string db_name = "test.db";
  const size_t buffer_pool_size = 10; // Lots of empty space

  auto disk_manager = std::make_unique<DiskManager>(db_name);
  auto bpm = std::make_unique<BufferPoolManager>(buffer_pool_size, disk_manager.get());

  page_id_t pageId0;
  auto *page0 = bpm->NewPage(&pageId0);
  ASSERT_NE(nullptr, page0);
  EXPECT_EQ(0, pageId0);
  snprintf(page0->GetData(), PAGE_SIZE, "Hello");
  EXPECT_EQ(0, strcmp(page0->GetData(), "Hello"));

	// Mark the page as dirty and flush the page manually
	bpm->FetchPage(pageId0);
	bpm->UnpinPage(pageId0, true);
	bpm->FlushAllPages();

	// This data is deliberately not marked as dirty
  snprintf(page0->GetData(), PAGE_SIZE, "World");
  EXPECT_EQ(0, strcmp(page0->GetData(), "World"));
	bpm->UnpinPage(pageId0, false);
	
	// Replace the page
	for (size_t i = 0; i < 10; ++i) {
		page_id_t tmpPageId;
		bpm->NewPage(&tmpPageId);
		bpm->UnpinPage(tmpPageId, false);
	}

	// Fetch the original page
	page0 = bpm->FetchPage(pageId0);
  EXPECT_EQ(0, strcmp(page0->GetData(), "Hello"));

  // Shutdown the disk manager and remove the temporary file we created.
  disk_manager->ShutDown();
  remove("test.db");
}

TEST(BufferPoolManagerTest, ConcurrencyTest) {
  const std::string db_name = "test.db";
  const size_t buffer_pool_size = 201;

  auto disk_manager = std::make_shared<DiskManager>(db_name);
  auto bpm = std::make_shared<BufferPoolManager>(buffer_pool_size, disk_manager.get());

	std::vector<std::thread> threadVector;

	// Have a single hot page that has high contention
	page_id_t hotPageId;
	auto *hotPage = bpm->NewPage(&hotPageId);
	ASSERT_NE(nullptr, hotPage);
	snprintf(hotPage->GetData(), PAGE_SIZE, "Hello");

	for (size_t i = 0; i < 100; ++i) {
		// We run 100 threads at a time
		threadVector.emplace_back([i, hotPageId, disk_manager, bpm]() {
			page_id_t pageId0;
			auto *page0 = bpm->NewPage(&pageId0);
			ASSERT_NE(nullptr, page0);
			snprintf(page0->GetData(), PAGE_SIZE, "thread %zu data 0", i);
			std::string data0 = page0->GetData();

			page_id_t pageId1;
			auto *page1 = bpm->NewPage(&pageId1);
			ASSERT_NE(nullptr, page1);
			snprintf(page1->GetData(), PAGE_SIZE, "thread %zu data 1", i);

			// Fetch the hot page
			auto* hotPagePtr = bpm->FetchPage(hotPageId);
			ASSERT_NE(nullptr, hotPagePtr);

			hotPagePtr->WLatch();
			snprintf(hotPagePtr->GetData(), PAGE_SIZE, "Hello %zu", i);
			hotPagePtr->WUnlatch();

			// Flush the hot page
			bpm->FlushPage(hotPageId);

			// Unpin the hot page
			bpm->UnpinPage(hotPageId, false);

			// Unpin one of the pages and create another one
			ASSERT_TRUE(bpm->UnpinPage(pageId0, true));

			page_id_t pageId2;
			auto *page2 = bpm->NewPage(&pageId2);
			ASSERT_NE(nullptr, page2);
			snprintf(page2->GetData(), PAGE_SIZE, "thread %zu data 2", i);

			// in memory - page 1 and page 2
			// Delete page 1 and bring back page 0
			ASSERT_FALSE(bpm->DeletePage(pageId1));
			bpm->UnpinPage(pageId1, true);
			page0 = bpm->FetchPage(pageId0);
			ASSERT_NE(nullptr, page0);
			EXPECT_EQ(0, std::strcmp(page0->GetData(), data0.c_str()));

			// Unpin both pages
			ASSERT_TRUE(bpm->UnpinPage(pageId0, false));
			ASSERT_FALSE(bpm->UnpinPage(pageId1, false));
			ASSERT_TRUE(bpm->UnpinPage(pageId2, true));
		});
	}

	// Join all the threads
	for (auto& thread : threadVector) {
		thread.join();
	}

	// Fetch the hot page and check the data
	hotPage = bpm->FetchPage(hotPageId);
	ASSERT_NE(nullptr, hotPage);
  EXPECT_NE(0, std::strcmp(hotPage->GetData(), "Hello"));
	ASSERT_TRUE(bpm->UnpinPage(hotPageId, false));

  // Shutdown the disk manager and remove the temporary file we created.
  disk_manager->ShutDown();
  remove("test.db");
}

TEST(BufferPoolManagerTest, ConcurrencyTest2) {
  const std::string db_name = "test.db";
  const size_t buffer_pool_size = 100;

  auto disk_manager = std::make_shared<DiskManager>(db_name);
  auto bpm = std::make_shared<BufferPoolManager>(buffer_pool_size, disk_manager.get());

	std::vector<std::thread> threadVector;

	// Create 100 unpinned pages
	for (size_t i = 0; i < 100; ++i) {
			page_id_t pageId;
			auto *page = bpm->NewPage(&pageId);
			ASSERT_NE(nullptr, page);
			snprintf(page->GetData(), PAGE_SIZE, "Hello World %zu", i);
			bpm->UnpinPage(pageId, true);
	}

	for (size_t i = 0; i < 100; ++i) {
		threadVector.emplace_back([i, bpm]() {
			// In a big loop, create new pages and delete new pages repeatedly
			for (size_t j = 0; j < 50; ++j) {
				page_id_t pageId;
				auto *page = bpm->NewPage(&pageId);
				ASSERT_NE(nullptr, page);
				snprintf(page->GetData(), PAGE_SIZE, "thread %zu iteration %zu", i, j);
				std::string data = page->GetData();
				ASSERT_TRUE(bpm->UnpinPage(pageId, true));

				page_id_t tmpId;
				bpm->NewPage(&tmpId);
				ASSERT_TRUE(bpm->UnpinPage(tmpId, false));

				page = bpm->FetchPage(pageId);
				EXPECT_EQ(0, std::strcmp(page->GetData(), data.c_str()));
				bpm->UnpinPage(pageId, false);
			}
		});
	}

	// Join all the threads
	for (auto& thread : threadVector) {
		thread.join();
	}

  // Shutdown the disk manager and remove the temporary file we created.
  disk_manager->ShutDown();
  remove("test.db");
}

}  // namespace bustub
