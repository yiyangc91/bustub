//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// clock_replacer_test.cpp
//
// Identification: test/buffer/clock_replacer_test.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/clock_replacer.h"

#include <cstdio>
#include <thread>  // NOLINT
#include <vector>

#include "gtest/gtest.h"

namespace bustub {

TEST(ClockReplacerTest, SampleTest) {
  ClockReplacer clock_replacer(7);

  // Scenario: unpin six elements, i.e. add them to the replacer.
  clock_replacer.Unpin(1);
  clock_replacer.Unpin(2);
  clock_replacer.Unpin(3);
  clock_replacer.Unpin(4);
  clock_replacer.Unpin(5);
  clock_replacer.Unpin(6);
  clock_replacer.Unpin(1);
  EXPECT_EQ(6, clock_replacer.Size());

  // Scenario: get three victims from the clock.
  int value;
  clock_replacer.Victim(&value);
  EXPECT_EQ(1, value);
  clock_replacer.Victim(&value);
  EXPECT_EQ(2, value);
  clock_replacer.Victim(&value);
  EXPECT_EQ(3, value);

  // Scenario: pin elements in the replacer.
  // Note that 3 has already been victimized, so pinning 3 should have no effect.
  clock_replacer.Pin(3);
  clock_replacer.Pin(4);
  EXPECT_EQ(2, clock_replacer.Size());

  // Scenario: unpin 4. We expect that the reference bit of 4 will be set to 1.
  clock_replacer.Unpin(4);

  // Scenario: continue looking for victims. We expect these victims.
  clock_replacer.Victim(&value);
  EXPECT_EQ(5, value);
  clock_replacer.Victim(&value);
  EXPECT_EQ(6, value);
  clock_replacer.Victim(&value);
  EXPECT_EQ(4, value);
}

TEST(ClockReplacerTest, InsertionIntoPreviousPosition) {
  ClockReplacer clock_replacer(6);

  // 1 (unpinned), 2 (pinned), 3 (unpinned, pointed)
  clock_replacer.Unpin(111);
  clock_replacer.Pin(222);
  clock_replacer.Unpin(333);

  frame_id_t value;
  clock_replacer.Victim(&value);
  EXPECT_EQ(111, value);

  // 1 (victimized, pointed), 2 (pinned), 3 (unpinned)
  clock_replacer.Unpin(444);
  clock_replacer.Pin(111);
  clock_replacer.Unpin(555);
  // 4 (unpinned), 1 (pinned), 5 (unpinned, pointed), 2 (pinned), 3 (unpinned)

  clock_replacer.Pin(333);
  clock_replacer.Unpin(333);
  clock_replacer.Pin(444);
  clock_replacer.Unpin(444);
  // 4 (referenced), 1 (pinned), 5 (unpinned, pointed), 2 (pinned), 3 (referenced)

  clock_replacer.Victim(&value);
  EXPECT_EQ(555, value);
  // 4 (unpinned!), 1 (pinned), 5 (victimized), 2 (pinned), 3 (unpinned!)

  clock_replacer.Pin(777);
  clock_replacer.Pin(666);
  // 4 (unpinned!), 1 (pinned), 7 (pinned), 6(pinned, pointed), 2 (pinned), 3 (unpinned!)

  clock_replacer.Victim(&value);
  EXPECT_EQ(333, value);
  // 4 (unpinned!), 1 (pinned), 7 (pinned), 6(pinned, pointed), 2 (pinned), 3 (victimized, pointed)
  clock_replacer.Unpin(333);
  // 4 (unpinned!), 1 (pinned), 7 (pinned), 6(pinned, pointed), 2 (pinned), 3 (unpinned, pointed)

  clock_replacer.Victim(&value);
  EXPECT_EQ(444, value);
}

TEST(ClockReplacerTest, SkipVictimizedAndPinnedElements) {
  ClockReplacer clock_replacer(6);

  clock_replacer.Unpin(1);
  clock_replacer.Unpin(2);
  clock_replacer.Unpin(3);
  clock_replacer.Pin(4);
  clock_replacer.Pin(5);
  clock_replacer.Unpin(6);
  EXPECT_EQ(4, clock_replacer.Size());

  // victimize
  // from bufferpool's point of view, this eliminates pages 1, 2 and 3
  frame_id_t value;
  clock_replacer.Victim(&value);
  EXPECT_EQ(1, value);
  clock_replacer.Victim(&value);
  EXPECT_EQ(2, value);
  clock_replacer.Victim(&value);
  EXPECT_EQ(3, value);
  EXPECT_EQ(1, clock_replacer.Size());

  // 4/5 is not victimized it is pinned
  clock_replacer.Victim(&value);
  EXPECT_EQ(6, value);

  // This sets up to test skipping referenced elements across a loop
  clock_replacer.Unpin(2);
  clock_replacer.Pin(3);
  clock_replacer.Unpin(3);
  // Final State:
  // 4 (pinned), 5 (pinned), 2 (unpinned), 3 (referenced, pointed)

  // Unpin 4
  clock_replacer.Unpin(4);
  // 4 (referenced), 5 (pinned), 2 (unpinned), 3 (referenced)

  // skips 4 but unreferences it, deletes 2
  clock_replacer.Victim(&value);
  EXPECT_EQ(2, value);
  // 4 (unpinned), 5 (pinned), 2 (victimized, pointed) 3 (referenced)

  // Victimizing the next two elements
  clock_replacer.Victim(&value);
  EXPECT_EQ(4, value);
  clock_replacer.Victim(&value);
  EXPECT_EQ(3, value);
}

TEST(ClockReplacerTest, OrderOfUnpins) {
  // Don't think we should assume frame IDs are in bounds
  ClockReplacer clock_replacer(6);
  clock_replacer.Unpin(1000);
  clock_replacer.Pin(2000);

  frame_id_t value;
  EXPECT_TRUE(clock_replacer.Victim(&value));
  EXPECT_EQ(1000, value);

  // Unpin some unrelated frames and then 2000
  // The expected behavior is that we attempt to reap 2000 first, as it was
  // originally next in the clock, but we fail, and so we reap 5000, 4000 and
  // finally 2000
  clock_replacer.Unpin(5000);
  clock_replacer.Unpin(4000);
  clock_replacer.Unpin(2000);

  clock_replacer.Victim(&value);
  EXPECT_EQ(5000, value);
  clock_replacer.Victim(&value);
  EXPECT_EQ(4000, value);
  clock_replacer.Victim(&value);
  EXPECT_EQ(2000, value);
  EXPECT_EQ(0, clock_replacer.Size());
}

TEST(ClockReplacerTest, SizeRepresentsVictimizablePages) {
  ClockReplacer clock_replacer(6);
  clock_replacer.Pin(1);
  clock_replacer.Unpin(1);
  EXPECT_EQ(1, clock_replacer.Size());
}

TEST(ClockReplacerTest, FailureNoPages) {
  ClockReplacer clock_replacer(6);

  frame_id_t value;
  EXPECT_FALSE(clock_replacer.Victim(&value));
  EXPECT_EQ(0, value);
}

TEST(ClockReplacerTest, FailureOnlyPinnedPages) {
  ClockReplacer clock_replacer(2);
  clock_replacer.Pin(1);

  frame_id_t value;
  EXPECT_FALSE(clock_replacer.Victim(&value));
  EXPECT_EQ(0, value);
}

TEST(ClockReplacerTest, FailureDoubleVictimize) {
  ClockReplacer clock_replacer(2);
  clock_replacer.Unpin(1);

  frame_id_t value;
  EXPECT_TRUE(clock_replacer.Victim(&value));
  EXPECT_EQ(1, value);
  EXPECT_FALSE(clock_replacer.Victim(&value));
  EXPECT_EQ(0, value);
}

TEST(ClockReplacerTest, FailureDoubleUnpin) {
  ClockReplacer clock_replacer(2);
  clock_replacer.Unpin(1);
  clock_replacer.Unpin(1);
  EXPECT_EQ(1, clock_replacer.Size());

  frame_id_t value;
  EXPECT_TRUE(clock_replacer.Victim(&value));
  EXPECT_EQ(1, value);
  EXPECT_FALSE(clock_replacer.Victim(&value));
  EXPECT_EQ(0, value);
}

TEST(ClockReplacerTest, EdgeCaseSingleSize) {
  ClockReplacer clock_replacer(1);
  clock_replacer.Unpin(1);

  frame_id_t value;
  EXPECT_TRUE(clock_replacer.Victim(&value));
  EXPECT_EQ(1, value);
}

TEST(ClockReplacerTest, EdgeCaseZeroSize) {
  ClockReplacer clock_replacer(0);

  frame_id_t value;
  EXPECT_FALSE(clock_replacer.Victim(&value));
  EXPECT_EQ(0, value);
}

TEST(ClockReplacerTest, EdgeCaseReferencedPages) {
  ClockReplacer clock_replacer(2);
  clock_replacer.Pin(1);
  clock_replacer.Unpin(1);

  frame_id_t value;
  EXPECT_TRUE(clock_replacer.Victim(&value));
  EXPECT_EQ(1, value);
}

TEST(ClockReplacerTest, EdgeCaseEmpty) {
  ClockReplacer clock_replacer(2);

  frame_id_t value;
  EXPECT_FALSE(clock_replacer.Victim(&value));
  EXPECT_EQ(0, value);
}

}  // namespace bustub
