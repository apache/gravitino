/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.gravitino.server.web.pagination;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestListPager {

  private static final String SCOPE = PageToken.scopeOf("test", "lake");
  private static final int MAX = 1000;

  @Test
  public void testIsPaged() {
    Assertions.assertFalse(ListPager.isPaged(null, null));
    Assertions.assertTrue(ListPager.isPaged(5, null));
    Assertions.assertTrue(ListPager.isPaged(null, "t"));
  }

  @Test
  public void testWalkReturnsEveryEntryOnceInOrder() {
    List<String> names = names(23);
    Collections.shuffle(names);
    List<String> walked = walk(names, n -> true, 5);
    Assertions.assertEquals(new ArrayList<>(new TreeSet<>(names)), walked);
  }

  @Test
  public void testEmptyList() {
    ListPager.Result<String> page = page(Collections.emptyList(), n -> true, 3, null);
    Assertions.assertTrue(page.items().isEmpty());
    Assertions.assertNull(page.nextPageToken());
  }

  @Test
  public void testExactMultipleOfPageSizeHasNoTrailingToken() {
    List<String> names = names(6);
    ListPager.Result<String> p1 = page(names, n -> true, 3, null);
    Assertions.assertNotNull(p1.nextPageToken());
    ListPager.Result<String> p2 = page(names, n -> true, 3, p1.nextPageToken());
    Assertions.assertEquals(3, p2.items().size());
    Assertions.assertNull(p2.nextPageToken(), "no empty last page");
  }

  @Test
  public void testBoundaryOnHiddenEntriesKeepsPagesFull() {
    // n00..n19; the caller sees every entry except n03..n09.
    List<String> names = names(20);
    Set<String> hidden = new HashSet<>(names.subList(3, 10));
    Predicate<String> canSee = n -> !hidden.contains(n);

    ListPager.Result<String> p1 = page(names, canSee, 3, null);
    Assertions.assertEquals(Arrays.asList("n00", "n01", "n02"), p1.items());
    Assertions.assertNotNull(p1.nextPageToken());

    ListPager.Result<String> p2 = page(names, canSee, 3, p1.nextPageToken());
    Assertions.assertEquals(Arrays.asList("n10", "n11", "n12"), p2.items());

    List<String> walked = walk(names, canSee, 3);
    Assertions.assertEquals(
        names.stream().filter(canSee).collect(Collectors.toList()), walked, "full walk");
  }

  @Test
  public void testTrailingHiddenEntriesYieldNoToken() {
    List<String> names = names(10);
    Predicate<String> canSee = n -> n.compareTo("n03") < 0;
    ListPager.Result<String> p1 = page(names, canSee, 3, null);
    Assertions.assertEquals(Arrays.asList("n00", "n01", "n02"), p1.items());
    Assertions.assertNull(p1.nextPageToken(), "nothing visible remains after n02");
  }

  @Test
  public void testAllHiddenReturnsEmptyPageWithoutToken() {
    ListPager.Result<String> page = page(names(50), n -> false, 5, null);
    Assertions.assertTrue(page.items().isEmpty());
    Assertions.assertNull(page.nextPageToken());
  }

  @Test
  public void testAuthorizerSeesOnlyChunksAndCallsGrowWhenMostAreHidden() {
    List<String> names = names(10_000);
    AtomicInteger calls = new AtomicInteger();
    AtomicInteger checked = new AtomicInteger();
    Function<List<String>, List<String>> visible =
        chunk -> {
          calls.incrementAndGet();
          checked.addAndGet(chunk.size());
          return chunk.stream().filter(n -> n.endsWith("999")).collect(Collectors.toList());
        };
    ListPager.Result<String> page =
        ListPager.page(names, Function.identity(), visible, 2, null, SCOPE, MAX);
    Assertions.assertEquals(Arrays.asList("n0999", "n1999"), page.items());
    Assertions.assertNotNull(page.nextPageToken());
    // Doubling chunks: 3, 6, 12, ..., capped at MAX. Far fewer calls than entries.
    Assertions.assertTrue(calls.get() < 20, "authorizer calls: " + calls.get());
    Assertions.assertTrue(checked.get() < names.size(), "checked: " + checked.get());
  }

  @Test
  public void testAuthorizerNotCalledBeyondFirstChunkWhenAllVisible() {
    AtomicInteger checked = new AtomicInteger();
    Function<List<String>, List<String>> visible =
        chunk -> {
          checked.addAndGet(chunk.size());
          return chunk;
        };
    ListPager.page(names(10_000), Function.identity(), visible, 10, null, SCOPE, MAX);
    Assertions.assertEquals(11, checked.get());
  }

  @Test
  public void testPageSizeClampedToMaximum() {
    ListPager.Result<String> page =
        ListPager.page(names(10), Function.identity(), c -> c, 50, null, SCOPE, 4);
    Assertions.assertEquals(4, page.items().size());
    Assertions.assertNotNull(page.nextPageToken());
  }

  @Test
  public void testTokenWithoutPageSizeUsesMaximum() {
    ListPager.Result<String> first =
        ListPager.page(names(10), Function.identity(), c -> c, 2, null, SCOPE, 4);
    ListPager.Result<String> next =
        ListPager.page(
            names(10), Function.identity(), c -> c, null, first.nextPageToken(), SCOPE, 4);
    Assertions.assertEquals(Arrays.asList("n02", "n03", "n04", "n05"), next.items());
  }

  @Test
  public void testRejectsPageSizeBelowOne() {
    for (int size : new int[] {0, -1, Integer.MIN_VALUE}) {
      IllegalArgumentException e =
          Assertions.assertThrows(
              IllegalArgumentException.class, () -> page(names(3), n -> true, size, null));
      Assertions.assertTrue(e.getMessage().contains("pageSize"), e.getMessage());
    }
  }

  @Test
  public void testRejectsTokenFromAnotherScope() {
    String token = PageToken.encode("n01", PageToken.scopeOf("other"));
    IllegalArgumentException e =
        Assertions.assertThrows(
            IllegalArgumentException.class, () -> page(names(3), n -> true, 2, token));
    Assertions.assertTrue(e.getMessage().contains("pageToken"), e.getMessage());
  }

  @Test
  public void testOrderIsBinaryNotLocale() {
    List<String> names = Arrays.asList("b", "B", "a", "A", "é", "e");
    ListPager.Result<String> page = page(names, n -> true, 10, null);
    Assertions.assertEquals(Arrays.asList("A", "B", "a", "b", "e", "é"), page.items());
  }

  @Test
  public void testCreatesAndDropsBetweenPages() {
    // Start with n00..n19, walk in pages of 4, and change the list between page requests.
    List<String> current = new ArrayList<>(names(20));
    List<String> walked = new ArrayList<>();

    ListPager.Result<String> page = page(current, n -> true, 4, null);
    walked.addAll(page.items()); // n00..n03

    current.remove("n01"); // already returned
    current.remove("n10"); // not yet reached
    current.add("n025"); // sorts before the cursor (n03)
    current.add("n105"); // sorts after the cursor
    while (page.nextPageToken() != null) {
      page = page(current, n -> true, 4, page.nextPageToken());
      walked.addAll(page.items());
      if (walked.contains("n13")) {
        current.add("n165"); // the page holding n13 ends at n15, so n165 is ahead
        current.remove("n18");
      }
    }

    Assertions.assertEquals(
        walked.size(), new HashSet<>(walked).size(), "no entry returned twice: " + walked);
    for (String survivor : names(20)) {
      if (!survivor.equals("n10") && !survivor.equals("n18")) {
        Assertions.assertTrue(walked.contains(survivor), "missing " + survivor);
      }
    }
    Assertions.assertFalse(walked.contains("n10"), "dropped before reached");
    Assertions.assertFalse(walked.contains("n18"), "dropped before reached");
    Assertions.assertFalse(walked.contains("n025"), "created behind the cursor");
    Assertions.assertTrue(walked.contains("n105"), "created ahead of the cursor");
    Assertions.assertTrue(walked.contains("n165"), "created ahead of the cursor");
    Assertions.assertTrue(walked.contains("n01"), "returned before it was dropped");
  }

  @Test
  public void testConcurrentModificationDuringWalkNeverDuplicates() throws Exception {
    List<String> current = Collections.synchronizedList(new ArrayList<>(names(500)));
    Set<String> stable =
        IntStream.range(0, 500)
            .filter(i -> i % 2 == 0)
            .mapToObj(TestListPager::name)
            .collect(Collectors.toSet());
    Thread mutator =
        new Thread(
            () -> {
              for (int i = 0; i < 2_000; i++) {
                String odd = name(1 + 2 * (i % 250));
                if (!current.remove(odd)) {
                  current.add(odd);
                }
              }
            });
    mutator.start();
    List<String> walked = new ArrayList<>();
    ListPager.Result<String> page = null;
    do {
      List<String> snapshot;
      synchronized (current) {
        snapshot = new ArrayList<>(current);
      }
      page = page(snapshot, n -> true, 7, page == null ? null : page.nextPageToken());
      walked.addAll(page.items());
    } while (page.nextPageToken() != null);
    mutator.join();

    Assertions.assertEquals(walked.size(), new HashSet<>(walked).size(), "no duplicates");
    Assertions.assertTrue(walked.containsAll(stable), "every stable entry returned");
    Assertions.assertEquals(new ArrayList<>(new TreeSet<>(walked)), walked, "sorted");
  }

  private static ListPager.Result<String> page(
      List<String> names, Predicate<String> canSee, Integer pageSize, String token) {
    return ListPager.page(
        names,
        Function.identity(),
        chunk -> chunk.stream().filter(canSee).collect(Collectors.toList()),
        pageSize,
        token,
        SCOPE,
        MAX);
  }

  private static List<String> walk(List<String> names, Predicate<String> canSee, int pageSize) {
    List<String> walked = new ArrayList<>();
    String token = null;
    do {
      ListPager.Result<String> page = page(names, canSee, pageSize, token);
      Assertions.assertTrue(
          page.items().size() == pageSize || page.nextPageToken() == null,
          "only the last page may be short");
      walked.addAll(page.items());
      token = page.nextPageToken();
    } while (token != null);
    return walked;
  }

  private static List<String> names(int count) {
    return IntStream.range(0, count).mapToObj(TestListPager::name).collect(Collectors.toList());
  }

  /** n00..n99 for small lists, then n0100 onward; every name is unique. */
  private static String name(int i) {
    return i < 100 ? String.format("n%02d", i) : String.format("n%04d", i);
  }
}
