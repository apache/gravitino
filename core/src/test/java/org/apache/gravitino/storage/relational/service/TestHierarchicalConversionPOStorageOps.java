/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.gravitino.storage.relational.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.function.UnaryOperator;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.utils.HierarchicalSchemaUtil;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

public class TestHierarchicalConversionPOStorageOps {

  private static final String SEP = HierarchicalSchemaUtil.schemaSeparator();
  private static final String PHYS = HierarchicalSchemaUtil.physicalSeparator();

  private BasePOStorageOps<String, Object> delegate;
  private Object mapper;

  @BeforeEach
  @SuppressWarnings("unchecked")
  public void setUp() {
    delegate = mock(BasePOStorageOps.class);
    mapper = new Object();
  }

  // ---------- Input name conversion ----------

  @Test
  public void getPOByParentIdConvertsHierarchicalName() {
    when(delegate.getPO(eq(mapper), eq(7L), eq("ns_a" + PHYS + "ns_b"))).thenReturn("found");
    HierarchicalConversionPOStorageOps<String, Object> wrapper =
        new HierarchicalConversionPOStorageOps<>(delegate);

    String result = wrapper.getPO(mapper, 7L, "ns_a" + SEP + "ns_b");

    assertEquals("found", result);
    verify(delegate).getPO(mapper, 7L, "ns_a" + PHYS + "ns_b");
  }

  @Test
  public void getPOByParentIdLeavesSimpleNameUnchanged() {
    when(delegate.getPO(eq(mapper), eq(7L), eq("plain"))).thenReturn("po");
    HierarchicalConversionPOStorageOps<String, Object> wrapper =
        new HierarchicalConversionPOStorageOps<>(delegate);

    String result = wrapper.getPO(mapper, 7L, "plain");

    assertEquals("po", result);
    verify(delegate).getPO(mapper, 7L, "plain");
  }

  @Test
  public void getPOByParentIdReturnsNullWhenDelegateMisses() {
    when(delegate.getPO(any(), any(Long.class), any(String.class))).thenReturn(null);
    HierarchicalConversionPOStorageOps<String, Object> wrapper =
        new HierarchicalConversionPOStorageOps<>(
            delegate, s -> s + "-rewritten", UnaryOperator.identity());

    assertNull(wrapper.getPO(mapper, 1L, "missing"));
  }

  @Test
  public void listPOsByNamespaceAndNamesConvertsEachHierarchicalName() {
    List<String> names = Arrays.asList("plain", "ns_a" + SEP + "ns_b", "other");
    Namespace ns = Namespace.of("ml", "cat");
    when(delegate.listPOs(eq(mapper), eq(ns), any(List.class)))
        .thenReturn(Collections.singletonList("po"));

    HierarchicalConversionPOStorageOps<String, Object> wrapper =
        new HierarchicalConversionPOStorageOps<>(delegate);

    wrapper.listPOs(mapper, ns, names);

    ArgumentCaptor<List<String>> namesCaptor = ArgumentCaptor.forClass(List.class);
    verify(delegate).listPOs(eq(mapper), eq(ns), namesCaptor.capture());
    assertEquals(Arrays.asList("plain", "ns_a" + PHYS + "ns_b", "other"), namesCaptor.getValue());
  }

  // ---------- Identifier / namespace logical → physical translation ----------

  @Test
  public void getPOByFullNameConvertsSchemaIdentifierName() {
    NameIdentifier ident = NameIdentifier.of(Namespace.of("ml", "cat"), "ns_a" + SEP + "ns_b");
    when(delegate.getPOByFullName(any(), any(NameIdentifier.class))).thenReturn("po");

    HierarchicalConversionPOStorageOps<String, Object> wrapper =
        new HierarchicalConversionPOStorageOps<>(delegate);

    wrapper.getPOByFullName(mapper, ident);

    ArgumentCaptor<NameIdentifier> captor = ArgumentCaptor.forClass(NameIdentifier.class);
    verify(delegate).getPOByFullName(eq(mapper), captor.capture());
    NameIdentifier converted = captor.getValue();
    assertEquals("ns_a" + PHYS + "ns_b", converted.name());
    assertEquals(Namespace.of("ml", "cat"), converted.namespace());
  }

  @Test
  public void getPOByFullNameConvertsSchemaSegmentForChildIdentifier() {
    NameIdentifier ident =
        NameIdentifier.of(Namespace.of("ml", "cat", "ns_a" + SEP + "ns_b"), "tbl");
    when(delegate.getPOByFullName(any(), any(NameIdentifier.class))).thenReturn("po");

    HierarchicalConversionPOStorageOps<String, Object> wrapper =
        new HierarchicalConversionPOStorageOps<>(delegate);

    wrapper.getPOByFullName(mapper, ident);

    ArgumentCaptor<NameIdentifier> captor = ArgumentCaptor.forClass(NameIdentifier.class);
    verify(delegate).getPOByFullName(eq(mapper), captor.capture());
    NameIdentifier converted = captor.getValue();
    assertEquals("tbl", converted.name());
    assertEquals(Namespace.of("ml", "cat", "ns_a" + PHYS + "ns_b"), converted.namespace());
  }

  @Test
  public void getPOByFullNamePassesThroughWhenNoSeparatorPresent() {
    NameIdentifier ident = NameIdentifier.of(Namespace.of("ml", "cat", "schema"), "tbl");
    when(delegate.getPOByFullName(any(), any(NameIdentifier.class))).thenReturn("po");

    HierarchicalConversionPOStorageOps<String, Object> wrapper =
        new HierarchicalConversionPOStorageOps<>(delegate);

    wrapper.getPOByFullName(mapper, ident);

    verify(delegate).getPOByFullName(mapper, ident);
  }

  @Test
  public void listPOsByNSFullNameConvertsSchemaSegment() {
    Namespace ns = Namespace.of("ml", "cat", "ns_a" + SEP + "ns_b");
    when(delegate.listPOsByNSFullName(any(), any(Namespace.class)))
        .thenReturn(Collections.singletonList("po"));

    HierarchicalConversionPOStorageOps<String, Object> wrapper =
        new HierarchicalConversionPOStorageOps<>(delegate);

    wrapper.listPOsByNSFullName(mapper, ns);

    ArgumentCaptor<Namespace> nsCaptor = ArgumentCaptor.forClass(Namespace.class);
    verify(delegate).listPOsByNSFullName(eq(mapper), nsCaptor.capture());
    assertEquals(Namespace.of("ml", "cat", "ns_a" + PHYS + "ns_b"), nsCaptor.getValue());
  }

  @Test
  public void listPOsByNSFullNameLeavesShortNamespaceUnchanged() {
    Namespace ns = Namespace.of("ml", "cat");
    when(delegate.listPOsByNSFullName(any(), any(Namespace.class)))
        .thenReturn(Collections.emptyList());

    HierarchicalConversionPOStorageOps<String, Object> wrapper =
        new HierarchicalConversionPOStorageOps<>(delegate);

    wrapper.listPOsByNSFullName(mapper, ns);

    verify(delegate).listPOsByNSFullName(mapper, ns);
  }

  // ---------- physicalToLogicalRewriter (read path) ----------

  @Test
  public void physicalToLogicalRewriterAppliedToGetPOByParentId() {
    when(delegate.getPO(any(), any(Long.class), any(String.class))).thenReturn("raw");
    HierarchicalConversionPOStorageOps<String, Object> wrapper =
        new HierarchicalConversionPOStorageOps<>(
            delegate, s -> s + "-rewritten", UnaryOperator.identity());

    assertEquals("raw-rewritten", wrapper.getPO(mapper, 1L, "plain"));
  }

  @Test
  public void physicalToLogicalRewriterAppliedToListPOsByParentId() {
    when(delegate.listPOs(any(), any(Long.class))).thenReturn(Arrays.asList("a", "b"));
    HierarchicalConversionPOStorageOps<String, Object> wrapper =
        new HierarchicalConversionPOStorageOps<>(
            delegate, s -> s.toUpperCase(), UnaryOperator.identity());

    assertEquals(Arrays.asList("A", "B"), wrapper.listPOs(mapper, 1L));
  }

  @Test
  public void physicalToLogicalRewriterAppliedToListPOsByIds() {
    when(delegate.listPOs(any(), any(List.class))).thenReturn(Arrays.asList("x", "y"));
    HierarchicalConversionPOStorageOps<String, Object> wrapper =
        new HierarchicalConversionPOStorageOps<>(delegate, s -> s + "!", UnaryOperator.identity());

    List<Long> ids = Arrays.asList(1L, 2L);
    assertEquals(Arrays.asList("x!", "y!"), wrapper.listPOs(mapper, ids));
  }

  @Test
  public void physicalToLogicalRewriterIsNotInvokedOnNullResult() {
    when(delegate.getPO(any(), any(Long.class), any(String.class))).thenReturn(null);
    HierarchicalConversionPOStorageOps<String, Object> wrapper =
        new HierarchicalConversionPOStorageOps<>(
            delegate,
            s -> {
              throw new AssertionError("rewriter must not be invoked on null");
            },
            UnaryOperator.identity());

    assertNull(wrapper.getPO(mapper, 1L, "x"));
  }

  @Test
  public void physicalToLogicalRewriterIsNotInvokedOnEmptyList() {
    when(delegate.listPOs(any(), any(Long.class))).thenReturn(Collections.emptyList());
    HierarchicalConversionPOStorageOps<String, Object> wrapper =
        new HierarchicalConversionPOStorageOps<>(
            delegate,
            s -> {
              throw new AssertionError("rewriter must not be invoked on empty list");
            },
            UnaryOperator.identity());

    assertEquals(Collections.emptyList(), wrapper.listPOs(mapper, 1L));
  }

  // ---------- logicalToPhysicalRewriter (write path) ----------

  @Test
  public void logicalToPhysicalRewriterAppliedToInsertPO() {
    HierarchicalConversionPOStorageOps<String, Object> wrapper =
        new HierarchicalConversionPOStorageOps<>(
            delegate, UnaryOperator.identity(), s -> s + "-written");

    wrapper.insertPO(mapper, "logical", true);

    verify(delegate).insertPO(mapper, "logical-written", true);
  }

  @Test
  public void logicalToPhysicalRewriterAppliedToBatchInsertPOs() {
    HierarchicalConversionPOStorageOps<String, Object> wrapper =
        new HierarchicalConversionPOStorageOps<>(delegate, UnaryOperator.identity(), s -> s + "-W");

    wrapper.batchInsertPOs(mapper, Arrays.asList("a", "b"), false);

    ArgumentCaptor<List<String>> captor = ArgumentCaptor.forClass(List.class);
    verify(delegate).batchInsertPOs(eq(mapper), captor.capture(), eq(false));
    assertEquals(Arrays.asList("a-W", "b-W"), captor.getValue());
  }

  @Test
  public void logicalToPhysicalRewriterAppliedToBothPOsInUpdate() {
    when(delegate.updatePO(any(), eq("new-W"), eq("old-W"))).thenReturn(1);
    HierarchicalConversionPOStorageOps<String, Object> wrapper =
        new HierarchicalConversionPOStorageOps<>(delegate, UnaryOperator.identity(), s -> s + "-W");

    assertEquals(1, wrapper.updatePO(mapper, "new", "old"));
    verify(delegate).updatePO(mapper, "new-W", "old-W");
  }

  @Test
  public void singleArgConstructorUsesIdentityRewritersForBothDirections() {
    when(delegate.getPO(any(), any(Long.class), any(String.class))).thenReturn("po");
    HierarchicalConversionPOStorageOps<String, Object> wrapper =
        new HierarchicalConversionPOStorageOps<>(delegate);

    assertEquals("po", wrapper.getPO(mapper, 1L, "plain"));
    wrapper.insertPO(mapper, "raw", false);
    verify(delegate).insertPO(mapper, "raw", false);

    wrapper.batchInsertPOs(mapper, Arrays.asList("a", "b"), true);
    ArgumentCaptor<List<String>> captor = ArgumentCaptor.forClass(List.class);
    verify(delegate).batchInsertPOs(eq(mapper), captor.capture(), eq(true));
    assertEquals(Arrays.asList("a", "b"), captor.getValue());
  }

  // ---------- Delegation ----------

  @Test
  public void supportsParentIdRelationalReadAndEntityTypeDelegated() {
    when(delegate.supportsParentIdRelationalRead()).thenReturn(true);

    HierarchicalConversionPOStorageOps<String, Object> wrapper =
        new HierarchicalConversionPOStorageOps<>(delegate);

    assertTrue(wrapper.supportsParentIdRelationalRead());
  }

  @Test
  public void batchInsertEmptyListIsForwarded() {
    HierarchicalConversionPOStorageOps<String, Object> wrapper =
        new HierarchicalConversionPOStorageOps<>(
            delegate,
            UnaryOperator.identity(),
            s -> {
              throw new AssertionError("rewriter must not be invoked on empty list");
            });

    wrapper.batchInsertPOs(mapper, new ArrayList<>(), false);

    verify(delegate).batchInsertPOs(eq(mapper), eq(Collections.emptyList()), eq(false));
  }
}
