/*
 *  Copyright 2024 Datastrato Pvt Ltd.
 *  This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.listener.api.event;

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.datastrato.gravitino.NameIdentifier;
import com.datastrato.gravitino.Namespace;
import com.datastrato.gravitino.catalog.TableDispatcher;
import com.datastrato.gravitino.exceptions.GravitinoRuntimeException;
import com.datastrato.gravitino.listener.DummyEventListener;
import com.datastrato.gravitino.listener.EventBus;
import com.datastrato.gravitino.listener.TableEventDispatcher;
import com.datastrato.gravitino.listener.api.info.TableInfo;
import com.datastrato.gravitino.rel.Column;
import com.datastrato.gravitino.rel.Table;
import com.datastrato.gravitino.rel.TableChange;
import com.datastrato.gravitino.rel.expressions.NamedReference;
import com.datastrato.gravitino.rel.expressions.distributions.Distribution;
import com.datastrato.gravitino.rel.expressions.distributions.Distributions;
import com.datastrato.gravitino.rel.expressions.distributions.Strategy;
import com.datastrato.gravitino.rel.expressions.sorts.SortOrder;
import com.datastrato.gravitino.rel.expressions.sorts.SortOrders;
import com.datastrato.gravitino.rel.expressions.transforms.Transform;
import com.datastrato.gravitino.rel.expressions.transforms.Transforms;
import com.datastrato.gravitino.rel.indexes.Index;
import com.datastrato.gravitino.rel.indexes.Indexes;
import com.datastrato.gravitino.rel.types.Types;
import com.google.common.collect.ImmutableMap;
import java.util.Arrays;
import java.util.Map;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;

@TestInstance(Lifecycle.PER_CLASS)
public class TestTableEvent {
  private TableEventDispatcher dispatcher;
  private TableEventDispatcher failureDispatcher;
  private DummyEventListener dummyEventListener;
  private Table table;

  @BeforeAll
  void init() {
    this.table = mockTable();
    this.dummyEventListener = new DummyEventListener();
    EventBus eventBus = new EventBus(Arrays.asList(dummyEventListener));
    TableDispatcher tableDispatcher = mockTableDispatcher();
    this.dispatcher = new TableEventDispatcher(eventBus, tableDispatcher);
    TableDispatcher tableExceptionDispatcher = mockExceptionTableDispatcher();
    this.failureDispatcher = new TableEventDispatcher(eventBus, tableExceptionDispatcher);
  }

  @Test
  void testCreateTableEvent() {
    NameIdentifier identifier = NameIdentifier.of("metalake", "catalog", table.name());
    dispatcher.createTable(
        identifier,
        table.columns(),
        table.comment(),
        table.properties(),
        table.partitioning(),
        table.distribution(),
        table.sortOrder(),
        table.index());
    Event event = dummyEventListener.popEvent();
    Assertions.assertEquals(identifier, event.identifier());
    Assertions.assertEquals(CreateTableEvent.class, event.getClass());
    TableInfo tableInfo = ((CreateTableEvent) event).createdTableInfo();
    checkTableInfo(tableInfo, table);
  }

  @Test
  void testLoadTableEvent() {
    NameIdentifier identifier = NameIdentifier.of("metalake", "catalog", table.name());
    dispatcher.loadTable(identifier);
    Event event = dummyEventListener.popEvent();
    Assertions.assertEquals(identifier, event.identifier());
    Assertions.assertEquals(LoadTableEvent.class, event.getClass());
    TableInfo tableInfo = ((LoadTableEvent) event).loadedTableInfo();
    checkTableInfo(tableInfo, table);
  }

  @Test
  void testAlterTableEvent() {
    NameIdentifier identifier = NameIdentifier.of("metalake", "catalog", table.name());
    TableChange change = TableChange.setProperty("a", "b");
    dispatcher.alterTable(identifier, change);
    Event event = dummyEventListener.popEvent();
    Assertions.assertEquals(identifier, event.identifier());
    Assertions.assertEquals(AlterTableEvent.class, event.getClass());
    TableInfo tableInfo = ((AlterTableEvent) event).updatedTableInfo();
    checkTableInfo(tableInfo, table);
    Assertions.assertEquals(1, ((AlterTableEvent) event).tableChanges().length);
    Assertions.assertEquals(change, ((AlterTableEvent) event).tableChanges()[0]);
  }

  @Test
  void testDropTableEvent() {
    NameIdentifier identifier = NameIdentifier.of("metalake", "catalog", table.name());
    dispatcher.dropTable(identifier);
    Event event = dummyEventListener.popEvent();
    Assertions.assertEquals(identifier, event.identifier());
    Assertions.assertEquals(DropTableEvent.class, event.getClass());
    Assertions.assertEquals(true, ((DropTableEvent) event).isExists());
  }

  @Test
  void testPurgeTableEvent() {
    NameIdentifier identifier = NameIdentifier.of("metalake", "catalog", table.name());
    dispatcher.purgeTable(identifier);
    Event event = dummyEventListener.popEvent();
    Assertions.assertEquals(identifier, event.identifier());
    Assertions.assertEquals(PurgeTableEvent.class, event.getClass());
    Assertions.assertEquals(true, ((PurgeTableEvent) event).isExists());
  }

  @Test
  void testListTableEvent() {
    Namespace namespace = Namespace.of("metalake", "catalog");
    dispatcher.listTables(namespace);
    Event event = dummyEventListener.popEvent();
    Assertions.assertEquals(namespace.toString(), event.identifier().toString());
    Assertions.assertEquals(ListTableEvent.class, event.getClass());
    Assertions.assertEquals(namespace, ((ListTableEvent) event).namespace());
  }

  @Test
  void testCreateTableFailureEvent() {
    NameIdentifier identifier = NameIdentifier.of("metalake", "table", table.name());
    Assertions.assertThrowsExactly(
        GravitinoRuntimeException.class,
        () ->
            failureDispatcher.createTable(
                identifier,
                table.columns(),
                table.comment(),
                table.properties(),
                table.partitioning(),
                table.distribution(),
                table.sortOrder(),
                table.index()));
    Event event = dummyEventListener.popEvent();
    Assertions.assertEquals(identifier, event.identifier());
    Assertions.assertEquals(CreateTableFailureEvent.class, event.getClass());
    Assertions.assertEquals(
        GravitinoRuntimeException.class, ((CreateTableFailureEvent) event).exception().getClass());
    checkTableInfo(((CreateTableFailureEvent) event).createTableRequest(), table);
  }

  @Test
  void testLoadTableFailureEvent() {
    NameIdentifier identifier = NameIdentifier.of("metalake", "table", table.name());
    Assertions.assertThrowsExactly(
        GravitinoRuntimeException.class, () -> failureDispatcher.loadTable(identifier));
    Event event = dummyEventListener.popEvent();
    Assertions.assertEquals(identifier, event.identifier());
    Assertions.assertEquals(LoadTableFailureEvent.class, event.getClass());
    Assertions.assertEquals(
        GravitinoRuntimeException.class, ((LoadTableFailureEvent) event).exception().getClass());
  }

  @Test
  void testAlterTableFailureEvent() {
    NameIdentifier identifier = NameIdentifier.of("metalake", "table", table.name());
    TableChange change = TableChange.setProperty("a", "b");
    Assertions.assertThrowsExactly(
        GravitinoRuntimeException.class, () -> failureDispatcher.alterTable(identifier, change));
    Event event = dummyEventListener.popEvent();
    Assertions.assertEquals(identifier, event.identifier());
    Assertions.assertEquals(AlterTableFailureEvent.class, event.getClass());
    Assertions.assertEquals(
        GravitinoRuntimeException.class, ((AlterTableFailureEvent) event).exception().getClass());
    Assertions.assertEquals(1, ((AlterTableFailureEvent) event).tableChanges().length);
    Assertions.assertEquals(change, ((AlterTableFailureEvent) event).tableChanges()[0]);
  }

  @Test
  void testDropTableFailureEvent() {
    NameIdentifier identifier = NameIdentifier.of("metalake", "table", table.name());
    Assertions.assertThrowsExactly(
        GravitinoRuntimeException.class, () -> failureDispatcher.dropTable(identifier));
    Event event = dummyEventListener.popEvent();
    Assertions.assertEquals(identifier, event.identifier());
    Assertions.assertEquals(DropTableFailureEvent.class, event.getClass());
    Assertions.assertEquals(
        GravitinoRuntimeException.class, ((DropTableFailureEvent) event).exception().getClass());
  }

  @Test
  void testPurgeTableFailureEvent() {
    NameIdentifier identifier = NameIdentifier.of("metalake", "table", table.name());
    Assertions.assertThrowsExactly(
        GravitinoRuntimeException.class, () -> failureDispatcher.purgeTable(identifier));
    Event event = dummyEventListener.popEvent();
    Assertions.assertEquals(identifier, event.identifier());
    Assertions.assertEquals(PurgeTableFailureEvent.class, event.getClass());
    Assertions.assertEquals(
        GravitinoRuntimeException.class, ((PurgeTableFailureEvent) event).exception().getClass());
  }

  @Test
  void testListTableFailureEvent() {
    Namespace namespace = Namespace.of("metalake", "catalog");
    Assertions.assertThrowsExactly(
        GravitinoRuntimeException.class, () -> failureDispatcher.listTables(namespace));
    Event event = dummyEventListener.popEvent();
    Assertions.assertEquals(namespace.toString(), event.identifier().toString());
    Assertions.assertEquals(ListTableFailureEvent.class, event.getClass());
    Assertions.assertEquals(
        GravitinoRuntimeException.class, ((ListTableFailureEvent) event).exception().getClass());
    Assertions.assertEquals(namespace, ((ListTableFailureEvent) event).namespace());
  }

  private void checkTableInfo(TableInfo tableInfo, Table table) {
    Assertions.assertEquals(table.name(), tableInfo.name());
    Assertions.assertEquals(table.properties(), tableInfo.properties());
    Assertions.assertEquals(table.comment(), tableInfo.comment());
    Assertions.assertArrayEquals(table.columns(), tableInfo.columns());
    Assertions.assertArrayEquals(table.partitioning(), tableInfo.partitioning());
    Assertions.assertEquals(table.distribution(), tableInfo.distribution());
    Assertions.assertArrayEquals(table.sortOrder(), tableInfo.sortOrder());
    Assertions.assertArrayEquals(table.index(), tableInfo.index());
    Assertions.assertEquals(table.auditInfo(), tableInfo.auditInfo());
  }

  private Table mockTable() {
    Table table = mock(Table.class);
    when(table.name()).thenReturn("table");
    when(table.comment()).thenReturn("comment");
    when(table.properties()).thenReturn(ImmutableMap.of("a", "b"));
    when(table.columns()).thenReturn(new Column[] {Column.of("a", Types.IntegerType.get())});
    when(table.distribution())
        .thenReturn(Distributions.of(Strategy.HASH, 10, NamedReference.field("a")));
    when(table.index())
        .thenReturn(new Index[] {Indexes.primary("p", new String[][] {{"a"}, {"b"}})});
    when(table.sortOrder())
        .thenReturn(new SortOrder[] {SortOrders.ascending(NamedReference.field("a"))});
    when(table.partitioning()).thenReturn(new Transform[] {Transforms.identity("a")});
    when(table.auditInfo()).thenReturn(null);
    return table;
  }

  private TableDispatcher mockTableDispatcher() {
    TableDispatcher dispatcher = mock(TableDispatcher.class);
    when(dispatcher.createTable(
            any(NameIdentifier.class),
            any(Column[].class),
            any(String.class),
            any(Map.class),
            any(Transform[].class),
            any(Distribution.class),
            any(SortOrder[].class),
            any(Index[].class)))
        .thenReturn(table);
    when(dispatcher.loadTable(any(NameIdentifier.class))).thenReturn(table);
    when(dispatcher.dropTable(any(NameIdentifier.class))).thenReturn(true);
    when(dispatcher.purgeTable(any(NameIdentifier.class))).thenReturn(true);
    when(dispatcher.listTables(any(Namespace.class))).thenReturn(null);
    when(dispatcher.alterTable(any(NameIdentifier.class), any(TableChange.class)))
        .thenReturn(table);
    return dispatcher;
  }

  private TableDispatcher mockExceptionTableDispatcher() {
    TableDispatcher dispatcher =
        mock(
            TableDispatcher.class,
            invocation -> {
              throw new GravitinoRuntimeException("Exception for all methods");
            });
    return dispatcher;
  }
}
