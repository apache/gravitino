package com.datastrato.graviton;

import com.datastrato.graviton.catalog.CatalogOperations;
import com.datastrato.graviton.exceptions.NoSuchSchemaException;
import com.datastrato.graviton.exceptions.NoSuchTableException;
import com.datastrato.graviton.exceptions.TableAlreadyExistsException;
import com.datastrato.graviton.meta.AuditInfo;
import com.datastrato.graviton.rel.Column;
import com.datastrato.graviton.rel.Table;
import com.datastrato.graviton.rel.TableCatalog;
import com.datastrato.graviton.rel.TableChange;
import com.google.common.collect.Maps;
import java.io.IOException;
import java.time.Instant;
import java.util.Map;

public class TestCatalogOperations implements CatalogOperations, TableCatalog {

  private final Map<NameIdentifier, TestTable> tables;

  public TestCatalogOperations() {
    tables = Maps.newHashMap();
  }

  @Override
  public void initialize(Map<String, String> config) throws RuntimeException {}

  @Override
  public void close() throws IOException {}

  @Override
  public NameIdentifier[] listTables(Namespace namespace) throws NoSuchSchemaException {
    return tables.keySet().stream()
        .filter(testTable -> testTable.namespace().equals(namespace))
        .toArray(NameIdentifier[]::new);
  }

  @Override
  public Table loadTable(NameIdentifier ident) throws NoSuchTableException {
    if (tables.containsKey(ident)) {
      return tables.get(ident);
    } else {
      throw new NoSuchTableException("Table " + ident + " does not exist");
    }
  }

  @Override
  public Table createTable(
      NameIdentifier ident, Column[] columns, String comment, Map<String, String> properties)
      throws NoSuchSchemaException, TableAlreadyExistsException {
    AuditInfo auditInfo =
        new AuditInfo.Builder().withCreator("test").withCreateTime(Instant.now()).build();

    TestTable table =
        new TestTable(ident.name(), ident.namespace(), comment, properties, auditInfo, columns);
    if (tables.containsKey(ident)) {
      throw new TableAlreadyExistsException("Table " + ident + " already exists");
    } else {
      tables.put(ident, table);
    }

    return table;
  }

  @Override
  public Table alterTable(NameIdentifier ident, TableChange... changes)
      throws NoSuchTableException, IllegalArgumentException {
    throw new UnsupportedOperationException("unsupported");
  }

  @Override
  public boolean dropTable(NameIdentifier ident) {
    if (tables.containsKey(ident)) {
      tables.remove(ident);
      return true;
    } else {
      return false;
    }
  }
}
