/*
 *  Copyright 2023 Datastrato Pvt Ltd.
 *  This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.catalog.lakehouse.iceberg.combine;

import java.lang.reflect.Method;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.iceberg.BaseMetastoreCatalog;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.NamespaceNotEmptyException;
import org.apache.iceberg.exceptions.NoSuchNamespaceException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CombineCatalog extends BaseMetastoreCatalog implements SupportsNamespaces {
  private static final Logger LOG = LoggerFactory.getLogger(CombineCatalog.class);

  public static final String PRIMARY = "primary";
  public static final String SECONDARY = "secondary";
  private final BaseMetastoreCatalog primaryCatalog;
  private final BaseMetastoreCatalog secondaryCatalog;

  public CombineCatalog(BaseMetastoreCatalog primaryCatalog, BaseMetastoreCatalog secondryCatalog) {
    this.primaryCatalog = primaryCatalog;
    this.secondaryCatalog = secondryCatalog;
    if (primaryCatalog instanceof SupportsNamespaces == false) {
      throw new RuntimeException(primaryCatalog.name() + " should support namespace operation");
    }
    if (secondaryCatalog instanceof SupportsNamespaces == false) {
      throw new RuntimeException(secondryCatalog.name() + " should support namespace operation");
    }
  }

  @Override
  public TableOperations newTableOps(TableIdentifier tableIdentifier) {
    TableOperations primaryTableOps = getTableOps(primaryCatalog, tableIdentifier);
    TableOperations secondaryTableOps = getTableOps(secondaryCatalog, tableIdentifier);
    return new CombinedTableOperations(primaryTableOps, secondaryTableOps);
  }

  @Override
  protected String defaultWarehouseLocation(TableIdentifier tableIdentifier) {
    return getDefaultWarehouseLocation(primaryCatalog, tableIdentifier);
  }

  /** Return all the tables in primary catalog. */
  @Override
  public List<TableIdentifier> listTables(Namespace namespace) {
    return primaryCatalog.listTables(namespace);
  }

  @Override
  public boolean dropTable(TableIdentifier identifier, boolean purge) {
    boolean succ = primaryCatalog.dropTable(identifier, purge);
    if (succ == false) {
      return false;
    }
    Utils.doSecondaryCatalogAction(() -> secondaryCatalog.dropTable(identifier, purge));
    return true;
  }

  @Override
  public void renameTable(TableIdentifier from, TableIdentifier to) {
    primaryCatalog.renameTable(from, to);
    Utils.doSecondaryCatalogAction(() -> secondaryCatalog.renameTable(from, to));
  }

  @Override
  public void createNamespace(Namespace namespace, Map<String, String> metadata) {
    ((SupportsNamespaces) primaryCatalog).createNamespace(namespace, metadata);
    Utils.doSecondaryCatalogAction(
        () -> ((SupportsNamespaces) secondaryCatalog).createNamespace(namespace, metadata));
  }

  @Override
  public List<Namespace> listNamespaces(Namespace namespace) throws NoSuchNamespaceException {
    return ((SupportsNamespaces) primaryCatalog).listNamespaces(namespace);
  }

  @Override
  public Map<String, String> loadNamespaceMetadata(Namespace namespace)
      throws NoSuchNamespaceException {
    return ((SupportsNamespaces) primaryCatalog).loadNamespaceMetadata(namespace);
  }

  @Override
  public boolean dropNamespace(Namespace namespace) throws NamespaceNotEmptyException {
    boolean succ = ((SupportsNamespaces) primaryCatalog).dropNamespace(namespace);
    if (succ == false) {
      return false;
    }
    Utils.doSecondaryCatalogAction(
        () -> ((SupportsNamespaces) secondaryCatalog).dropNamespace(namespace));
    return true;
  }

  @Override
  public boolean setProperties(Namespace namespace, Map<String, String> properties)
      throws NoSuchNamespaceException {
    boolean succ = ((SupportsNamespaces) primaryCatalog).setProperties(namespace, properties);
    if (succ == false) {
      return false;
    }
    Utils.doSecondaryCatalogAction(
        () -> ((SupportsNamespaces) secondaryCatalog).setProperties(namespace, properties));
    return true;
  }

  @Override
  public boolean removeProperties(Namespace namespace, Set<String> properties)
      throws NoSuchNamespaceException {
    boolean succ = ((SupportsNamespaces) primaryCatalog).removeProperties(namespace, properties);
    if (succ == false) {
      return false;
    }
    Utils.doSecondaryCatalogAction(
        () -> ((SupportsNamespaces) secondaryCatalog).removeProperties(namespace, properties));
    return true;
  }

  /** newTableOps is a protected interface, use java reflect to skip the limitation */
  private TableOperations getTableOps(
      BaseMetastoreCatalog baseMetastoreCatalog, TableIdentifier tableIdentifier) {
    try {
      Class metaStoreCatalogClass = baseMetastoreCatalog.getClass();
      Method newTableOps =
          metaStoreCatalogClass.getDeclaredMethod("newTableOps", TableIdentifier.class);
      newTableOps.setAccessible(true);
      Object o = newTableOps.invoke(baseMetastoreCatalog, tableIdentifier);
      return (TableOperations) o;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * getDefaultWarehouseLocation is a protected interface, use java reflect to skip the limitation
   */
  private String getDefaultWarehouseLocation(
      BaseMetastoreCatalog baseMetastoreCatalog, TableIdentifier tableIdentifier) {
    try {
      Class metaStoreCatalogClass = baseMetastoreCatalog.getClass();
      Method newTableOps =
          metaStoreCatalogClass.getDeclaredMethod(
              "defaultWarehouseLocation", TableIdentifier.class);
      newTableOps.setAccessible(true);
      Object o = newTableOps.invoke(baseMetastoreCatalog, tableIdentifier);
      return (String) o;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
