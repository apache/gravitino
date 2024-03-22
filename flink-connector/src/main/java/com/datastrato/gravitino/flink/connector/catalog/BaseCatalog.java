/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.flink.connector.catalog;

import java.util.List;
import org.apache.flink.table.catalog.AbstractCatalog;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.CatalogDatabase;
import org.apache.flink.table.catalog.CatalogFunction;
import org.apache.flink.table.catalog.CatalogPartition;
import org.apache.flink.table.catalog.CatalogPartitionSpec;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.exceptions.CatalogException;
import org.apache.flink.table.catalog.exceptions.DatabaseAlreadyExistException;
import org.apache.flink.table.catalog.exceptions.DatabaseNotEmptyException;
import org.apache.flink.table.catalog.exceptions.DatabaseNotExistException;
import org.apache.flink.table.catalog.exceptions.FunctionAlreadyExistException;
import org.apache.flink.table.catalog.exceptions.FunctionNotExistException;
import org.apache.flink.table.catalog.exceptions.PartitionAlreadyExistsException;
import org.apache.flink.table.catalog.exceptions.PartitionNotExistException;
import org.apache.flink.table.catalog.exceptions.PartitionSpecInvalidException;
import org.apache.flink.table.catalog.exceptions.TableAlreadyExistException;
import org.apache.flink.table.catalog.exceptions.TableNotExistException;
import org.apache.flink.table.catalog.exceptions.TableNotPartitionedException;
import org.apache.flink.table.catalog.exceptions.TablePartitionedException;
import org.apache.flink.table.catalog.stats.CatalogColumnStatistics;
import org.apache.flink.table.catalog.stats.CatalogTableStatistics;
import org.apache.flink.table.expressions.Expression;

/**
 * The BaseCatalog that provides a default implementation for all methods in the {@link
 * org.apache.flink.table.catalog.Catalog} interface.
 */
public abstract class BaseCatalog extends AbstractCatalog {
  protected BaseCatalog(String catalogName, String defaultDatabase) {
    super(catalogName, defaultDatabase);
  }

  @Override
  public void open() throws CatalogException {}

  @Override
  public void close() throws CatalogException {}

  @Override
  public List<String> listDatabases() throws CatalogException {
    throw new UnsupportedOperationException();
  }

  @Override
  public CatalogDatabase getDatabase(String s) throws DatabaseNotExistException, CatalogException {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean databaseExists(String s) throws CatalogException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void createDatabase(String s, CatalogDatabase catalogDatabase, boolean b)
      throws DatabaseAlreadyExistException, CatalogException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void dropDatabase(String s, boolean b, boolean b1)
      throws DatabaseNotExistException, DatabaseNotEmptyException, CatalogException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void alterDatabase(String s, CatalogDatabase catalogDatabase, boolean b)
      throws DatabaseNotExistException, CatalogException {
    throw new UnsupportedOperationException();
  }

  @Override
  public List<String> listTables(String s) throws DatabaseNotExistException, CatalogException {
    throw new UnsupportedOperationException();
  }

  @Override
  public List<String> listViews(String s) throws DatabaseNotExistException, CatalogException {
    throw new UnsupportedOperationException();
  }

  @Override
  public CatalogBaseTable getTable(ObjectPath objectPath)
      throws TableNotExistException, CatalogException {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean tableExists(ObjectPath objectPath) throws CatalogException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void dropTable(ObjectPath objectPath, boolean b)
      throws TableNotExistException, CatalogException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void renameTable(ObjectPath objectPath, String s, boolean b)
      throws TableNotExistException, TableAlreadyExistException, CatalogException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void createTable(ObjectPath objectPath, CatalogBaseTable catalogBaseTable, boolean b)
      throws TableAlreadyExistException, DatabaseNotExistException, CatalogException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void alterTable(ObjectPath objectPath, CatalogBaseTable catalogBaseTable, boolean b)
      throws TableNotExistException, CatalogException {
    throw new UnsupportedOperationException();
  }

  @Override
  public List<CatalogPartitionSpec> listPartitions(ObjectPath objectPath)
      throws TableNotExistException, TableNotPartitionedException, CatalogException {
    throw new UnsupportedOperationException();
  }

  @Override
  public List<CatalogPartitionSpec> listPartitions(
      ObjectPath objectPath, CatalogPartitionSpec catalogPartitionSpec)
      throws TableNotExistException, TableNotPartitionedException, PartitionSpecInvalidException,
          CatalogException {
    throw new UnsupportedOperationException();
  }

  @Override
  public List<CatalogPartitionSpec> listPartitionsByFilter(
      ObjectPath objectPath, List<Expression> list)
      throws TableNotExistException, TableNotPartitionedException, CatalogException {
    throw new UnsupportedOperationException();
  }

  @Override
  public CatalogPartition getPartition(
      ObjectPath objectPath, CatalogPartitionSpec catalogPartitionSpec)
      throws PartitionNotExistException, CatalogException {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean partitionExists(ObjectPath objectPath, CatalogPartitionSpec catalogPartitionSpec)
      throws CatalogException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void createPartition(
      ObjectPath objectPath,
      CatalogPartitionSpec catalogPartitionSpec,
      CatalogPartition catalogPartition,
      boolean b)
      throws TableNotExistException, TableNotPartitionedException, PartitionSpecInvalidException,
          PartitionAlreadyExistsException, CatalogException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void dropPartition(
      ObjectPath objectPath, CatalogPartitionSpec catalogPartitionSpec, boolean b)
      throws PartitionNotExistException, CatalogException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void alterPartition(
      ObjectPath objectPath,
      CatalogPartitionSpec catalogPartitionSpec,
      CatalogPartition catalogPartition,
      boolean b)
      throws PartitionNotExistException, CatalogException {
    throw new UnsupportedOperationException();
  }

  @Override
  public List<String> listFunctions(String s) throws DatabaseNotExistException, CatalogException {
    throw new UnsupportedOperationException();
  }

  @Override
  public CatalogFunction getFunction(ObjectPath objectPath)
      throws FunctionNotExistException, CatalogException {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean functionExists(ObjectPath objectPath) throws CatalogException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void createFunction(ObjectPath objectPath, CatalogFunction catalogFunction, boolean b)
      throws FunctionAlreadyExistException, DatabaseNotExistException, CatalogException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void alterFunction(ObjectPath objectPath, CatalogFunction catalogFunction, boolean b)
      throws FunctionNotExistException, CatalogException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void dropFunction(ObjectPath objectPath, boolean b)
      throws FunctionNotExistException, CatalogException {
    throw new UnsupportedOperationException();
  }

  @Override
  public CatalogTableStatistics getTableStatistics(ObjectPath objectPath)
      throws TableNotExistException, CatalogException {
    throw new UnsupportedOperationException();
  }

  @Override
  public CatalogColumnStatistics getTableColumnStatistics(ObjectPath objectPath)
      throws TableNotExistException, CatalogException {
    throw new UnsupportedOperationException();
  }

  @Override
  public CatalogTableStatistics getPartitionStatistics(
      ObjectPath objectPath, CatalogPartitionSpec catalogPartitionSpec)
      throws PartitionNotExistException, CatalogException {
    throw new UnsupportedOperationException();
  }

  @Override
  public CatalogColumnStatistics getPartitionColumnStatistics(
      ObjectPath objectPath, CatalogPartitionSpec catalogPartitionSpec)
      throws PartitionNotExistException, CatalogException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void alterTableStatistics(
      ObjectPath objectPath, CatalogTableStatistics catalogTableStatistics, boolean b)
      throws TableNotExistException, CatalogException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void alterTableColumnStatistics(
      ObjectPath objectPath, CatalogColumnStatistics catalogColumnStatistics, boolean b)
      throws TableNotExistException, CatalogException, TablePartitionedException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void alterPartitionStatistics(
      ObjectPath objectPath,
      CatalogPartitionSpec catalogPartitionSpec,
      CatalogTableStatistics catalogTableStatistics,
      boolean b)
      throws PartitionNotExistException, CatalogException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void alterPartitionColumnStatistics(
      ObjectPath objectPath,
      CatalogPartitionSpec catalogPartitionSpec,
      CatalogColumnStatistics catalogColumnStatistics,
      boolean b)
      throws PartitionNotExistException, CatalogException {
    throw new UnsupportedOperationException();
  }
}
