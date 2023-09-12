/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.graviton.catalog;

import com.datastrato.graviton.NameIdentifier;
import com.datastrato.graviton.Namespace;
import com.datastrato.graviton.exceptions.IllegalNameIdentifierException;
import com.datastrato.graviton.exceptions.NoSuchCatalogException;
import com.datastrato.graviton.exceptions.NoSuchSchemaException;
import com.datastrato.graviton.exceptions.NoSuchTableException;
import com.datastrato.graviton.exceptions.NonEmptySchemaException;
import com.datastrato.graviton.exceptions.SchemaAlreadyExistsException;
import com.datastrato.graviton.exceptions.TableAlreadyExistsException;
import com.datastrato.graviton.rel.Column;
import com.datastrato.graviton.rel.Distribution;
import com.datastrato.graviton.rel.Schema;
import com.datastrato.graviton.rel.SchemaChange;
import com.datastrato.graviton.rel.SortOrder;
import com.datastrato.graviton.rel.SupportsSchemas;
import com.datastrato.graviton.rel.Table;
import com.datastrato.graviton.rel.TableCatalog;
import com.datastrato.graviton.rel.TableChange;
import com.datastrato.graviton.utils.ThrowableFunction;
import com.google.common.annotations.VisibleForTesting;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * A catalog operation dispatcher that dispatches the catalog operations to the underlying catalog
 * implementation.
 */
public class CatalogOperationDispatcher implements TableCatalog, SupportsSchemas {

  private final CatalogManager catalogManager;

  /**
   * Creates a new CatalogOperationDispatcher instance.
   *
   * @param catalogManager The CatalogManager instance to be used for catalog operations.
   */
  public CatalogOperationDispatcher(CatalogManager catalogManager) {
    this.catalogManager = catalogManager;
  }

  /**
   * Lists the schemas within the specified namespace.
   *
   * @param namespace The namespace in which to list schemas.
   * @return An array of NameIdentifier objects representing the schemas within the specified
   *     namespace.
   * @throws NoSuchCatalogException If the catalog namespace does not exist.
   */
  @Override
  public NameIdentifier[] listSchemas(Namespace namespace) throws NoSuchCatalogException {
    NameIdentifier catalogIdent = NameIdentifier.of(namespace.levels());

    return doWithCatalog(
        catalogIdent,
        c -> c.doWithSchemaOps(s -> s.listSchemas(namespace)),
        NoSuchCatalogException.class);
  }

  /**
   * Creates a new schema.
   *
   * @param ident The identifier for the schema to be created.
   * @param comment The comment for the new schema.
   * @param metadata Additional metadata for the new schema.
   * @return The created Schema object.
   * @throws NoSuchCatalogException If the catalog corresponding to the provided identifier does not
   *     exist.
   * @throws SchemaAlreadyExistsException If a schema with the same identifier already exists.
   */
  @Override
  public Schema createSchema(NameIdentifier ident, String comment, Map<String, String> metadata)
      throws NoSuchCatalogException, SchemaAlreadyExistsException {
    NameIdentifier catalogIdent = NameIdentifier.of(ident.namespace().levels());

    return doWithCatalog(
        catalogIdent,
        c -> c.doWithSchemaOps(s -> s.createSchema(ident, comment, metadata)),
        NoSuchCatalogException.class,
        SchemaAlreadyExistsException.class);
  }

  /**
   * Loads and retrieves a schema.
   *
   * @param ident The identifier of the schema to be loaded.
   * @return The loaded Schema object.
   * @throws NoSuchSchemaException If the schema does not exist.
   */
  @Override
  public Schema loadSchema(NameIdentifier ident) throws NoSuchSchemaException {
    NameIdentifier catalogIdent = NameIdentifier.of(ident.namespace().levels());

    return doWithCatalog(
        catalogIdent,
        c -> c.doWithSchemaOps(s -> s.loadSchema(ident)),
        NoSuchSchemaException.class);
  }

  /**
   * Alters the schema by applying the provided schema changes.
   *
   * @param ident The identifier of the schema to be altered.
   * @param changes The array of SchemaChange objects representing the alterations to apply.
   * @return The altered Schema object.
   * @throws NoSuchSchemaException If the schema corresponding to the provided identifier does not
   *     exist.
   */
  @Override
  public Schema alterSchema(NameIdentifier ident, SchemaChange... changes)
      throws NoSuchSchemaException {
    NameIdentifier catalogIdent = NameIdentifier.of(ident.namespace().levels());

    return doWithCatalog(
        catalogIdent,
        c -> c.doWithSchemaOps(s -> s.alterSchema(ident, changes)),
        NoSuchSchemaException.class);
  }

  /**
   * Drops a schema.
   *
   * @param ident The identifier of the schema to be dropped.
   * @param cascade If true, drops all tables within the schema as well.
   * @return True if the schema was successfully dropped, false otherwise.
   * @throws NonEmptySchemaException If the schema contains tables and cascade is set to false.
   */
  @Override
  public boolean dropSchema(NameIdentifier ident, boolean cascade) throws NonEmptySchemaException {
    NameIdentifier catalogIdent = NameIdentifier.of(ident.namespace().levels());

    return doWithCatalog(
        catalogIdent,
        c -> c.doWithSchemaOps(s -> s.dropSchema(ident, cascade)),
        NonEmptySchemaException.class);
  }

  /**
   * Lists the tables within a schema.
   *
   * @param namespace The namespace of the schema containing the tables.
   * @return An array of {@link NameIdentifier} objects representing the identifiers of the tables
   *     in the schema.
   * @throws NoSuchSchemaException If the specified schema does not exist.
   */
  @Override
  public NameIdentifier[] listTables(Namespace namespace) throws NoSuchSchemaException {
    NameIdentifier catalogIdent = NameIdentifier.of(namespace.levels());

    return doWithCatalog(
        catalogIdent,
        c -> c.doWithTableOps(t -> t.listTables(namespace)),
        NoSuchSchemaException.class);
  }

  /**
   * Loads a table.
   *
   * @param ident The identifier of the table to load.
   * @return The loaded {@link Table} object representing the requested table.
   * @throws NoSuchTableException If the specified table does not exist.
   */
  @Override
  public Table loadTable(NameIdentifier ident) throws NoSuchTableException {
    NameIdentifier catalogIdent = NameIdentifier.of(ident.namespace().levels());

    return doWithCatalog(
        catalogIdent, c -> c.doWithTableOps(t -> t.loadTable(ident)), NoSuchTableException.class);
  }

  /**
   * Creates a new table in a schema.
   *
   * @param ident The identifier of the table to create.
   * @param columns An array of {@link Column} objects representing the columns of the table.
   * @param comment A description or comment associated with the table.
   * @param properties Additional properties to set for the table.
   * @return The newly created {@link Table} object.
   * @throws NoSuchSchemaException If the schema in which to create the table does not exist.
   * @throws TableAlreadyExistsException If a table with the same name already exists in the schema.
   */
  @Override
  public Table createTable(
      NameIdentifier ident,
      Column[] columns,
      String comment,
      Map<String, String> properties,
      Distribution distribution,
      SortOrder[] sortOrders)
      throws NoSuchSchemaException, TableAlreadyExistsException {
    NameIdentifier catalogIdent = NameIdentifier.of(ident.namespace().levels());

    return doWithCatalog(
        catalogIdent,
        c ->
            c.doWithTableOps(
                t -> t.createTable(ident, columns, comment, properties, distribution, sortOrders)),
        NoSuchSchemaException.class,
        TableAlreadyExistsException.class);
  }

  /**
   * Alters an existing table.
   *
   * @param ident The identifier of the table to alter.
   * @param changes An array of {@link TableChange} objects representing the changes to apply to the
   *     table.
   * @return The altered {@link Table} object after applying the changes.
   * @throws NoSuchTableException If the table to alter does not exist.
   * @throws IllegalArgumentException If an unsupported or invalid change is specified.
   */
  @Override
  public Table alterTable(NameIdentifier ident, TableChange... changes)
      throws NoSuchTableException, IllegalArgumentException {
    NameIdentifier catalogIdent = NameIdentifier.of(ident.namespace().levels());

    return doWithCatalog(
        catalogIdent,
        c -> c.doWithTableOps(t -> t.alterTable(ident, changes)),
        NoSuchTableException.class,
        IllegalArgumentException.class);
  }

  /**
   * Drops a table from the catalog.
   *
   * @param ident The identifier of the table to drop.
   * @return {@code true} if the table was successfully dropped, {@code false} if the table does not
   *     exist.
   * @throws NoSuchTableException If the table to drop does not exist.
   */
  @Override
  public boolean dropTable(NameIdentifier ident) {
    NameIdentifier catalogIdent = NameIdentifier.of(ident.namespace().levels());

    return doWithCatalog(
        catalogIdent, c -> c.doWithTableOps(t -> t.dropTable(ident)), NoSuchTableException.class);
  }

  private <R, E extends Throwable> R doWithCatalog(
      NameIdentifier ident, ThrowableFunction<CatalogManager.CatalogWrapper, R> fn, Class<E> ex)
      throws E {
    try {
      NameIdentifier catalogIdent = getCatalogIdentifier(ident);
      CatalogManager.CatalogWrapper c = catalogManager.loadCatalogAndWrap(catalogIdent);
      return fn.apply(c);
    } catch (Throwable throwable) {
      if (ex.isInstance(throwable)) {
        throw ex.cast(throwable);
      }
      throw new RuntimeException(throwable);
    }
  }

  private <R, E1 extends Throwable, E2 extends Throwable> R doWithCatalog(
      NameIdentifier ident,
      ThrowableFunction<CatalogManager.CatalogWrapper, R> fn,
      Class<E1> ex1,
      Class<E2> ex2)
      throws E1, E2 {
    try {
      NameIdentifier catalogIdent = getCatalogIdentifier(ident);

      CatalogManager.CatalogWrapper c = catalogManager.loadCatalogAndWrap(catalogIdent);
      return fn.apply(c);
    } catch (Throwable throwable) {
      if (ex1.isInstance(throwable)) {
        throw ex1.cast(throwable);
      } else if (ex2.isInstance(throwable)) {
        throw ex2.cast(throwable);
      }

      throw new RuntimeException(throwable);
    }
  }

  @VisibleForTesting
  // TODO(xun): Remove this method when we implement a better way to get the catalog identifier
  //  [#257] Add an explicit get catalog functions in NameIdentifier
  NameIdentifier getCatalogIdentifier(NameIdentifier ident) {
    NameIdentifier.check(
        ident.name() != null, "The name variable in the NameIdentifier must have value.");
    Namespace.check(
        ident.namespace() != null && ident.namespace().length() > 0,
        String.format(
            "Catalog namespace must be non-null and have 1 level, the input namespace is %s",
            ident.namespace()));

    List<String> allElems =
        Stream.concat(Arrays.stream(ident.namespace().levels()), Stream.of(ident.name()))
            .collect(Collectors.toList());
    if (allElems.size() < 2) {
      throw new IllegalNameIdentifierException(
          "Cannot create a catalog NameIdentifier less than two elements.");
    }
    return NameIdentifier.of(allElems.get(0), allElems.get(1));
  }
}
