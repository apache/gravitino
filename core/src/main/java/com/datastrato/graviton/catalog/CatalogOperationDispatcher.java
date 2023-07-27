/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.graviton.catalog;

import com.datastrato.graviton.NameIdentifier;
import com.datastrato.graviton.Namespace;
import com.datastrato.graviton.exceptions.CatalogAlreadyExistsException;
import com.datastrato.graviton.exceptions.NoSuchCatalogException;
import com.datastrato.graviton.exceptions.NoSuchNamespaceException;
import com.datastrato.graviton.exceptions.NoSuchSchemaException;
import com.datastrato.graviton.exceptions.NoSuchTableException;
import com.datastrato.graviton.exceptions.NonEmptySchemaException;
import com.datastrato.graviton.exceptions.SchemaAlreadyExistsException;
import com.datastrato.graviton.exceptions.TableAlreadyExistsException;
import com.datastrato.graviton.rel.Column;
import com.datastrato.graviton.rel.Schema;
import com.datastrato.graviton.rel.SchemaChange;
import com.datastrato.graviton.rel.SupportsSchemas;
import com.datastrato.graviton.rel.Table;
import com.datastrato.graviton.rel.TableCatalog;
import com.datastrato.graviton.rel.TableChange;
import com.datastrato.graviton.util.ThrowableFunction;
import java.util.Map;

/**
 * A catalog operation dispatcher that dispatches the catalog operations to the underlying catalog
 * implementation.
 */
public class CatalogOperationDispatcher implements TableCatalog, SupportsSchemas {

  private final CatalogManager catalogManager;

  public CatalogOperationDispatcher(CatalogManager catalogManager) {
    this.catalogManager = catalogManager;
  }

  @Override
  public NameIdentifier[] listSchemas(Namespace namespace) throws NoSuchNamespaceException {
    NameIdentifier catalogIdent = NameIdentifier.of(namespace.levels());

    return doWithCatalog(
        catalogIdent,
        c -> c.doWithSchemaOps(s -> s.listSchemas(namespace)),
        NoSuchCatalogException.class);
  }

  @Override
  public Schema createSchema(NameIdentifier ident, String comment, Map<String, String> metadata)
      throws SchemaAlreadyExistsException {
    NameIdentifier catalogIdent = NameIdentifier.of(ident.namespace().levels());

    return doWithCatalog(
        catalogIdent,
        c -> c.doWithSchemaOps(s -> s.createSchema(ident, comment, metadata)),
        CatalogAlreadyExistsException.class);
  }

  @Override
  public Schema loadSchema(NameIdentifier ident) throws NoSuchSchemaException {
    NameIdentifier catalogIdent = NameIdentifier.of(ident.namespace().levels());

    return doWithCatalog(
        catalogIdent,
        c -> c.doWithSchemaOps(s -> s.loadSchema(ident)),
        NoSuchSchemaException.class);
  }

  @Override
  public Schema alterSchema(NameIdentifier ident, SchemaChange... changes)
      throws NoSuchSchemaException {
    NameIdentifier catalogIdent = NameIdentifier.of(ident.namespace().levels());

    return doWithCatalog(
        catalogIdent,
        c -> c.doWithSchemaOps(s -> s.alterSchema(ident, changes)),
        NoSuchSchemaException.class);
  }

  @Override
  public boolean dropSchema(NameIdentifier ident, boolean cascade) throws NonEmptySchemaException {
    NameIdentifier catalogIdent = NameIdentifier.of(ident.namespace().levels());

    return doWithCatalog(
        catalogIdent,
        c -> c.doWithSchemaOps(s -> s.dropSchema(ident, cascade)),
        NoSuchSchemaException.class);
  }

  @Override
  public NameIdentifier[] listTables(Namespace namespace) throws NoSuchSchemaException {
    NameIdentifier catalogIdent = NameIdentifier.of(namespace.levels());

    return doWithCatalog(
        catalogIdent,
        c -> c.doWithTableOps(t -> t.listTables(namespace)),
        NoSuchSchemaException.class);
  }

  @Override
  public Table loadTable(NameIdentifier ident) throws NoSuchTableException {
    NameIdentifier catalogIdent = NameIdentifier.of(ident.namespace().levels());

    return doWithCatalog(
        catalogIdent, c -> c.doWithTableOps(t -> t.loadTable(ident)), NoSuchTableException.class);
  }

  @Override
  public Table createTable(
      NameIdentifier ident, Column[] columns, String comment, Map<String, String> properties)
      throws NoSuchSchemaException, TableAlreadyExistsException {
    NameIdentifier catalogIdent = NameIdentifier.of(ident.namespace().levels());

    return doWithCatalog(
        catalogIdent,
        c -> c.doWithTableOps(t -> t.createTable(ident, columns, comment, properties)),
        NoSuchSchemaException.class,
        TableAlreadyExistsException.class);
  }

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

  @Override
  public boolean dropTable(NameIdentifier ident) {
    NameIdentifier catalogIdent = NameIdentifier.of(ident.namespace().levels());

    return doWithCatalog(
        catalogIdent, c -> c.doWithTableOps(t -> t.dropTable(ident)), NoSuchTableException.class);
  }

  private <R, E extends Throwable> R doWithCatalog(
      NameIdentifier ident, ThrowableFunction<CatalogManager.CatalogWrapper, R> fn, Class<E> ex)
      throws E {
    CatalogManager.CatalogWrapper c = catalogManager.loadCatalogAndWrap(ident);

    try {
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
    CatalogManager.CatalogWrapper c = catalogManager.loadCatalogAndWrap(ident);

    try {
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
}
