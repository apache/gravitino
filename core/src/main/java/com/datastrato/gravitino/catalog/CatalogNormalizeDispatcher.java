/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.catalog;

import static com.datastrato.gravitino.Entity.SECURABLE_ENTITY_RESERVED_NAME;
import static com.datastrato.gravitino.Entity.SYSTEM_CATALOG_RESERVED_NAME;

import com.datastrato.gravitino.Catalog;
import com.datastrato.gravitino.CatalogChange;
import com.datastrato.gravitino.NameIdentifier;
import com.datastrato.gravitino.Namespace;
import com.datastrato.gravitino.exceptions.CatalogAlreadyExistsException;
import com.datastrato.gravitino.exceptions.NoSuchCatalogException;
import com.datastrato.gravitino.exceptions.NoSuchMetalakeException;
import com.google.common.collect.ImmutableSet;
import java.util.Arrays;
import java.util.Map;
import java.util.Set;

public class CatalogNormalizeDispatcher implements CatalogDispatcher {
  private static final Set<String> RESERVED_WORDS =
      ImmutableSet.of(SECURABLE_ENTITY_RESERVED_NAME, SYSTEM_CATALOG_RESERVED_NAME);
  /**
   * Regular expression explanation:
   *
   * <p>^\w - Starts with a letter, digit, or underscore
   *
   * <p>[\w]{0,63} - Followed by 0 to 63 characters (making the total length at most 64) of letters
   * (both cases), digits, underscores
   *
   * <p>$ - End of the string
   */
  private static final String CATALOG_NAME_PATTERN = "^\\w[\\w]{0,63}$";

  private final CatalogDispatcher dispatcher;

  public CatalogNormalizeDispatcher(CatalogDispatcher dispatcher) {
    this.dispatcher = dispatcher;
  }

  @Override
  public NameIdentifier[] listCatalogs(Namespace namespace) throws NoSuchMetalakeException {
    return dispatcher.listCatalogs(namespace);
  }

  @Override
  public Catalog[] listCatalogsInfo(Namespace namespace) throws NoSuchMetalakeException {
    return dispatcher.listCatalogsInfo(namespace);
  }

  @Override
  public Catalog loadCatalog(NameIdentifier ident) throws NoSuchCatalogException {
    return dispatcher.loadCatalog(ident);
  }

  @Override
  public boolean catalogExists(NameIdentifier ident) {
    return dispatcher.catalogExists(ident);
  }

  @Override
  public Catalog createCatalog(
      NameIdentifier ident,
      Catalog.Type type,
      String provider,
      String comment,
      Map<String, String> properties)
      throws NoSuchMetalakeException, CatalogAlreadyExistsException {
    validateCatalogName(ident.name());
    return dispatcher.createCatalog(ident, type, provider, comment, properties);
  }

  @Override
  public Catalog alterCatalog(NameIdentifier ident, CatalogChange... changes)
      throws NoSuchCatalogException, IllegalArgumentException {
    Arrays.stream(changes)
        .forEach(
            c -> {
              if (c instanceof CatalogChange.RenameCatalog) {
                validateCatalogName(((CatalogChange.RenameCatalog) c).getNewName());
              }
            });
    return dispatcher.alterCatalog(ident, changes);
  }

  @Override
  public boolean dropCatalog(NameIdentifier ident) {
    return dispatcher.dropCatalog(ident);
  }

  private void validateCatalogName(String name) throws IllegalArgumentException {
    if (RESERVED_WORDS.contains(name.toLowerCase())) {
      throw new IllegalArgumentException("The catalog name '" + name + "' is reserved.");
    }

    if (!name.matches(CATALOG_NAME_PATTERN)) {
      throw new IllegalArgumentException("The catalog name '" + name + "' is illegal.");
    }
  }
}
