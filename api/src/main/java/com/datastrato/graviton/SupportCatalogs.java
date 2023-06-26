package com.datastrato.graviton;

import com.datastrato.graviton.exceptions.CatalogAlreadyExistsException;
import com.datastrato.graviton.exceptions.NoSuchCatalogException;
import com.datastrato.graviton.exceptions.NoSuchLakehouseException;
import java.util.Map;

public interface SupportCatalogs {

  Catalog[] listCatalogs(Namespace namespace) throws NoSuchLakehouseException;

  Catalog loadCatalog(NameIdentifier ident) throws NoSuchCatalogException;

  default boolean catalogExists(NameIdentifier ident) {
    try {
      loadCatalog(ident);
      return true;
    } catch (NoSuchCatalogException e) {
      return false;
    }
  }

  Catalog createCatalog(
      NameIdentifier ident, Catalog.Type type, String comment, Map<String, String> properties)
      throws NoSuchLakehouseException, CatalogAlreadyExistsException;

  Catalog alterCatalog(NameIdentifier ident, CatalogChange... changes)
      throws NoSuchCatalogException, IllegalArgumentException;

  boolean dropCatalog(NameIdentifier ident);
}
