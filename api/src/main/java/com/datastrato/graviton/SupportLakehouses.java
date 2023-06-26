package com.datastrato.graviton;

import com.datastrato.graviton.exceptions.LakehouseAlreadyExistsException;
import com.datastrato.graviton.exceptions.NoSuchLakehouseException;
import java.util.Map;

public interface SupportLakehouses {

  Lakehouse[] listLakehouses();

  Lakehouse loadLakehouse(NameIdentifier ident) throws NoSuchLakehouseException;

  default boolean lakehouseExists(NameIdentifier ident) {
    try {
      loadLakehouse(ident);
      return true;
    } catch (NoSuchLakehouseException e) {
      return false;
    }
  }

  Lakehouse createLakehouse(NameIdentifier ident, String comment, Map<String, String> properties)
      throws LakehouseAlreadyExistsException;

  Lakehouse alterLakehouse(NameIdentifier ident, LakehouseChange... changes)
      throws NoSuchLakehouseException, IllegalArgumentException;

  boolean dropLakehouse(NameIdentifier ident);
}
