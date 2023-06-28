package com.datastrato.graviton;

import com.datastrato.graviton.exceptions.LakehouseAlreadyExistsException;
import com.datastrato.graviton.exceptions.NoSuchLakehouseException;
import java.util.Map;

public interface SupportLakehouses {

  /** List all lakehouses. */
  Lakehouse[] listLakehouses();

  /**
   * Load a lakehouse by its identifier.
   *
   * @param ident the identifier of the lakehouse.
   * @return The lakehouse.
   * @throws NoSuchLakehouseException If the lakehouse does not exist.
   */
  Lakehouse loadLakehouse(NameIdentifier ident) throws NoSuchLakehouseException;

  /**
   * Check if a lakehouse exists.
   *
   * @param ident The identifier of the lakehouse.
   * @return True if the lakehouse exists, false otherwise.
   */
  default boolean lakehouseExists(NameIdentifier ident) {
    try {
      loadLakehouse(ident);
      return true;
    } catch (NoSuchLakehouseException e) {
      return false;
    }
  }

  /**
   * Create a lakehouse with specified identifier.
   *
   * @param ident The identifier of the lakehouse.
   * @param comment The comment of the lakehouse.
   * @param properties The properties of the lakehouse.
   * @return The created lakehouse.
   * @throws LakehouseAlreadyExistsException If the lakehouse already exists.
   */
  Lakehouse createLakehouse(NameIdentifier ident, String comment, Map<String, String> properties)
      throws LakehouseAlreadyExistsException;

  /**
   * Alter a lakehouse with specified identifier.
   *
   * @param ident The identifier of the lakehouse.
   * @param changes The changes to apply.
   * @return The altered lakehouse.
   * @throws NoSuchLakehouseException If the lakehouse does not exist.
   * @throws IllegalArgumentException If the changes cannot be applied to the lakehouse.
   */
  Lakehouse alterLakehouse(NameIdentifier ident, LakehouseChange... changes)
      throws NoSuchLakehouseException, IllegalArgumentException;

  /**
   * Drop a lakehouse with specified identifier.
   *
   * @param ident The identifier of the lakehouse.
   * @return True if the lakehouse was dropped, false otherwise.
   */
  boolean dropLakehouse(NameIdentifier ident);
}
