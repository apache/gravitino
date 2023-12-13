/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.catalog;

import org.apache.commons.lang3.tuple.Pair;

/**
 * {@link HasCatalogDefaultConfig} holds several default configurations for catalog that should be
 * known by the gravitino core.
 *
 * <p>For example, whether the schema name is case-sensitive or not will heavily impact the behavior
 * of gravitino storage system. If the schema name is case-insensitive, then we should always use
 * the same case for the schema name, otherwise, we will get inconsistent results.
 *
 * <pre>
 *   NameIdentifier tableIdentifier = NameIdentifier.of("Schema", "Table");
 * </pre>
 *
 * In the hive catalog, the name of the scheme listing above will be 'schema' NOT 'Schema' and the
 * name of table will be 'table' NOT 'Table'. We need to use the same case for the schema name and
 * table name in gravitino to keep the consistency.
 */
public interface HasCatalogDefaultConfig {

  /**
   * Case sensitivity of schema name, As we will store schema name in gravitino, we need to know
   * whether the schema name is case-sensitive or not. For example, Hive is case-insensitive by
   * default. If users create a schema name like "Zhang", then the name "zhang" will be treated as
   * the same schema in the underlying storage system, so we should use "zhang" as well in gravitino
   * to keep the consistency.
   *
   * <p>Table name and column name are similar to schema name.
   *
   * @return true if the schema name is case-sensitive, false otherwise.
   */
  default boolean nameCaseSensitivity() {
    return true;
  }

  /**
   * When {@link HasCatalogDefaultConfig#nameCaseSensitivity()} is false, we should convert names to
   * lower-case or upper-case.
   *
   * <p>For example, Hive will use lower-case for all names and SQL Server will use upper-case for
   * all names.
   *
   * <p>Note:If {@link HasCatalogDefaultConfig#nameCaseSensitivity()} is true, this method will be
   * ignored.
   *
   * @return true if we should convert all names to upper-case and * false if we should convert
   *     names to lower-case.
   */
  default boolean toUpperCaseIfNotSensitive() {
    return true;
  }

  default Pair<Boolean, Boolean> caseConfiguration() {
    return Pair.of(nameCaseSensitivity(), toUpperCaseIfNotSensitive());
  }
}
