/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino;

import com.datastrato.gravitino.annotation.Evolving;
import com.datastrato.gravitino.file.FilesetCatalog;
import com.datastrato.gravitino.messaging.TopicCatalog;
import com.datastrato.gravitino.rel.TableCatalog;

/**
 * The client interface of a catalog. The catalog is the second level entity in the gravitino
 * system, containing a set of tables. The server side should use the other one with the same name
 * in the core module.
 */
@Evolving
public interface Catalog extends CatalogBasicInfo {

  /**
   * Return the {@link SupportsSchemas} if the catalog supports schema operations.
   *
   * @return The {@link SupportsSchemas} if the catalog supports schema operations.
   * @throws UnsupportedOperationException if the catalog does not support schema operations.
   */
  default SupportsSchemas asSchemas() throws UnsupportedOperationException {
    throw new UnsupportedOperationException("Catalog does not support schema operations");
  }

  /**
   * @return the {@link TableCatalog} if the catalog supports table operations.
   * @throws UnsupportedOperationException if the catalog does not support table operations.
   */
  default TableCatalog asTableCatalog() throws UnsupportedOperationException {
    throw new UnsupportedOperationException("Catalog does not support table operations");
  }

  /**
   * @return the {@link FilesetCatalog} if the catalog supports fileset operations.
   * @throws UnsupportedOperationException if the catalog does not support fileset operations.
   */
  default FilesetCatalog asFilesetCatalog() throws UnsupportedOperationException {
    throw new UnsupportedOperationException("Catalog does not support fileset operations");
  }

  /**
   * @return the {@link TopicCatalog} if the catalog supports topic operations.
   * @throws UnsupportedOperationException if the catalog does not support topic operations.
   */
  default TopicCatalog asTopicCatalog() throws UnsupportedOperationException {
    throw new UnsupportedOperationException("Catalog does not support topic operations");
  }
}
