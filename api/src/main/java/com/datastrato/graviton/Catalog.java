package com.datastrato.graviton;

import com.datastrato.graviton.rel.SupportsSchemas;
import com.datastrato.graviton.rel.TableCatalog;
import java.util.Map;

public interface Catalog extends Auditable {

  enum Type {
    RELATIONAL, // Catalog Type for Relational Data Structure, like db.table, catalog.db.table.
    FILE, // Catalog Type for File System (including HDFS, S3, etc.), like path/to/file
    STREAM, // Catalog Type for Streaming Data, like kafka://topic
  }

  /** The name of the catalog. */
  String name();

  /** The type of the catalog. */
  Type type();

  /**
   * The comment of the catalog. Note. this method will return null if the comment is not set for
   * this catalog.
   */
  String comment();

  /**
   * The properties of the catalog. Note, this method will return null if the properties are not
   * set.
   */
  Map<String, String> properties();

  /**
   * Return the {@link SupportsSchemas} if the catalog supports schema operations.
   *
   * @throws UnsupportedOperationException if the catalog does not support schema operations.
   */
  default SupportsSchemas asSchemas() throws UnsupportedOperationException {
    throw new UnsupportedOperationException("Catalog does not support schema operations");
  }

  /**
   * return the {@link TableCatalog} if the catalog supports table operations.
   *
   * @throws UnsupportedOperationException if the catalog does not support table operations.
   */
  default TableCatalog asTableCatalog() throws UnsupportedOperationException {
    throw new UnsupportedOperationException("Catalog does not support table operations");
  }
}
