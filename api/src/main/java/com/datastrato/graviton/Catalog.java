package com.datastrato.graviton;

import com.datastrato.graviton.rel.TableCatalog;
import java.util.Map;

public interface Catalog extends Auditable {

  enum Type {
    RELATIONAL, // Catalog Type for Relational Data Structure, like db.table, catalog.db.table.
    FILE, // Catalog Type for File System (including HDFS, S3, etc.), like path/to/file
    STREAM, // Catalog Type for Streaming Data, like kafka://topic
  }

  String name();

  Type type();

  String comment();

  Map<String, String> properties();

  default SupportsNamespaces asNamespaces() throws UnsupportedOperationException {
    throw new UnsupportedOperationException("Catalog does not support namespace operations");
  }

  default TableCatalog asTableCatalog() throws UnsupportedOperationException {
    throw new UnsupportedOperationException("Catalog does not support table operations");
  }
}
