package org.apache.gravitino.iceberg.common.ops;

import java.util.Optional;
import org.apache.gravitino.iceberg.common.IcebergConfig;

/**
 * IcebergTableOpsProvider is an interface defining how iceberg rest catalog server gets iceberg
 * catalogs.
 */
public interface IcebergTableOpsProvider {

  /** @param config The configuration parameters for creating Provider. */
  void initialize(IcebergConfig config);

  /**
   * @param prefix the path param send by clients.
   * @return the instance of IcebergTableOps.
   */
  IcebergTableOps getIcebergTableOps(String prefix);

  /**
   * Get a path prefix using by clients.
   *
   * @param warehouse the identifier for an iceberg catalog.
   * @return a path prefix.
   */
  Optional<String> getPrefix(String warehouse);
}
