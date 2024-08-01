package org.apache.gravitino.iceberg.service.rest;

import org.apache.gravitino.iceberg.common.ops.ConfigIcebergTableOpsProvider;
import org.apache.gravitino.iceberg.common.ops.IcebergTableOps;

public class ConfigIcebergTableOpsProviderForTest extends ConfigIcebergTableOpsProvider {
  @Override
  public IcebergTableOps getIcebergTableOps(String prefix) {
    return new IcebergTableOpsForTest();
  }
}
