/*
 *  Copyright 2023 Datastrato Pvt Ltd.
 *  This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.catalog.lakehouse.iceberg.combine;

import com.google.common.base.Preconditions;
import java.util.function.Supplier;
import org.apache.iceberg.TableMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Utils {
  private static final Logger LOG = LoggerFactory.getLogger(Utils.class);

  // todo error handle
  public static <T> void doSecondaryCatalogAction(Supplier<T> supplier) {
    try {
      supplier.get();
    } catch (Exception e) {
      LOG.warn("Secondary catalog action failed,", e);
    }
  }

  public static void doSecondaryCatalogAction(Runnable runnable) {
    try {
      runnable.run();
    } catch (Exception e) {
      LOG.warn("Secondary catalog action failed,", e);
    }
  }

  public static String resueMetadataLocation(TableMetadata metadata) {
    Preconditions.checkArgument(
        metadata.metadataFileLocation() != null, "table metadata should be null");
    return metadata.metadataFileLocation();
  }
}
