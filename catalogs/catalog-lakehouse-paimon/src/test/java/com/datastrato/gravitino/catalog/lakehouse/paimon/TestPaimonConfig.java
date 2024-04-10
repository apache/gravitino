/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.catalog.lakehouse.paimon;

import static com.datastrato.gravitino.catalog.lakehouse.paimon.PaimonCatalogPropertiesMetadata.GRAVITINO_TABLE_TYPE;
import static com.datastrato.gravitino.catalog.lakehouse.paimon.PaimonCatalogPropertiesMetadata.PAIMON_TABLE_TYPE;
import static com.datastrato.gravitino.catalog.lakehouse.paimon.PaimonConfig.TABLE_TYPE;
import static com.datastrato.gravitino.catalog.lakehouse.paimon.PaimonConfig.loadPaimonConfig;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.google.common.collect.ImmutableMap;
import java.util.Map;
import org.apache.paimon.table.TableType;
import org.junit.jupiter.api.Test;

/** Tests for {@link PaimonConfig}. */
public class TestPaimonConfig {

  @Test
  public void testLoadFromMap() {
    Map<String, String> properties =
        ImmutableMap.of(TABLE_TYPE.getKey(), TableType.EXTERNAL.toString());
    PaimonConfig paimonConfig = new PaimonConfig();
    paimonConfig.loadFromMap(properties, k -> k.startsWith("gravitino."));
    assertEquals(TABLE_TYPE.getDefaultValue(), paimonConfig.get(TABLE_TYPE));
    assertEquals(TableType.EXTERNAL.toString(), new PaimonConfig(properties).get(TABLE_TYPE));
  }

  @Test
  public void testLoadPaimonConfig() {
    assertEquals(
        TableType.EXTERNAL.toString(),
        loadPaimonConfig(ImmutableMap.of(GRAVITINO_TABLE_TYPE, TableType.EXTERNAL.toString()))
            .get(TABLE_TYPE));
    assertEquals(
        TABLE_TYPE.getDefaultValue(),
        loadPaimonConfig(ImmutableMap.of(PAIMON_TABLE_TYPE, TableType.EXTERNAL.toString()))
            .get(TABLE_TYPE));
  }
}
