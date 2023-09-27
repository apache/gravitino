/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.graviton.catalog.lakehouse.iceberg;

import com.datastrato.graviton.PropertyEntry;
import com.datastrato.graviton.TableProperty;
import com.google.common.collect.Maps;
import java.util.Map;

public class IcebergTableProperty extends TableProperty {
  @Override
  protected Map<String, PropertyEntry<?>> tableProperty() {
    // TODO: support Iceberg table property specs
    return Maps.newHashMap();
  }
}
