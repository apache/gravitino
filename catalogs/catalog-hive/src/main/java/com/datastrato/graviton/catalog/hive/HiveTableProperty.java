/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.graviton.catalog.hive;

import com.datastrato.graviton.PropertyEntry;
import com.datastrato.graviton.TableProperty;
import com.google.common.collect.Maps;
import java.util.Map;

public class HiveTableProperty extends TableProperty {
  @Override
  protected Map<String, PropertyEntry<?>> tableProperty() {
    // TODO(Minghuang): support Hive table property specs
    return Maps.newHashMap();
  }
}
