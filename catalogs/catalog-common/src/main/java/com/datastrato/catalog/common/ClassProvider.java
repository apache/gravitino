/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.catalog.common;

import com.datastrato.gravitino.catalog.BasePropertiesMetadata;
import com.datastrato.gravitino.catalog.HasPropertyMetadata;
import com.datastrato.gravitino.catalog.PropertiesMetadata;
import com.datastrato.gravitino.catalog.PropertyEntry;
import com.datastrato.gravitino.catalog.hive.HiveCatalogPropertiesMeta;
import com.datastrato.gravitino.catalog.hive.HiveSchemaPropertiesMetadata;
import com.datastrato.gravitino.catalog.hive.HiveTablePropertiesMetadata;
import java.util.HashSet;
import java.util.Set;

public class ClassProvider {

  public static final Set<Class<?>> BASE_CLASS =
      new HashSet<Class<?>>() {
        {
          add(BasePropertiesMetadata.class);
          add(PropertyEntry.class);
          add(PropertiesMetadata.class);
          add(HasPropertyMetadata.class);
        }
      };

  public static final Set<Class<?>> HIVE_NEED_CLASS =
      new HashSet<Class<?>>() {
        {
          add(HiveTablePropertiesMetadata.class);
          add(HiveSchemaPropertiesMetadata.class);
          add(HiveCatalogPropertiesMeta.class);
        }
      };
}
