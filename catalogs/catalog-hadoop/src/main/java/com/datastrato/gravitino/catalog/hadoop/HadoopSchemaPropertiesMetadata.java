/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.catalog.hadoop;

import com.datastrato.gravitino.connector.BasePropertiesMetadata;
import com.datastrato.gravitino.connector.PropertyEntry;
import com.google.common.collect.ImmutableMap;
import java.util.Map;

public class HadoopSchemaPropertiesMetadata extends BasePropertiesMetadata {

  // Property "location" is used to specify the schema's storage location managed by Hadoop fileset
  // catalog.
  // If specified, the property "location" specified in the catalog level will be overridden. Also,
  // the location will be used as the default storage location for all the managed filesets created
  // under this schema.
  //
  // If not specified, the property "location" specified in the catalog level will be used (if
  // specified), or users have to specify the storage location in the Fileset level.
  public static final String LOCATION = "location";

  private static final Map<String, PropertyEntry<?>> HADOOP_SCHEMA_ENTRIES =
      ImmutableMap.<String, PropertyEntry<?>>builder()
          .put(
              LOCATION,
              PropertyEntry.stringOptionalPropertyEntry(
                  LOCATION,
                  "The storage location managed by Hadoop schema",
                  true /* immutable */,
                  null,
                  false /* hidden */))
          .build();

  @Override
  protected Map<String, PropertyEntry<?>> specificPropertyEntries() {
    return HADOOP_SCHEMA_ENTRIES;
  }
}
