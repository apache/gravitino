/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.graviton.catalog.lakehouse.iceberg.web.rest;

import com.datastrato.graviton.catalog.lakehouse.iceberg.web.IcebergExceptionMapper;
import com.datastrato.graviton.catalog.lakehouse.iceberg.web.IcebergObjectMapperProvider;
import org.glassfish.jersey.jackson.JacksonFeature;
import org.glassfish.jersey.server.ResourceConfig;

public class IcebergRestTestUtil {

  private static String PREFIX = "v1";
  public static String CONFIG_PATH = PREFIX + "/config";
  public static String NAMESPACE_PATH = PREFIX + "/namespaces";

  public static ResourceConfig getIcebergResourceConfig(Class c) {
    ResourceConfig resourceConfig = new ResourceConfig();
    resourceConfig.register(c);
    resourceConfig.register(IcebergObjectMapperProvider.class).register(JacksonFeature.class);
    resourceConfig.register(IcebergExceptionMapper.class);
    return resourceConfig;
  }
}
