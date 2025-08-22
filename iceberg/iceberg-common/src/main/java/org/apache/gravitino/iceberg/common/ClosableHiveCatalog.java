/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.gravitino.iceberg.common;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import java.io.Closeable;
import java.io.IOException;
import java.lang.reflect.Field;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.gravitino.iceberg.common.utils.IcebergHiveCachedClientPool;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.hadoop.HadoopFileIO;
import org.apache.iceberg.hive.HiveCatalog;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.util.LocationUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * ClosableHiveCatalog is a wrapper class to wrap Iceberg HiveCatalog to do some clean-up work like
 * closing resources.
 */
public class ClosableHiveCatalog extends HiveCatalog implements Closeable {

  private static final Logger LOGGER = LoggerFactory.getLogger(ClosableHiveCatalog.class);

  private final List<Closeable> resources = Lists.newArrayList();

  public ClosableHiveCatalog() {
    super();
  }

  public void addResource(Closeable resource) {
    resources.add(resource);
  }

  @Override
  public void initialize(String inputName, Map<String, String> properties) {
    try {
      FieldUtils.writeField(this, "catalogProperties", ImmutableMap.copyOf(properties), true);
      FieldUtils.writeField(this, "name", inputName, true);

      Field confField = FieldUtils.getField(HiveCatalog.class, "conf", true);
      Configuration configuration = (Configuration) confField.get(this);
      if (configuration != null) {
        LOGGER.warn("No Hadoop Configuration was set, using the default environment Configuration");
        FieldUtils.writeField(this, "conf", new Configuration(), true);
      }

      configuration = (Configuration) confField.get(this);
      if (properties.containsKey("uri")) {
        configuration.set(ConfVars.METASTOREURIS.varname, properties.get("uri"));
      }
      if (properties.containsKey("warehouse")) {
        configuration.set(
            ConfVars.METASTOREWAREHOUSE.varname,
            LocationUtil.stripTrailingSlash(properties.get("warehouse")));
      }

      FieldUtils.writeField(
          this,
          "listAllTables",
          Boolean.parseBoolean(properties.getOrDefault("list-all-tables", "false")),
          true);
      String fileIOImpl = properties.get("io-impl");

      FileIO io =
          fileIOImpl == null
              ? new HadoopFileIO(configuration)
              : CatalogUtil.loadFileIO(fileIOImpl, properties, configuration);
      FieldUtils.writeField(this, "fileIO", io, true);

      IcebergHiveCachedClientPool icebergHiveCachedClientPool =
          new IcebergHiveCachedClientPool(configuration, properties);
      this.addResource(icebergHiveCachedClientPool);

      FieldUtils.writeField(this, "clients", icebergHiveCachedClientPool, true);

    } catch (Exception e) {
      throw new RuntimeException(
          "Failed to initialize ClosableHiveCatalog with name: " + inputName, e);
    }
  }

  @Override
  public void close() throws IOException {
    // Do clean up work here. We need a mechanism to close the HiveCatalog; however, HiveCatalog
    // doesn't implement the Closeable interface.
    resources.forEach(
        resource -> {
          try {
            if (resource != null) {
              resource.close();
            }
          } catch (Exception e) {
            LOGGER.warn("Failed to close resource: {}", resource, e);
          }
        });
  }
}
