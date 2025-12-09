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
package org.apache.gravitino.hive.client;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Properties;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

public class Util {

  public static final String HIVE_CONFIG_RESOURCES = "hive.config.resources";

  public static Method findMethod(Class<?> klass, String name, Class<?>... args)
      throws NoSuchMethodException {
    return klass.getMethod(name, args);
  }

  protected static Method findStaticMethod(Class<?> klass, String name, Class<?>... args)
      throws NoSuchMethodException {
    Method method = findMethod(klass, name, args);

    if (!Modifier.isStatic(method.getModifiers())) {
      throw new IllegalArgumentException(
          "Method " + name + " of class " + klass.getName() + " is not static.");
    }
    return method;
  }

  public static void buildConfiguration(Properties config, Configuration configuration) {
    try {
      String hdfsConfigResources = config.getProperty(HIVE_CONFIG_RESOURCES);
      if (StringUtils.isNotBlank(hdfsConfigResources)) {
        for (String resource : hdfsConfigResources.split(",")) {
          resource = resource.trim();
          if (StringUtils.isNotBlank(resource)) {
            configuration.addResource(new Path(resource));
          }
        }
      }

      config.forEach((k, v) -> configuration.set(k.toString(), v.toString()));
    } catch (Exception e) {
      throw new RuntimeException("Failed to create configuration", e);
    }
  }
}
