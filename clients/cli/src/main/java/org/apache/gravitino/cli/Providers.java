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

package org.apache.gravitino.cli;

import java.util.HashSet;
import org.apache.gravitino.Catalog;

/**
 * The {@code CommandEntities} class defines a set of standard entities that can be used in the
 * Gravitino CLI. It also can validate if a given entity is a valid entity.
 */
public class Providers {
  public static final String HIVE = "hive";
  public static final String HADOOP = "hadoop";
  public static final String ICEBERG = "iceberg";
  public static final String MYSQL = "mysql";
  public static final String POSTGRES = "postgres";
  public static final String KAFKA = "kafka";

  private static final HashSet<String> VALID_PROVIDERS = new HashSet<>();

  static {
    VALID_PROVIDERS.add(HIVE);
    VALID_PROVIDERS.add(HADOOP);
    VALID_PROVIDERS.add(ICEBERG);
    VALID_PROVIDERS.add(MYSQL);
    VALID_PROVIDERS.add(POSTGRES);
    VALID_PROVIDERS.add(KAFKA);
  }

  /**
   * Checks if a given provider is a valid provider.
   *
   * @param provider The provider to check.
   * @return true if the provider is valid, false otherwise.
   */
  public static boolean isValidProvider(String provider) {
    return VALID_PROVIDERS.contains(provider);
  }

  public static String internal(String provider) {
    switch (provider) {
      case HIVE:
        return "hive";
      case HADOOP:
        return "hadoop";
      case MYSQL:
        return "jdbc-mysql";
      case POSTGRES:
        return "jdbc-postgresql";
      case ICEBERG:
        return "lakehouse-iceberg";
      case KAFKA:
        return "kafka";
      default:
        throw new IllegalArgumentException("Unsupported provider: " + provider);
    }
  }

  public static Catalog.Type catalogType(String provider) {
    switch (provider) {
      case HADOOP:
        return Catalog.Type.FILESET;
      case HIVE:
      case MYSQL:
      case POSTGRES:
      case ICEBERG:
        return Catalog.Type.RELATIONAL;
      case KAFKA:
        return Catalog.Type.MESSAGING;
      default:
        throw new IllegalArgumentException("Unsupported provider: " + provider);
    }
  }
}
