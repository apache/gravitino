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
package org.apache.gravitino.catalog.kafka;

import java.util.Collections;
import java.util.Map;
import org.apache.gravitino.connector.BaseCatalogPropertiesMetadata;
import org.apache.gravitino.connector.PropertyEntry;

public class KafkaCatalogPropertiesMetadata extends BaseCatalogPropertiesMetadata {

  // The "bootstrap.servers" property specifies the Kafka broker(s) to connect to, allowing for
  // multiple brokers by comma-separating them.
  public static final String BOOTSTRAP_SERVERS = "bootstrap.servers";

  private static final Map<String, PropertyEntry<?>> KAFKA_CATALOG_PROPERTY_ENTRIES =
      Collections.singletonMap(
          BOOTSTRAP_SERVERS,
          PropertyEntry.stringRequiredPropertyEntry(
              BOOTSTRAP_SERVERS,
              "The Kafka broker(s) to connect to, allowing for multiple brokers by comma-separating them",
              false /* immutable */,
              false /* hidden */));

  @Override
  protected Map<String, PropertyEntry<?>> specificPropertyEntries() {
    return KAFKA_CATALOG_PROPERTY_ENTRIES;
  }
}
