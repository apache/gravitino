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

import java.util.Map;
import org.apache.gravitino.connector.PropertyEntry;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestKafkaCatalogPropertiesMetadata {

  @Test
  public void testTableTopicCatalogPropertyEntries() {
    KafkaCatalogPropertiesMetadata metadata = new KafkaCatalogPropertiesMetadata();
    Map<String, PropertyEntry<?>> entries = metadata.propertyEntries();

    PropertyEntry<?> icebergCatalogEntry =
        entries.get(KafkaCatalogPropertiesMetadata.TABLE_TOPIC_ICEBERG_CATALOG);
    Assertions.assertNotNull(icebergCatalogEntry);
    Assertions.assertFalse(icebergCatalogEntry.isRequired());
    Assertions.assertFalse(icebergCatalogEntry.isImmutable());
    Assertions.assertFalse(icebergCatalogEntry.isHidden());
    Assertions.assertFalse(icebergCatalogEntry.isReserved());
    Assertions.assertNull(icebergCatalogEntry.getDefaultValue());
    Assertions.assertEquals("iceberg_prod", icebergCatalogEntry.decode(" iceberg_prod "));
    Assertions.assertThrows(IllegalArgumentException.class, () -> icebergCatalogEntry.decode(""));
    Assertions.assertThrows(
        IllegalArgumentException.class, () -> icebergCatalogEntry.decode("   "));

    PropertyEntry<?> defaultNsEntry =
        entries.get(KafkaCatalogPropertiesMetadata.TABLE_TOPIC_DEFAULT_NAMESPACE);
    Assertions.assertNotNull(defaultNsEntry);
    Assertions.assertFalse(defaultNsEntry.isRequired());
    Assertions.assertFalse(defaultNsEntry.isImmutable());
    Assertions.assertFalse(defaultNsEntry.isHidden());
    Assertions.assertFalse(defaultNsEntry.isReserved());
    Assertions.assertEquals("default", defaultNsEntry.getDefaultValue());
    Assertions.assertEquals("analytics", defaultNsEntry.decode(" analytics "));
    Assertions.assertThrows(IllegalArgumentException.class, () -> defaultNsEntry.decode(""));
    Assertions.assertThrows(IllegalArgumentException.class, () -> defaultNsEntry.decode("   "));
  }
}
