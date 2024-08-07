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

package org.apache.gravitino.catalog.doris;

import static org.apache.gravitino.catalog.doris.DorisTablePropertiesMetadata.REPLICATION_FACTOR;

import java.util.Map;
import org.apache.gravitino.connector.PropertyEntry;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestDorisCatalog {

  @Test
  void testDorisTablePropertiesMetadata() {
    DorisTablePropertiesMetadata dorisTablePropertiesMetadata = new DorisTablePropertiesMetadata();
    Map<String, PropertyEntry<?>> propertyEntryMap =
        dorisTablePropertiesMetadata.specificPropertyEntries();
    Assertions.assertTrue(propertyEntryMap.containsKey(REPLICATION_FACTOR));

    PropertyEntry<?> propertyEntry = propertyEntryMap.get(REPLICATION_FACTOR);
    Assertions.assertEquals(REPLICATION_FACTOR, propertyEntry.getName());
    Assertions.assertFalse(propertyEntry.isImmutable());
    Assertions.assertEquals(
        DorisTablePropertiesMetadata.DEFAULT_REPLICATION_FACTOR, propertyEntry.getDefaultValue());
    Assertions.assertFalse(propertyEntry.isHidden());
  }
}
