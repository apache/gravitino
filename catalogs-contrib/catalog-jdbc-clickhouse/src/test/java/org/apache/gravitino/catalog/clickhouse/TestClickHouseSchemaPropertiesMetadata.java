/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */
package org.apache.gravitino.catalog.clickhouse;

import java.util.Map;
import org.apache.gravitino.StringIdentifier;
import org.apache.gravitino.connector.PropertyEntry;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestClickHouseSchemaPropertiesMetadata {

  @Test
  void testSchemaPropertyEntriesIncludeClusterToggle() {
    ClickHouseSchemaPropertiesMetadata metadata = new ClickHouseSchemaPropertiesMetadata();
    Map<String, PropertyEntry<?>> entries = metadata.propertyEntries();

    Assertions.assertTrue(
        entries.containsKey(ClickHouseConfig.CK_ON_CLUSTER.getKey()), "on cluster missing");
    PropertyEntry<?> onCluster = entries.get(ClickHouseConfig.CK_ON_CLUSTER.getKey());
    Assertions.assertEquals(Boolean.FALSE, onCluster.getDefaultValue());
    Assertions.assertFalse(onCluster.isRequired());
    Assertions.assertFalse(onCluster.isImmutable());
    Assertions.assertFalse(onCluster.isHidden());

    // Reserved ID property should still exist
    Assertions.assertTrue(entries.containsKey(StringIdentifier.ID_KEY));
  }
}
