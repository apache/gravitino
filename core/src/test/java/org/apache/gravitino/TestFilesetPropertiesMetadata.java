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
package org.apache.gravitino;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import java.util.List;
import java.util.Map;
import org.apache.gravitino.connector.PropertyEntry;

public class TestFilesetPropertiesMetadata extends TestBasePropertiesMetadata {

  public static final String TEST_FILESET_HIDDEN_KEY = "fileset_key";

  private static final Map<String, PropertyEntry<?>> TEST_FILESET_PROPERTY;

  static {
    List<PropertyEntry<?>> tablePropertyMetadata =
        ImmutableList.of(
            PropertyEntry.stringPropertyEntry(
                TEST_FILESET_HIDDEN_KEY,
                "test fileset required k1 property",
                false,
                false,
                "test",
                true,
                false));

    TEST_FILESET_PROPERTY = Maps.uniqueIndex(tablePropertyMetadata, PropertyEntry::getName);
  }

  @Override
  protected Map<String, PropertyEntry<?>> specificPropertyEntries() {
    return ImmutableMap.<String, PropertyEntry<?>>builder()
        .putAll(super.specificPropertyEntries())
        .putAll(TEST_FILESET_PROPERTY)
        .build();
  }
}
