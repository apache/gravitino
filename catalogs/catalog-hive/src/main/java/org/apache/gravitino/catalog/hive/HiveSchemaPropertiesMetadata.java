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
package org.apache.gravitino.catalog.hive;

import static org.apache.gravitino.connector.PropertyEntry.stringOptionalPropertyEntry;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import java.util.List;
import java.util.Map;
import org.apache.gravitino.connector.BasePropertiesMetadata;
import org.apache.gravitino.connector.PropertyEntry;

public class HiveSchemaPropertiesMetadata extends BasePropertiesMetadata {
  public static final String LOCATION = HiveConstants.LOCATION;
  private static final Map<String, PropertyEntry<?>> propertiesMetadata;

  static {
    List<PropertyEntry<?>> propertyEntries =
        ImmutableList.of(
            stringOptionalPropertyEntry(
                LOCATION,
                "The directory for Hive database storage. Not required, HMS uses the value of "
                    + "`hive.metastore.warehouse.dir` in the hive-site.xml by default",
                false,
                null,
                false));

    propertiesMetadata = Maps.uniqueIndex(propertyEntries, PropertyEntry::getName);
  }

  @Override
  protected Map<String, PropertyEntry<?>> specificPropertyEntries() {
    return propertiesMetadata;
  }
}
