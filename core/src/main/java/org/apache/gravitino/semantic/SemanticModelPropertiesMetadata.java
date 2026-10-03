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
package org.apache.gravitino.semantic;

import static org.apache.gravitino.semantic.SemanticModel.DEFAULT_OSSIE_VERSION;
import static org.apache.gravitino.semantic.SemanticModel.PROPERTY_OSSIE_VERSION;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import java.util.Map;
import java.util.function.Function;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.connector.PropertiesMetadata;
import org.apache.gravitino.connector.PropertyEntry;

/** Property metadata shared by all Gravitino-managed Semantic Models. */
public final class SemanticModelPropertiesMetadata implements PropertiesMetadata {

  private static final Map<String, PropertyEntry<?>> PROPERTY_ENTRIES =
      ImmutableMap.of(
          PROPERTY_OSSIE_VERSION,
          new PropertyEntry.Builder<String>()
              .withName(PROPERTY_OSSIE_VERSION)
              .withDescription("The Apache Ossie document version used for import and export")
              .withRequired(false)
              .withImmutable(false)
              .withJavaType(String.class)
              .withDefaultValue(DEFAULT_OSSIE_VERSION)
              .withDecoder(SemanticModelPropertiesMetadata::decodeOssieVersion)
              .withEncoder(Function.identity())
              .withHidden(false)
              .withReserved(false)
              .build());

  /** Creates Semantic Model property metadata. */
  public SemanticModelPropertiesMetadata() {}

  @Override
  public Map<String, PropertyEntry<?>> propertyEntries() {
    return PROPERTY_ENTRIES;
  }

  private static String decodeOssieVersion(String value) {
    Preconditions.checkArgument(
        StringUtils.isNotBlank(value), "Apache Ossie version must not be blank");
    return value;
  }
}
