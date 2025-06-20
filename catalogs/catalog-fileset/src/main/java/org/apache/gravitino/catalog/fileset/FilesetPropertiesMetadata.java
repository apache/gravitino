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
package org.apache.gravitino.catalog.fileset;

import static org.apache.gravitino.file.Fileset.PROPERTY_CATALOG_PLACEHOLDER;
import static org.apache.gravitino.file.Fileset.PROPERTY_DEFAULT_LOCATION_NAME;
import static org.apache.gravitino.file.Fileset.PROPERTY_FILESET_PLACEHOLDER;
import static org.apache.gravitino.file.Fileset.PROPERTY_LOCATION_PLACEHOLDER_PREFIX;
import static org.apache.gravitino.file.Fileset.PROPERTY_SCHEMA_PLACEHOLDER;

import com.google.common.collect.ImmutableMap;
import java.util.Map;
import org.apache.gravitino.catalog.fileset.authentication.AuthenticationConfig;
import org.apache.gravitino.catalog.fileset.authentication.kerberos.KerberosConfig;
import org.apache.gravitino.connector.BasePropertiesMetadata;
import org.apache.gravitino.connector.PropertyEntry;
import org.apache.gravitino.credential.config.CredentialConfig;

public class FilesetPropertiesMetadata extends BasePropertiesMetadata {

  @Override
  protected Map<String, PropertyEntry<?>> specificPropertyEntries() {
    ImmutableMap.Builder<String, PropertyEntry<?>> builder = ImmutableMap.builder();
    builder
        .put(
            PROPERTY_CATALOG_PLACEHOLDER,
            PropertyEntry.stringReservedPropertyEntry(
                PROPERTY_CATALOG_PLACEHOLDER,
                "The placeholder will be replaced to catalog name in the location",
                true /* hidden */))
        .put(
            PROPERTY_SCHEMA_PLACEHOLDER,
            PropertyEntry.stringReservedPropertyEntry(
                PROPERTY_SCHEMA_PLACEHOLDER,
                "The placeholder will be replaced to schema name in the location",
                true /* hidden */))
        .put(
            PROPERTY_FILESET_PLACEHOLDER,
            PropertyEntry.stringReservedPropertyEntry(
                PROPERTY_FILESET_PLACEHOLDER,
                "The placeholder will be replaced to fileset name in the location",
                true /* hidden */))
        .put(
            PROPERTY_LOCATION_PLACEHOLDER_PREFIX,
            PropertyEntry.stringImmutablePropertyPrefixEntry(
                PROPERTY_LOCATION_PLACEHOLDER_PREFIX,
                "The prefix of fileset placeholder property",
                false /* required */,
                null /* default value */,
                false /* hidden */,
                false /* reserved */))
        .put(
            PROPERTY_DEFAULT_LOCATION_NAME,
            PropertyEntry.stringOptionalPropertyEntry(
                PROPERTY_DEFAULT_LOCATION_NAME,
                "The default location name for the fileset",
                true /* immutable */,
                null,
                false /* hidden */))
        .putAll(KerberosConfig.KERBEROS_PROPERTY_ENTRIES)
        .putAll(AuthenticationConfig.AUTHENTICATION_PROPERTY_ENTRIES)
        .putAll(CredentialConfig.CREDENTIAL_PROPERTY_ENTRIES);
    return builder.build();
  }
}
