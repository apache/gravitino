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

import static org.apache.gravitino.file.Fileset.LOCATION_NAME_UNKNOWN;
import static org.apache.gravitino.file.Fileset.PROPERTY_MULTIPLE_LOCATIONS_PREFIX;

import com.google.common.collect.ImmutableMap;
import java.util.Map;
import org.apache.gravitino.catalog.fileset.authentication.AuthenticationConfig;
import org.apache.gravitino.catalog.fileset.authentication.kerberos.KerberosConfig;
import org.apache.gravitino.connector.BasePropertiesMetadata;
import org.apache.gravitino.connector.PropertyEntry;
import org.apache.gravitino.credential.config.CredentialConfig;

public class FilesetSchemaPropertiesMetadata extends BasePropertiesMetadata {

  // Property "location" is used to specify the schema's storage location managed by Hadoop fileset
  // catalog.
  // If specified, the property "location" specified in the catalog level will be overridden. Also,
  // the location will be used as the default storage location for all the managed filesets created
  // under this schema.
  //
  // If not specified, the property "location" specified in the catalog level will be used (if
  // specified), or users have to specify the storage location in the Fileset level.
  public static final String LOCATION = "location";

  private static final Map<String, PropertyEntry<?>> FILESET_SCHEMA_ENTRIES =
      ImmutableMap.<String, PropertyEntry<?>>builder()
          .put(
              LOCATION,
              PropertyEntry.stringOptionalPropertyEntry(
                  LOCATION,
                  "The storage location managed by Fileset schema",
                  true /* immutable */,
                  null,
                  false /* hidden */))
          .put(
              PROPERTY_MULTIPLE_LOCATIONS_PREFIX + LOCATION_NAME_UNKNOWN,
              PropertyEntry.stringReservedPropertyEntry(
                  PROPERTY_MULTIPLE_LOCATIONS_PREFIX + LOCATION_NAME_UNKNOWN,
                  "The storage location equivalent to property 'location'",
                  true /* hidden */))
          .put(
              PROPERTY_MULTIPLE_LOCATIONS_PREFIX,
              PropertyEntry.stringImmutablePropertyPrefixEntry(
                  PROPERTY_MULTIPLE_LOCATIONS_PREFIX,
                  "The prefix of the location name",
                  false /* required */,
                  null /* default value */,
                  false /* hidden */,
                  false /* reserved */))
          .putAll(KerberosConfig.KERBEROS_PROPERTY_ENTRIES)
          .putAll(AuthenticationConfig.AUTHENTICATION_PROPERTY_ENTRIES)
          .putAll(CredentialConfig.CREDENTIAL_PROPERTY_ENTRIES)
          .build();

  @Override
  protected Map<String, PropertyEntry<?>> specificPropertyEntries() {
    return FILESET_SCHEMA_ENTRIES;
  }
}
