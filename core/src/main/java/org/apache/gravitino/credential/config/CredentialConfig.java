/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */

package org.apache.gravitino.credential.config;

import com.google.common.collect.ImmutableMap;
import java.util.Map;
import org.apache.gravitino.connector.PropertyEntry;
import org.apache.gravitino.credential.CredentialConstants;

public class CredentialConfig {

  public static final Map<String, PropertyEntry<?>> CREDENTIAL_PROPERTY_ENTRIES =
      new ImmutableMap.Builder<String, PropertyEntry<?>>()
          .put(
              CredentialConstants.CREDENTIAL_PROVIDERS,
              PropertyEntry.booleanPropertyEntry(
                  CredentialConstants.CREDENTIAL_PROVIDERS,
                  "Credential providers for the Gravitino catalog, schema, fileset, table, etc.",
                  false /* required */,
                  false /* immutable */,
                  null /* default value */,
                  false /* hidden */,
                  false /* reserved */))
          .build();
}
