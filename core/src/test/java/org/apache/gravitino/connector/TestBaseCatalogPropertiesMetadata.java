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
package org.apache.gravitino.connector;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.common.collect.ImmutableMap;
import java.util.Collections;
import java.util.Map;
import org.apache.gravitino.credential.CredentialConstants;
import org.apache.gravitino.credential.config.CredentialConfig;
import org.junit.jupiter.api.Test;

public class TestBaseCatalogPropertiesMetadata {

  private final PropertiesMetadata emptySpecific =
      new BaseCatalogPropertiesMetadata() {
        @Override
        protected Map<String, PropertyEntry<?>> specificPropertyEntries() {
          return Collections.emptyMap();
        }
      };

  private final PropertiesMetadata withCredentials =
      new BaseCatalogPropertiesMetadata() {
        @Override
        protected Map<String, PropertyEntry<?>> specificPropertyEntries() {
          return ImmutableMap.copyOf(CredentialConfig.CREDENTIAL_PROPERTY_ENTRIES);
        }
      };

  @Test
  void testCredentialPropertyEntriesAreNotInjectedByBase() {
    assertFalse(emptySpecific.containsProperty(CredentialConstants.CREDENTIAL_PROVIDERS));
    assertFalse(emptySpecific.containsProperty(CredentialConstants.S3_TOKEN_EXPIRE_IN_SECS));
  }

  @Test
  void testCatalogCanDeclareCredentialPropertyEntries() {
    assertTrue(withCredentials.containsProperty(CredentialConstants.CREDENTIAL_PROVIDERS));
    assertTrue(withCredentials.containsProperty(CredentialConstants.S3_TOKEN_EXPIRE_IN_SECS));
    assertFalse(withCredentials.isHiddenProperty(CredentialConstants.CREDENTIAL_PROVIDERS));
    assertFalse(withCredentials.isHiddenProperty(CredentialConstants.S3_TOKEN_EXPIRE_IN_SECS));
  }
}
