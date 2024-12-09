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

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.gravitino.authorization.AuthorizationPropertiesMetadata;
import org.apache.gravitino.connector.WildcardPropertiesMetadata;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestAuthorizationPropertiesMeta {
  private static final Logger LOG = LoggerFactory.getLogger(TestAuthorizationPropertiesMeta.class);

  @Test
  public void checkChainPropertyDefines() throws IllegalAccessException {
    Map<String, String> mapVariable =
        getPublicStaticVariableFromClass(AuthorizationPropertiesMetadata.class);
    List<String> ignoreChecks =
        Arrays.asList(
            AuthorizationPropertiesMetadata.CHAIN_CATALOG_PROVIDER,
            AuthorizationPropertiesMetadata.CHAIN_PROVIDER,
            AuthorizationPropertiesMetadata.getInstance().wildcardNodePropertyKey());
    mapVariable.values().stream()
        .forEach(
            value -> {
              if (!ignoreChecks.stream().anyMatch(ignore -> value.equals(ignore))
                  && value.contains(AuthorizationPropertiesMetadata.getInstance().prefixName())) {
                String pluginPropValue =
                    value.replace(
                        AuthorizationPropertiesMetadata.getInstance()
                            .getPropertyValue(WildcardPropertiesMetadata.Constants.WILDCARD, ""),
                        String.format("%s.", AuthorizationPropertiesMetadata.FIRST_SEGMENT_NAME));
                LOG.info("Checking variable: {}, pluginPropValue: {}", value, pluginPropValue);
                Assertions.assertTrue(
                    mapVariable.values().contains(pluginPropValue),
                    String.format("Variable %s is not defined in the class", value));
              }
            });
  }

  /**
   * Get all public static member variables from a class
   *
   * @param clazz The class to get public member variables from
   * @return A map of the Map<String(Variable name) to String(Variable value)>
   */
  private Map<String, String> getPublicStaticVariableFromClass(Class<?> clazz)
      throws IllegalAccessException {
    Field[] fields = clazz.getFields();
    Map<String, String> publicStaticFields = new HashMap<>();

    for (Field field : fields) {
      if (Modifier.isPublic(field.getModifiers())
          && Modifier.isStatic(field.getModifiers())
          && field.getDeclaringClass().equals(clazz)
          && field.getType().equals(String.class)) {
        publicStaticFields.put(field.getName(), field.get(null).toString());
      }
    }
    return publicStaticFields;
  }
}
