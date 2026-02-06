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

package org.apache.gravitino.auth;

/** Factory class for creating {@link PrincipalMapper} instances. */
public class PrincipalMapperFactory {

  private PrincipalMapperFactory() {}

  /**
   * Creates a principal mapper based on the type and configuration.
   *
   * @param mapperType "regex" for built-in regex mapper, or fully qualified class name for custom
   *     mapper
   * @param regexPattern the regex pattern (only used when mapperType is "regex")
   * @return a configured PrincipalMapper instance
   * @throws IllegalArgumentException if the mapper cannot be created
   */
  public static PrincipalMapper create(String mapperType, String regexPattern) {
    if ("regex".equalsIgnoreCase(mapperType)) {
      return new RegexPrincipalMapper(regexPattern);
    }

    // Load custom mapper class
    try {
      Class<?> clazz = Class.forName(mapperType);
      if (!PrincipalMapper.class.isAssignableFrom(clazz)) {
        throw new IllegalArgumentException(
            "Class " + mapperType + " does not implement PrincipalMapper");
      }
      return (PrincipalMapper) clazz.getDeclaredConstructor().newInstance();
    } catch (ClassNotFoundException e) {
      throw new IllegalArgumentException("Unknown principal mapper type: " + mapperType, e);
    } catch (Exception e) {
      throw new IllegalArgumentException(
          "Failed to instantiate principal mapper: " + mapperType, e);
    }
  }
}
