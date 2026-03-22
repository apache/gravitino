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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Factory class for creating {@link GroupMapper} instances. */
public class GroupMapperFactory {

  private static final Logger LOG = LoggerFactory.getLogger(GroupMapperFactory.class);

  private GroupMapperFactory() {}

  /**
   * Creates a GroupMapper instance based on the configuration.
   *
   * @param mapperType the type of the mapper (e.g., "regex" or fully qualified class name)
   * @param regexPattern the regex pattern to use (only for "regex" mapper type)
   * @return a configured GroupMapper instance
   * @throws IllegalArgumentException if the mapper type is invalid or initialization fails
   */
  public static GroupMapper create(String mapperType, String regexPattern) {
    if ("regex".equalsIgnoreCase(mapperType)) {
      if (regexPattern == null) {
        throw new IllegalArgumentException("Regex pattern cannot be null for regex mapper");
      }
      return new RegexGroupMapper(regexPattern);
    }

    try {
      Class<?> clazz = Class.forName(mapperType);
      if (!GroupMapper.class.isAssignableFrom(clazz)) {
        throw new IllegalArgumentException(
            "Class " + mapperType + " does not implement GroupMapper");
      }
      return (GroupMapper) clazz.getDeclaredConstructor().newInstance();
    } catch (Exception e) {
      LOG.error("Failed to create GroupMapper: {}", mapperType, e);
      throw new IllegalArgumentException("Failed to create GroupMapper: " + mapperType, e);
    }
  }
}
