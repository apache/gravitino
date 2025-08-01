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

package org.apache.gravitino.server.authentication;

import org.apache.gravitino.Config;

/**
 * Factory for creating OAuth token validators based on configuration.
 *
 * <p>Creates validators using the {@code tokenValidatorClass} configuration.
 */
public class OAuthTokenValidatorFactory {
  /**
   * Create and initialize a token validator based on configuration.
   *
   * @param config The configuration object
   * @return An initialized OAuthTokenValidator
   * @throws IllegalArgumentException if configuration is invalid
   */
  public static OAuthTokenValidator createValidator(Config config) {
    if (config == null) {
      throw new IllegalArgumentException("Configuration cannot be null");
    }

    String validatorClass = config.get(OAuthConfig.TOKEN_VALIDATOR_CLASS);
    OAuthTokenValidator validator = createValidatorFromClass(validatorClass);

    validator.initialize(config);
    return validator;
  }

  private static OAuthTokenValidator createValidatorFromClass(String className) {
    try {
      Class<?> clazz = Class.forName(className);
      if (!OAuthTokenValidator.class.isAssignableFrom(clazz)) {
        throw new IllegalArgumentException(
            "Class " + className + " does not implement OAuthTokenValidator");
      }
      return (OAuthTokenValidator) clazz.getDeclaredConstructor().newInstance();
    } catch (Exception e) {
      throw new IllegalArgumentException(
          "Failed to create validator instance from class: " + className, e);
    }
  }
}
