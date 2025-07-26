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
package org.apache.gravitino.policy;

import java.util.Map;
import java.util.Objects;

/** Utility class for creating instances of {@link PolicyContent}. */
public class PolicyContents {

  /**
   * Creates a custom policy content with the given rules and properties.
   *
   * @param rules The custom rules of the policy.
   * @param properties The additional properties of the policy.
   * @return A new instance of {@link PolicyContent} with the specified rules and properties.
   */
  public static PolicyContent custom(Map<String, Object> rules, Map<String, String> properties) {
    return new CustomContent(rules, properties);
  }

  private PolicyContents() {}

  /**
   * A custom content implementation of {@link PolicyContent} that holds custom rules and
   * properties.
   */
  public static class CustomContent implements PolicyContent {
    private final Map<String, Object> customRules;
    private final Map<String, String> properties;

    /** Default constructor for Jackson deserialization only. */
    private CustomContent() {
      this(null, null);
    }

    /**
     * Constructor for CustomContent.
     *
     * @param customRules the custom rules of the policy
     * @param properties the additional properties of the policy
     */
    private CustomContent(Map<String, Object> customRules, Map<String, String> properties) {
      this.customRules = customRules;
      this.properties = properties;
    }

    /**
     * Returns the custom rules of the policy.
     *
     * @return a map of custom rules
     */
    public Map<String, Object> customRules() {
      return customRules;
    }

    @Override
    public Map<String, String> properties() {
      return properties;
    }

    @Override
    public void validate() throws IllegalArgumentException {
      // nothing to validate for custom content
    }

    @Override
    public boolean equals(Object o) {
      if (!(o instanceof CustomContent)) return false;
      CustomContent that = (CustomContent) o;
      return Objects.equals(customRules, that.customRules)
          && Objects.equals(properties, that.properties);
    }

    @Override
    public int hashCode() {
      return Objects.hash(customRules, properties);
    }

    @Override
    public String toString() {
      return "CustomContent{" + "customRules=" + customRules + ", properties=" + properties + '}';
    }
  }
}
