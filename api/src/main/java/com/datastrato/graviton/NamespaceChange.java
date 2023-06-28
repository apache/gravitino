/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// Referred from Apache's connector/catalog implementation
// sql/catalyst/src/main/java/org/apache/spark/sql/connector/catalog/NamespaceChange.java

package com.datastrato.graviton;

import lombok.EqualsAndHashCode;
import lombok.Getter;

public interface NamespaceChange {

  /**
   * NamespaceChange class to set the property and value for the namespace.
   *
   * @param property the property name to set
   * @param value the property value to set
   * @return the NamespaceChange object
   */
  static NamespaceChange setProperty(String property, String value) {
    return new SetProperty(property, value);
  }

  /**
   * NamespaceChange class to remove the property from the namespace.
   *
   * @param property the property name to remove
   * @return the NamespaceChange object
   */
  static NamespaceChange removeProperty(String property) {
    return new RemoveProperty(property);
  }

  @Getter
  @EqualsAndHashCode
  final class SetProperty implements NamespaceChange {
    private final String property;
    private final String value;

    private SetProperty(String property, String value) {
      this.property = property;
      this.value = value;
    }
  }

  @Getter
  @EqualsAndHashCode
  final class RemoveProperty implements NamespaceChange {
    private final String property;

    private RemoveProperty(String property) {
      this.property = property;
    }
  }
}
