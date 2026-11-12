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

package org.apache.gravitino.secret;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import java.util.Map;
import java.util.Objects;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.annotation.Evolving;

/**
 * External secret locator for create/alter requests: a registered provider instance name plus
 * provider-specific attributes. The server builds the URN; clients must not send a raw URN.
 */
@Evolving
public final class SecretReference {

  private final String provider;
  private final Map<String, String> attributes;

  /**
   * Creates an external secret reference.
   *
   * @param provider registered provider instance name
   * @param attributes provider-specific locator keys; must be non-null and non-empty
   */
  public SecretReference(String provider, Map<String, String> attributes) {
    Preconditions.checkArgument(StringUtils.isNotBlank(provider), "provider must not be blank");
    Preconditions.checkArgument(
        attributes != null && !attributes.isEmpty(), "attributes must not be null or empty");
    this.provider = provider;
    this.attributes = ImmutableMap.copyOf(attributes);
  }

  /**
   * Returns the registered provider instance name.
   *
   * @return the provider name
   */
  public String provider() {
    return provider;
  }

  /**
   * Returns provider-specific locator attributes (never {@code null} or empty).
   *
   * @return an unmodifiable attributes map
   */
  public Map<String, String> attributes() {
    return attributes;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof SecretReference)) {
      return false;
    }
    SecretReference that = (SecretReference) o;
    return Objects.equals(provider, that.provider) && Objects.equals(attributes, that.attributes);
  }

  @Override
  public int hashCode() {
    return Objects.hash(provider, attributes);
  }

  @Override
  public String toString() {
    return "SecretReference{provider='" + provider + "', attributes=" + attributes + "}";
  }
}
