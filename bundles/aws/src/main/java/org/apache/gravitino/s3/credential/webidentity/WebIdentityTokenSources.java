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
package org.apache.gravitino.s3.credential.webidentity;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.ServiceLoader;
import org.apache.commons.lang3.StringUtils;

/** Factory for resolving and initializing a {@link WebIdentityTokenSource} from properties. */
public final class WebIdentityTokenSources {

  /**
   * Resolve the {@link WebIdentityTokenSource} configured by {@code s3-web-identity-token-source}
   * (defaults to {@code file}) and initialize it with the given properties.
   *
   * <p>Sources are discovered through {@link ServiceLoader}, so additional implementations can be
   * registered by listing them under {@code
   * META-INF/services/org.apache.gravitino.s3.credential.webidentity.WebIdentityTokenSource}.
   */
  public static WebIdentityTokenSource create(Map<String, String> properties) {
    return create(properties, ServiceLoader.load(WebIdentityTokenSource.class));
  }

  /**
   * Package-private overload that takes the candidate sources directly, enabling tests to exercise
   * the resolution logic (in particular duplicate-name detection) without touching the {@link
   * ServiceLoader} registry.
   */
  static WebIdentityTokenSource create(
      Map<String, String> properties, Iterable<WebIdentityTokenSource> candidates) {
    String type =
        StringUtils.defaultIfBlank(
            properties.get(WebIdentityTokenSourceConfig.SOURCE),
            WebIdentityTokenSourceConfig.DEFAULT_SOURCE);

    List<String> available = new ArrayList<>();
    List<WebIdentityTokenSource> matched = new ArrayList<>();
    Map<String, WebIdentityTokenSource> sourcesByName = new HashMap<>();
    for (WebIdentityTokenSource candidate : candidates) {
      String candidateName = candidate.name();
      if (StringUtils.isBlank(candidateName)) {
        throw new IllegalStateException(
            "WebIdentity token source "
                + candidate.getClass().getName()
                + " must return a non-blank name().");
      }
      available.add(candidateName);
      String normalizedName = candidateName.toLowerCase(Locale.ROOT);
      WebIdentityTokenSource existing = sourcesByName.putIfAbsent(normalizedName, candidate);
      if (existing != null) {
        throw new IllegalStateException(
            "Multiple WebIdentity token sources registered with name '"
                + existing.name()
                + "': "
                + existing.getClass().getName()
                + ", "
                + candidate.getClass().getName()
                + ". Each implementation must return a unique name().");
      }
      if (type.equalsIgnoreCase(candidateName)) {
        matched.add(candidate);
      }
    }
    if (matched.isEmpty()) {
      throw new IllegalArgumentException(
          "Unknown WebIdentity token source: "
              + type
              + ". Available sources: "
              + String.join(", ", available));
    }
    if (matched.size() > 1) {
      List<String> classes = new ArrayList<>();
      for (WebIdentityTokenSource source : matched) {
        classes.add(source.getClass().getName());
      }
      throw new IllegalStateException(
          "Multiple WebIdentity token sources registered with name '"
              + type
              + "': "
              + String.join(", ", classes)
              + ". Each implementation must return a unique name().");
    }
    WebIdentityTokenSource source = matched.get(0);
    source.initialize(properties);
    return source;
  }

  private WebIdentityTokenSources() {}
}
