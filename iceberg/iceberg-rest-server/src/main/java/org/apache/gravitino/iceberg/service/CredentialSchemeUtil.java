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

package org.apache.gravitino.iceberg.service;

import com.google.common.collect.ImmutableMap;
import java.net.URI;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import org.apache.commons.lang3.StringUtils;

/**
 * Maps URI schemes to the credential-type prefix used by Gravitino's credential providers.
 *
 * <p>Credential providers declare a {@code credentialType()} like {@code "s3-token"} or {@code
 * "gcs-token"}. The portion before the first hyphen identifies the cloud or storage family. This
 * utility translates a URI scheme to the corresponding family prefix so that {@link
 * CatalogWrapperForREST} can dispatch credential requests to the correct provider when a catalog is
 * configured with more than one.
 *
 * <p>Notable normalizations:
 *
 * <ul>
 *   <li>{@code s3}, {@code s3a}, and {@code s3n} all map to the {@code s3} family. S3A and S3N are
 *       Hadoop filesystem schemes that talk to the same backend with the same credentials.
 *   <li>Azure is split into two distinct families. {@code abfs} and {@code abfss} (ADLS Gen2) map
 *       to {@code adls}, while {@code wasb} and {@code wasbs} (Blob Storage) map to {@code azure}.
 *       These use different credential providers and must not be merged.
 *   <li>Local schemes ({@code file}, no scheme) and {@code hdfs} are not cloud-backed and return
 *       empty — they do not require vended credentials.
 * </ul>
 */
public final class CredentialSchemeUtil {

  private CredentialSchemeUtil() {}

  private static final Map<String, String> SCHEME_TO_PREFIX =
      ImmutableMap.<String, String>builder()
          .put("s3", "s3")
          .put("s3a", "s3")
          .put("s3n", "s3")
          .put("gs", "gcs")
          .put("gcs", "gcs")
          .put("abfs", "adls")
          .put("abfss", "adls")
          .put("wasb", "azure")
          .put("wasbs", "azure")
          .put("oss", "oss")
          .build();

  /**
   * Returns the credential-type prefix corresponding to the scheme of the given location, or {@link
   * Optional#empty()} if the scheme is unrecognized, local, HDFS, or absent.
   *
   * @param location a URI-formatted location, e.g. {@code "s3://bucket/path"}
   * @return the matching credential-type prefix (e.g. {@code "s3"}, {@code "gcs"}, {@code "adls"}),
   *     or empty when the scheme does not correspond to a cloud-backed credential family
   */
  public static Optional<String> credentialTypePrefixFor(String location) {
    if (StringUtils.isBlank(location)) {
      return Optional.empty();
    }
    String scheme;
    try {
      scheme = URI.create(location).getScheme();
    } catch (IllegalArgumentException e) {
      return Optional.empty();
    }
    if (scheme == null) {
      return Optional.empty();
    }
    return Optional.ofNullable(SCHEME_TO_PREFIX.get(scheme.toLowerCase(Locale.ROOT)));
  }
}
