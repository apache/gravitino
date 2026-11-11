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
package org.apache.gravitino.glossary;

import com.google.common.base.Preconditions;
import java.util.Objects;
import javax.annotation.Nullable;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.annotation.Evolving;

/** A documentation link of a glossary term: a label and a URL. */
@Evolving
public final class TermLink {

  @Nullable private final String label;
  private final String url;

  private TermLink(@Nullable String label, String url) {
    this.label = label;
    this.url = url;
  }

  /**
   * Creates a documentation link.
   *
   * @param label The label shown for the link, or null to show the URL.
   * @param url The URL of the link.
   * @return The link.
   * @throws IllegalArgumentException If the URL is blank.
   */
  public static TermLink of(@Nullable String label, String url) {
    Preconditions.checkArgument(StringUtils.isNotBlank(url), "Link URL cannot be blank");
    return new TermLink(label, url);
  }

  /**
   * Returns the label of the link.
   *
   * @return The label of the link, or null if it is not set.
   */
  @Nullable
  public String label() {
    return label;
  }

  /**
   * Returns the URL of the link.
   *
   * @return The URL of the link.
   */
  public String url() {
    return url;
  }

  @Override
  public boolean equals(@Nullable Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof TermLink)) {
      return false;
    }
    TermLink that = (TermLink) o;
    return Objects.equals(label, that.label) && Objects.equals(url, that.url);
  }

  @Override
  public int hashCode() {
    return Objects.hash(label, url);
  }

  @Override
  public String toString() {
    return "TermLink{label=" + label + ", url=" + url + "}";
  }
}
