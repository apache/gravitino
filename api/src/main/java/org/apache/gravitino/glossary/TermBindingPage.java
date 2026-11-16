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
import java.util.Arrays;
import java.util.Optional;
import javax.annotation.Nullable;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.annotation.Evolving;

/**
 * One bounded page of authorized term bindings and an optional continuation.
 *
 * <p>An empty page can have a continuation when only hidden candidates were examined. A token means
 * scanning can continue, not that another visible binding exists. Only an absent token means the
 * scan is complete. Tokens are opaque and grant no access to the term or its targets.
 */
@Evolving
public final class TermBindingPage {

  /** The visible-result size used by the first-page convenience method. */
  public static final int DEFAULT_PAGE_SIZE = 100;

  /** The maximum number of visible bindings in a page. */
  public static final int MAX_PAGE_SIZE = 100;

  private final TermBinding[] bindings;
  @Nullable private final String nextPageToken;

  private TermBindingPage(TermBinding[] bindings, @Nullable String nextPageToken) {
    this.bindings = bindings.clone();
    this.nextPageToken = nextPageToken;
  }

  /**
   * Creates a page, copying its bindings.
   *
   * @param bindings The visible bindings, possibly empty, with no null elements.
   * @param nextPageToken The opaque continuation, or null when scanning is complete.
   * @return The page.
   * @throws IllegalArgumentException If bindings are null, exceed the page limit or contain null,
   *     or a non-null continuation is blank.
   */
  public static TermBindingPage of(TermBinding[] bindings, @Nullable String nextPageToken) {
    Preconditions.checkArgument(bindings != null, "Bindings cannot be null");
    Preconditions.checkArgument(
        bindings.length <= MAX_PAGE_SIZE, "A binding page cannot exceed %s results", MAX_PAGE_SIZE);
    Preconditions.checkArgument(
        Arrays.stream(bindings).allMatch(binding -> binding != null),
        "Binding elements cannot be null");
    Preconditions.checkArgument(
        nextPageToken == null || StringUtils.isNotBlank(nextPageToken),
        "A continuation token cannot be blank");
    return new TermBindingPage(bindings, nextPageToken);
  }

  /**
   * Returns the authorized bindings in this page.
   *
   * @return A copy of the bindings; an empty array does not prove exhaustion when a token exists.
   */
  public TermBinding[] bindings() {
    return bindings.clone();
  }

  /**
   * Returns the continuation for the next page, if scanning is not complete.
   *
   * <p>Pass it unchanged with the same filters. If reading stops early, report an incomplete result
   * rather than treating this page as the complete binding list.
   *
   * @return The continuation, or empty only when scanning is complete.
   */
  public Optional<String> nextPageToken() {
    return Optional.ofNullable(nextPageToken);
  }
}
