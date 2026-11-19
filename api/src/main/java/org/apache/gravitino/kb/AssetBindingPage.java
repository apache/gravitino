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

package org.apache.gravitino.kb;

import com.google.common.collect.ImmutableList;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.gravitino.annotation.Evolving;

/** A bounded page of visible assets bound to an article. */
@Evolving
public final class AssetBindingPage {
  private final List<ArticleObjectIdentifier> objects;
  private final boolean hasMore;
  private final String revisionToken;

  /**
   * Creates a page of visible bindings.
   *
   * @param objects the bindings in this window
   * @param hasMore whether another readable binding follows
   * @param revisionToken the new opaque token after a binding write, or null for a list response
   */
  public AssetBindingPage(
      List<ArticleObjectIdentifier> objects, boolean hasMore, @Nullable String revisionToken) {
    this.objects = ImmutableList.copyOf(objects);
    this.hasMore = hasMore;
    this.revisionToken = revisionToken;
  }

  /**
   * @return the visible bindings on this page
   */
  public List<ArticleObjectIdentifier> objects() {
    return objects;
  }

  /**
   * @return whether more visible bindings follow this page
   */
  public boolean hasMore() {
    return hasMore;
  }

  /**
   * @return the article revision after a binding mutation, or null for a list response
   */
  @Nullable
  public String revisionToken() {
    return revisionToken;
  }
}
