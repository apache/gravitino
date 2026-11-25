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

import com.google.common.base.Preconditions;
import org.apache.gravitino.annotation.Evolving;

/** The stored body and opaque editing token observed in one Article revision. */
@Evolving
public final class ArticleContent {
  private final ArticleBody body;
  private final String revisionToken;

  /** Creates a content response for the supplied body and revision token. */
  public ArticleContent(ArticleBody body, String revisionToken) {
    this.body = Preconditions.checkNotNull(body, "body");
    this.revisionToken = Preconditions.checkNotNull(revisionToken, "revisionToken");
  }

  /** Returns the stored text and its markup format. */
  public ArticleBody body() {
    return body;
  }

  /** Returns the token for conditional edits based on this content. */
  public String revisionToken() {
    return revisionToken;
  }
}
