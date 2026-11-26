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
 * software under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 */

package org.apache.gravitino.kb;

import javax.annotation.Nullable;
import org.apache.gravitino.Audit;
import org.apache.gravitino.annotation.Evolving;
import org.apache.gravitino.authorization.Owner;

/**
 * A lightweight view of a Knowledge Base Article used by list and reverse-lookup operations. It
 * never carries the body.
 */
@Evolving
public interface ArticleSummary {

  /**
   * @return the immutable article name
   */
  String name();

  /**
   * @return the display title
   */
  String title();

  /**
   * @return the article status
   */
  ArticleStatus status();

  /**
   * @return the Domain names visible to the caller; may be empty
   */
  String[] domains();

  /**
   * @return the direct owner (a user or group), or {@code null} if no owner is set
   */
  @Nullable
  Owner owner();

  /**
   * @return the audit information
   */
  Audit auditInfo();

  /**
   * @return the readable Terms in the preview of up to 20 readable Terms
   */
  ArticleTermBinding[] terms();

  /**
   * @return whether more readable Terms follow this preview
   */
  boolean termsHasMore();
}
