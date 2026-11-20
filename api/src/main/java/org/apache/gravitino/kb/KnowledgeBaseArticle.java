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
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.gravitino.Audit;
import org.apache.gravitino.Auditable;
import org.apache.gravitino.annotation.Evolving;

/** A metalake-scoped article with current rich-text content, Domain membership and bindings. */
@Evolving
public interface KnowledgeBaseArticle extends Auditable {

  /**
   * Validates an explicit article name and returns its NFC spelling for storage and lookup.
   *
   * @param name the article name
   * @return the normalized article name
   */
  static String normalizeName(String name) {
    return ArticleValidation.name(Preconditions.checkNotNull(name, "name"));
  }

  /** Maximum length of the immutable article name. */
  int NAME_MAX_LENGTH = 128;

  /** Maximum length of the title. */
  int TITLE_MAX_LENGTH = 512;

  /** Maximum length of the short comment. */
  int COMMENT_MAX_LENGTH = 256;

  /** Maximum length of one synonym. */
  int SYNONYM_MAX_LENGTH = 128;

  /** Maximum number of synonyms of one article. */
  int SYNONYMS_MAX_COUNT = 20;

  /** Maximum length of the source reference. */
  int SOURCE_REFERENCE_MAX_LENGTH = 1024;

  /**
   * The default maximum body size in UTF-8 bytes (1 MiB). The size is enforced by the server at
   * write time; this API does not check the body size.
   */
  long DEFAULT_MAX_BODY_BYTES = 1L << 20;

  /**
   * The configurable maximum body size, the MySQL {@code MEDIUMTEXT} capacity. The size is enforced
   * by the server at write time; this API does not check the body size.
   */
  long MAX_BODY_BYTES_LIMIT = 16_777_215L;

  /**
   * @return the immutable article name, unique within the metalake
   */
  String name();

  /**
   * @return the display title, unique among active articles in each directly assigned Domain, or in
   *     the metalake scope for unassigned articles
   */
  String title();

  /**
   * @return the rich-text body of the article
   */
  ArticleBody body();

  /**
   * @return the article status
   */
  ArticleStatus status();

  /**
   * @return the visible Domain names; may be empty
   */
  String[] domains();

  /**
   * @return the short comment, or {@code null} if not set
   */
  @Nullable
  String comment();

  /**
   * @return alternative words for the same topic; never {@code null}
   */
  String[] synonyms();

  /**
   * @return free text describing where the article came from, or {@code null} if not set
   */
  @Nullable
  String sourceReference();

  /**
   * @return custom properties of the article
   */
  Map<String, String> properties();

  /**
   * @return the visible asset bindings in the requested preview window
   */
  ArticleObjectIdentifier[] objects();

  /**
   * @return whether more visible assets follow the preview
   */
  boolean objectsHasMore();

  /**
   * @return the audit information
   */
  @Override
  Audit auditInfo();
}
