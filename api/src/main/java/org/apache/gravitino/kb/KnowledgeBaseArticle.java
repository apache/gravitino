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

import java.util.Map;
import javax.annotation.Nullable;
import org.apache.gravitino.Audit;
import org.apache.gravitino.Auditable;
import org.apache.gravitino.annotation.Evolving;

/**
 * A Knowledge Base Article: a metalake-scoped, versioned rich-text document that can belong to
 * Domains and can be bound to Glossary Terms and metadata objects.
 *
 * <p>The {@link #title()}, {@link #body()}, {@link #domains()} and the bindings belong to the live
 * version; {@link #version()} is its number. Other fields are not versioned.
 */
@Evolving
public interface KnowledgeBaseArticle extends Auditable {

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

  /** The default maximum body size in UTF-8 bytes (1 MiB). */
  long DEFAULT_MAX_BODY_BYTES = 1L << 20;

  /** The configurable maximum body size, the MySQL {@code MEDIUMTEXT} capacity. */
  long MAX_BODY_BYTES_LIMIT = 16_777_215L;

  /**
   * @return the immutable article name, unique within the metalake
   */
  String name();

  /**
   * @return the display title, unique among non-deprecated articles within the metalake
   */
  String title();

  /**
   * @return the rich-text body of the live version
   */
  ArticleBody body();

  /**
   * @return the article status
   */
  ArticleStatus status();

  /**
   * @return the Domain names of the live version; may be empty
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
   * @return the current content version, starting at one
   */
  int version();

  /**
   * @return the audit information
   */
  @Override
  Audit auditInfo();
}
