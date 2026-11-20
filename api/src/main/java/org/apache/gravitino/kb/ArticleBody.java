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
import java.util.Objects;
import org.apache.gravitino.annotation.Evolving;

/**
 * The rich-text body of a Knowledge Base Article. The server preserves Markdown source and
 * sanitizes HTML on write. Consumers sanitize rendered HTML before display.
 */
@Evolving
public final class ArticleBody {

  /** The markup format of an article body. */
  public enum Format {
    /** Markdown source. Render embedded HTML as escaped text. */
    MARKDOWN,

    /** HTML text. */
    HTML
  }

  private final Format format;
  private final String content;

  /**
   * Creates an article body.
   *
   * @param format the markup format
   * @param content the non-empty body text
   * @return the article body
   */
  public static ArticleBody of(Format format, String content) {
    return new ArticleBody(format, content);
  }

  private ArticleBody(Format format, String content) {
    Preconditions.checkArgument(format != null, "Article body format must not be null");
    Preconditions.checkArgument(
        content != null && !content.isEmpty(), "Article body content must not be null or empty");
    this.format = format;
    this.content = content;
  }

  /**
   * @return the markup format of the body
   */
  public Format format() {
    return format;
  }

  /**
   * @return the body text in {@link #format()}
   */
  public String content() {
    return content;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ArticleBody that = (ArticleBody) o;
    return format == that.format && Objects.equals(content, that.content);
  }

  @Override
  public int hashCode() {
    return Objects.hash(format, content);
  }

  @Override
  public String toString() {
    return "ArticleBody{format=" + format + ", contentLength=" + content.length() + "}";
  }
}
