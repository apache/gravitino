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
package org.apache.gravitino.semantic;

import com.google.common.base.Preconditions;
import java.util.Objects;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.annotation.Evolving;

/**
 * An immutable standalone Apache Ossie document containing serialized content and its format.
 *
 * <p>The content is the document text, not a file path or URL, and must not be blank. Constructing
 * this value does not parse the content; document syntax and structure are validated when imported.
 */
@Evolving
public final class OssieDocument {

  private final String content;
  private final OssieFormat format;

  private OssieDocument(String content, OssieFormat format) {
    Preconditions.checkArgument(StringUtils.isNotBlank(content), "content must not be blank");
    this.content = content;
    this.format = format;
  }

  /**
   * Creates a document whose content is serialized as YAML.
   *
   * @param content The document text.
   * @return The YAML document.
   * @throws IllegalArgumentException If the content is null, empty, or whitespace-only.
   */
  public static OssieDocument yaml(String content) {
    return new OssieDocument(content, OssieFormat.YAML);
  }

  /**
   * Creates a document whose content is serialized as JSON.
   *
   * @param content The document text.
   * @return The JSON document.
   * @throws IllegalArgumentException If the content is null, empty, or whitespace-only.
   */
  public static OssieDocument json(String content) {
    return new OssieDocument(content, OssieFormat.JSON);
  }

  /**
   * Returns the serialized document content.
   *
   * @return The document text.
   */
  public String content() {
    return content;
  }

  /**
   * Returns the document's serialization format.
   *
   * @return The serialization format.
   */
  public OssieFormat format() {
    return format;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    OssieDocument that = (OssieDocument) o;
    return content.equals(that.content) && format == that.format;
  }

  @Override
  public int hashCode() {
    return Objects.hash(content, format);
  }
}
