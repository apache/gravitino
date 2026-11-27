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
import com.google.common.base.Utf8;
import java.text.Normalizer;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Supplier;
import java.util.regex.Pattern;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.utils.PrincipalUtils;
import org.owasp.html.PolicyFactory;
import org.owasp.html.Sanitizers;

/** Content transformations applied to the complete current Article. */
final class ArticleEdits {
  private static final Pattern SEPARATORS = Pattern.compile("[^\\p{L}\\p{Nd}\\p{M}]+");
  private static final PolicyFactory HTML =
      Sanitizers.FORMATTING
          .and(Sanitizers.BLOCKS)
          .and(Sanitizers.LINKS)
          .and(Sanitizers.IMAGES)
          .and(Sanitizers.TABLES);

  private final int maxBodyBytes;

  ArticleEdits(int maxBodyBytes) {
    Preconditions.checkArgument(
        maxBodyBytes > 0 && maxBodyBytes <= KnowledgeBaseArticle.MAX_BODY_BYTES_LIMIT,
        "Invalid Article body size limit: %s",
        maxBodyBytes);
    this.maxBodyBytes = maxBodyBytes;
  }

  static String generatedName(String title, long attempt) {
    String slug =
        Normalizer.normalize(
                SEPARATORS.matcher(title.toLowerCase(Locale.ROOT)).replaceAll("-"),
                Normalizer.Form.NFC)
            .replaceAll("^-|-$", "");
    int baseLength =
        Math.min(slug.codePointCount(0, slug.length()), KnowledgeBaseArticle.NAME_MAX_LENGTH);
    String base = slug.substring(0, slug.offsetByCodePoints(0, baseLength));
    if (base.codePoints().anyMatch(Character::isLetterOrDigit)) {
      String suffix = attempt == 0 ? "" : "-" + (attempt + 1);
      int length = Math.min(baseLength, KnowledgeBaseArticle.NAME_MAX_LENGTH - suffix.length());
      return base.substring(0, base.offsetByCodePoints(0, length)) + suffix;
    }
    StringBuilder fallback = new StringBuilder("article-");
    for (int i = 0; i < 8; i++) {
      fallback.append(Character.forDigit(ThreadLocalRandom.current().nextInt(36), 36));
    }
    return fallback.toString();
  }

  KnowledgeBaseArticleEntity create(
      String metalake, long id, String name, ArticleCreate request, Supplier<ArticleBody> body) {
    Preconditions.checkArgument(
        request.domains().isEmpty() && request.terms().isEmpty() && request.objects().isEmpty(),
        "Initial Article relations require endpoint resolution");
    return KnowledgeBaseArticleEntity.builder()
        .withNamespace(Namespace.of(metalake))
        .withId(id)
        .withName(name)
        .withTitle(request.title())
        .withBodyUpdate(body.get())
        .withStatus(request.status())
        .withComment(request.comment())
        .withSynonyms(request.synonyms())
        .withSourceReference(request.sourceReference())
        .withProperties(request.properties())
        .withAuditInfo(
            AuditInfo.builder()
                .withCreator(PrincipalUtils.getCurrentUserName())
                .withCreateTime(Instant.now())
                .build())
        .build();
  }

  KnowledgeBaseArticleEntity apply(
      KnowledgeBaseArticleEntity current, KnowledgeBaseArticleChange[] changes) {
    var builder = current.toBuilder().withBodyUpdate(null);
    Map<String, String> properties = new HashMap<>(current.properties());
    for (KnowledgeBaseArticleChange change : changes) {
      if (change instanceof KnowledgeBaseArticleChange.UpdateTitle title) {
        builder.withTitle(title.getNewTitle());
      } else if (change instanceof KnowledgeBaseArticleChange.UpdateBody body) {
        builder.withBodyUpdate(storedBody(body.getNewBody()));
      } else if (change instanceof KnowledgeBaseArticleChange.UpdateStatus status) {
        builder.withStatus(status.getNewStatus());
      } else if (change instanceof KnowledgeBaseArticleChange.UpdateComment comment) {
        builder.withComment(comment.getNewComment());
      } else if (change instanceof KnowledgeBaseArticleChange.UpdateSynonyms synonyms) {
        builder.withSynonyms(List.of(synonyms.getNewSynonyms()));
      } else if (change instanceof KnowledgeBaseArticleChange.UpdateSourceReference source) {
        builder.withSourceReference(source.getNewSourceReference());
      } else if (change instanceof KnowledgeBaseArticleChange.SetProperty property) {
        properties.put(property.getProperty(), property.getValue());
      } else if (change instanceof KnowledgeBaseArticleChange.RemoveProperty property) {
        properties.remove(property.getProperty());
      } else {
        throw new IllegalArgumentException("Unsupported Article change: " + change);
      }
    }
    return builder
        .withProperties(properties)
        .withAuditInfo(
            AuditInfo.builder()
                .withCreator(current.auditInfo().creator())
                .withCreateTime(current.auditInfo().createTime())
                .withLastModifier(PrincipalUtils.getCurrentUserName())
                .withLastModifiedTime(Instant.now())
                .build())
        .build();
  }

  ArticleBody storedBody(ArticleBody body) {
    checkSize(body.content());
    if (body.format() == ArticleBody.Format.MARKDOWN) {
      return body;
    }
    String cleaned = HTML.sanitize(body.content());
    checkSize(cleaned);
    return ArticleBody.of(ArticleBody.Format.HTML, cleaned);
  }

  private void checkSize(String content) {
    Preconditions.checkArgument(
        content.length() <= maxBodyBytes && Utf8.encodedLength(content) <= maxBodyBytes,
        "Article body exceeds %s UTF-8 bytes",
        maxBodyBytes);
  }
}
