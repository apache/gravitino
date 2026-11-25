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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.gravitino.Config;
import org.apache.gravitino.Configs;
import org.junit.jupiter.api.Test;

class TestArticleEdits {
  @Test
  void generatedNamesFollowUnicodeAndCodePointBoundaries() {
    assertEquals("收入-calculation-guide", ArticleEdits.generatedName("收入 Calculation Guide", 0));
    assertEquals("café", ArticleEdits.generatedName("!CAFE\u0301!", 0));
    assertTrue(ArticleEdits.generatedName("👩‍💻!", 3).matches("article-[0-9a-z]{8}"));
    String longTitle = "𠀀".repeat(129);
    assertEquals("𠀀".repeat(128), ArticleEdits.generatedName(longTitle, 0));
    assertEquals("𠀀".repeat(126) + "-2", ArticleEdits.generatedName(longTitle, 1));
  }

  @Test
  void configBounds() {
    Config config = new Config(false) {};
    assertEquals(1_048_576, config.get(Configs.KB_ARTICLE_MAX_BODY_BYTES));
    for (int limit : new int[] {1, 16_777_215}) {
      config.set(Configs.KB_ARTICLE_MAX_BODY_BYTES, limit);
      assertEquals(limit, config.get(Configs.KB_ARTICLE_MAX_BODY_BYTES));
    }
    for (int limit : new int[] {0, 16_777_216}) {
      config.set(Configs.KB_ARTICLE_MAX_BODY_BYTES, limit);
      assertThrows(
          IllegalArgumentException.class, () -> config.get(Configs.KB_ARTICLE_MAX_BODY_BYTES));
    }
  }

  @Test
  void preservesMarkdownAndSanitizesHtmlOnBodyWritesOnly() {
    ArticleBody markdown = ArticleBody.of(ArticleBody.Format.MARKDOWN, "# Guide\n\n<b>raw</b>");
    ArticleEdits edits = new ArticleEdits(1024);
    assertEquals(markdown, create(edits, markdown).bodyUpdate());
    String html =
        "<p onclick=\"read()\"><b>Guide</b><script>call()</script></p>"
            + "<a href=\"jav&#x61;script:link\">bad</a><img src=\"data:text/plain,img\">"
            + "<a href=\"https://example.org\">good</a>";
    String stored =
        create(edits, ArticleBody.of(ArticleBody.Format.HTML, html)).bodyUpdate().content();
    assertTrue(stored.contains("<b>Guide</b>"));
    assertTrue(stored.contains("https://example.org"));
    assertFalse(stored.contains("onclick"));
    assertFalse(stored.contains("script"));
    assertFalse(stored.contains("call()"));
    assertFalse(stored.contains("data:"));
    KnowledgeBaseArticleEntity existing =
        create(edits, markdown).toBuilder()
            .withBodyUpdate(ArticleBody.of(ArticleBody.Format.HTML, html))
            .build();
    assertNull(
        edits
            .apply(
                existing,
                new KnowledgeBaseArticleChange[] {KnowledgeBaseArticleChange.updateTitle("new")})
            .bodyUpdate());
    assertEquals(
        stored,
        edits
            .apply(
                existing,
                new KnowledgeBaseArticleChange[] {
                  KnowledgeBaseArticleChange.updateBody(existing.bodyUpdate())
                })
            .bodyUpdate()
            .content());
  }

  @Test
  void limitsSubmittedAndStoredUtf8Bytes() {
    ArticleEdits edits = new ArticleEdits(4);
    assertEquals(
        "𠀀",
        create(edits, ArticleBody.of(ArticleBody.Format.MARKDOWN, "𠀀")).bodyUpdate().content());
    assertThrows(
        IllegalArgumentException.class,
        () -> create(edits, ArticleBody.of(ArticleBody.Format.MARKDOWN, "𠀀a")));
    // Escaping a single ampersand yields five stored bytes.
    assertThrows(
        IllegalArgumentException.class,
        () -> create(edits, ArticleBody.of(ArticleBody.Format.HTML, "&")));
    assertThrows(
        IllegalArgumentException.class,
        () -> create(edits, ArticleBody.of(ArticleBody.Format.HTML, "<b></b>")));
  }

  private static KnowledgeBaseArticleEntity create(ArticleEdits edits, ArticleBody body) {
    return edits.create(
        "lake", 1, "guide", ArticleCreate.builder().withTitle("Guide").withBody(body).build());
  }
}
