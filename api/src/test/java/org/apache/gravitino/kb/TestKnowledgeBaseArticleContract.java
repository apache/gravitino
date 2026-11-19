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

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.common.collect.ImmutableList;
import java.util.Arrays;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.MetadataObjects;
import org.junit.jupiter.api.Test;

public class TestKnowledgeBaseArticleContract {

  @Test
  public void testArticleBody() {
    ArticleBody body = ArticleBody.of(ArticleBody.Format.MARKDOWN, "# Revenue");
    assertEquals(ArticleBody.Format.MARKDOWN, body.format());
    assertEquals("# Revenue", body.content());
    assertEquals(body, ArticleBody.of(ArticleBody.Format.MARKDOWN, "# Revenue"));
    assertFalse(body.equals(ArticleBody.of(ArticleBody.Format.HTML, "# Revenue")));
    assertThrows(IllegalArgumentException.class, () -> ArticleBody.of(null, "x"));
    assertThrows(
        IllegalArgumentException.class, () -> ArticleBody.of(ArticleBody.Format.HTML, null));
  }

  @Test
  public void testArticleCreateRequiresTitleAndBody() {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            ArticleCreate.builder().withBody(ArticleBody.of(ArticleBody.Format.HTML, "x")).build());
    assertThrows(
        IllegalArgumentException.class, () -> ArticleCreate.builder().withTitle("  ").build());
    assertThrows(
        IllegalArgumentException.class, () -> ArticleCreate.builder().withTitle("Title").build());

    ArticleCreate create =
        ArticleCreate.builder()
            .withTitle("Revenue Guide")
            .withBody(ArticleBody.of(ArticleBody.Format.MARKDOWN, "body"))
            .addDomains("Finance", "Tax")
            .addSynonyms("rev rec", "revenue")
            .withSourceReference("https://wiki.example/revenue")
            .withProperty("k", "v")
            .build();
    assertEquals("Revenue Guide", create.title());
    assertEquals(Arrays.asList("Finance", "Tax"), create.domains());
    assertEquals(Arrays.asList("rev rec", "revenue"), create.synonyms());
    assertEquals("https://wiki.example/revenue", create.sourceReference());
    assertEquals("v", create.properties().get("k"));
  }

  @Test
  public void testObjectIdentifierKeepsDottedColumnNameAsOneComponent() {
    ArticleObjectIdentifier column =
        ArticleObjectIdentifier.of(
            MetadataObject.Type.COLUMN,
            Arrays.asList("iceberg", "sales", "orders"),
            "total.amount");
    assertEquals(MetadataObject.Type.COLUMN, column.type());
    assertEquals(Arrays.asList("iceberg", "sales", "orders"), column.namespace());
    assertEquals("total.amount", column.name());
    assertEquals(
        column,
        ArticleObjectIdentifier.of(
            MetadataObject.Type.COLUMN,
            new String[] {"iceberg", "sales", "orders"},
            "total.amount"));

    ArticleObjectIdentifier catalog =
        ArticleObjectIdentifier.of(MetadataObject.Type.CATALOG, ImmutableList.of(), "iceberg");
    assertTrue(catalog.namespace().isEmpty());

    assertThrows(
        IllegalArgumentException.class,
        () -> ArticleObjectIdentifier.of(MetadataObject.Type.TABLE, Arrays.asList("s"), ""));
    assertThrows(
        IllegalArgumentException.class,
        () -> ArticleObjectIdentifier.of(MetadataObject.Type.TABLE, Arrays.asList("s"), "  "));
    assertThrows(
        IllegalArgumentException.class,
        () -> ArticleObjectIdentifier.of(null, Arrays.asList("s"), "t"));
  }

  @Test
  public void testListFilterValidation() {
    ArticleListFilter filter =
        ArticleListFilter.builder()
            .withQuery("Revenue")
            .withDomain("Finance")
            .withStatus(ArticleStatus.APPROVED)
            .withLimit(10)
            .withOffset(20)
            .build();
    assertEquals("Revenue", filter.query());
    assertEquals("Finance", filter.domain());
    assertEquals(ArticleStatus.APPROVED, filter.status());
    assertEquals(10, filter.limit());
    assertEquals(20, filter.offset());

    assertEquals(ArticleListFilter.DEFAULT_LIMIT, ArticleListFilter.builder().build().limit());
    assertThrows(
        IllegalArgumentException.class, () -> ArticleListFilter.builder().withLimit(0).build());
    assertThrows(
        IllegalArgumentException.class, () -> ArticleListFilter.builder().withOffset(-1).build());
  }

  @Test
  public void testChangeFactories() {
    KnowledgeBaseArticleChange.UpdateTitle title =
        (KnowledgeBaseArticleChange.UpdateTitle) KnowledgeBaseArticleChange.updateTitle("New");
    assertEquals("New", title.getNewTitle());
    assertEquals(title, KnowledgeBaseArticleChange.updateTitle("New"));

    KnowledgeBaseArticleChange.UpdateDomains domains =
        (KnowledgeBaseArticleChange.UpdateDomains)
            KnowledgeBaseArticleChange.updateDomains(new String[] {"Finance"});
    assertArrayEquals(new String[] {"Finance"}, domains.getNewDomains());

    KnowledgeBaseArticleChange.UpdateBody body =
        (KnowledgeBaseArticleChange.UpdateBody)
            KnowledgeBaseArticleChange.updateBody(
                ArticleBody.of(ArticleBody.Format.HTML, "<p>x</p>"));
    assertEquals(ArticleBody.Format.HTML, body.getNewBody().format());

    KnowledgeBaseArticleChange.UpdateStatus status =
        (KnowledgeBaseArticleChange.UpdateStatus)
            KnowledgeBaseArticleChange.updateStatus(ArticleStatus.DEPRECATED);
    assertEquals(ArticleStatus.DEPRECATED, status.getNewStatus());

    KnowledgeBaseArticleChange.UpdateComment comment =
        (KnowledgeBaseArticleChange.UpdateComment) KnowledgeBaseArticleChange.updateComment("c");
    assertEquals("c", comment.getNewComment());

    KnowledgeBaseArticleChange.UpdateSynonyms synonyms =
        (KnowledgeBaseArticleChange.UpdateSynonyms)
            KnowledgeBaseArticleChange.updateSynonyms(new String[] {"a"});
    assertArrayEquals(new String[] {"a"}, synonyms.getNewSynonyms());

    KnowledgeBaseArticleChange.UpdateSourceReference source =
        (KnowledgeBaseArticleChange.UpdateSourceReference)
            KnowledgeBaseArticleChange.updateSourceReference("url");
    assertEquals("url", source.getNewSourceReference());

    KnowledgeBaseArticleChange.SetProperty set =
        (KnowledgeBaseArticleChange.SetProperty) KnowledgeBaseArticleChange.setProperty("k", "v");
    assertEquals("k", set.getProperty());
    assertEquals("v", set.getValue());

    KnowledgeBaseArticleChange.RemoveProperty remove =
        (KnowledgeBaseArticleChange.RemoveProperty) KnowledgeBaseArticleChange.removeProperty("k");
    assertEquals("k", remove.getProperty());
  }

  @Test
  public void testNormalizedArticleFields() {
    ArticleCreate request =
        ArticleCreate.builder()
            .withTitle(" Guide ")
            .withBody(ArticleBody.of(ArticleBody.Format.HTML, "text"))
            .addSynonyms(" Alias ", "alias")
            .build();
    assertEquals("Guide", request.title());
    assertEquals(ImmutableList.of("Alias"), request.synonyms());
    assertThrows(
        IllegalArgumentException.class,
        () -> KnowledgeBaseArticleChange.updateTitle(StringUtils.repeat("x", 513)));
    assertThrows(
        IllegalArgumentException.class,
        () -> KnowledgeBaseArticleChange.updateComment(StringUtils.repeat("x", 257)));
    assertThrows(
        IllegalArgumentException.class,
        () -> KnowledgeBaseArticleChange.updateSourceReference(StringUtils.repeat("x", 1025)));
    assertThrows(
        IllegalArgumentException.class,
        () ->
            KnowledgeBaseArticleChange.updateSynonyms(new String[] {StringUtils.repeat("x", 129)}));
  }

  @Test
  public void testObjectTypeAndDepth() {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            ArticleObjectIdentifier.of(
                MetadataObject.Type.TABLE, ImmutableList.of("catalog"), "table"));
    assertThrows(
        IllegalArgumentException.class,
        () -> ArticleObjectIdentifier.of(MetadataObject.Type.TAG, ImmutableList.of(), "tag"));
    assertThrows(
        IllegalArgumentException.class,
        () ->
            ArticleObjectIdentifier.of(
                MetadataObject.Type.SCHEMA, ImmutableList.of(" "), "schema"));
    assertEquals(
        "guide",
        MetadataObjects.of(null, "guide", MetadataObject.Type.KNOWLEDGE_BASE_ARTICLE).name());
  }

  @Test
  public void testFieldLimits() {
    assertEquals(128, KnowledgeBaseArticle.NAME_MAX_LENGTH);
    assertEquals(512, KnowledgeBaseArticle.TITLE_MAX_LENGTH);
    assertEquals(256, KnowledgeBaseArticle.COMMENT_MAX_LENGTH);
    assertEquals(128, KnowledgeBaseArticle.SYNONYM_MAX_LENGTH);
    assertEquals(20, KnowledgeBaseArticle.SYNONYMS_MAX_COUNT);
    assertEquals(1024, KnowledgeBaseArticle.SOURCE_REFERENCE_MAX_LENGTH);
    assertEquals(1L << 20, KnowledgeBaseArticle.DEFAULT_MAX_BODY_BYTES);
    assertEquals(16_777_215L, KnowledgeBaseArticle.MAX_BODY_BYTES_LIMIT);
  }
}
