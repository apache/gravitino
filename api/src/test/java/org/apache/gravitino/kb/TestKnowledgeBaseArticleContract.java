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
import java.util.List;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.Audit;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.MetadataObjects;
import org.apache.gravitino.authorization.Owner;
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
    assertThrows(
        IllegalArgumentException.class, () -> ArticleBody.of(ArticleBody.Format.MARKDOWN, ""));
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
            .withStatus(ArticleStatus.PUBLISHED)
            .withLimit(10)
            .withOffset(20)
            .build();
    assertEquals("Revenue", filter.query());
    assertEquals("Finance", filter.domain());
    assertEquals(ArticleStatus.PUBLISHED, filter.status());
    assertEquals(10, filter.limit());
    assertEquals(20, filter.offset());

    assertEquals(20, ArticleListFilter.builder().build().limit());
    assertEquals(1, ArticleListFilter.builder().withLimit(1).build().limit());
    assertEquals(1_000, ArticleListFilter.builder().withLimit(1_000).build().limit());
    assertThrows(
        IllegalArgumentException.class, () -> ArticleListFilter.builder().withLimit(1_001).build());
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

  @Test
  public void testFieldLimitsAcceptAtLimit() {
    assertEquals(
        StringUtils.repeat("x", 128),
        ArticleCreate.builder()
            .withName(StringUtils.repeat("x", 128))
            .withTitle(StringUtils.repeat("t", 512))
            .withBody(ArticleBody.of(ArticleBody.Format.HTML, "text"))
            .addSynonym(StringUtils.repeat("s", 128))
            .withComment(StringUtils.repeat("c", 256))
            .withSourceReference(StringUtils.repeat("r", 1024))
            .build()
            .name());
    assertEquals(
        StringUtils.repeat("t", 512),
        ((KnowledgeBaseArticleChange.UpdateTitle)
                KnowledgeBaseArticleChange.updateTitle(StringUtils.repeat("t", 512)))
            .getNewTitle());
    assertEquals(
        StringUtils.repeat("s", 128),
        ((KnowledgeBaseArticleChange.UpdateSynonyms)
                KnowledgeBaseArticleChange.updateSynonyms(
                    new String[] {StringUtils.repeat("s", 128)}))
            .getNewSynonyms()[0]);
    assertThrows(
        IllegalArgumentException.class,
        () ->
            ArticleCreate.builder()
                .withName(StringUtils.repeat("x", 129))
                .withTitle("t")
                .withBody(ArticleBody.of(ArticleBody.Format.HTML, "text"))
                .build());
  }

  @Test
  public void testExplicitArticleNames() {
    ArticleCreate.Builder builder =
        ArticleCreate.builder()
            .withTitle("Guide")
            .withBody(ArticleBody.of(ArticleBody.Format.MARKDOWN, "text"));
    assertEquals(null, builder.build().name());
    String valid = "收入-e\u0301_2";
    assertEquals("收入-é_2", builder.withName(valid).build().name());
    assertEquals(
        "收入-é_2", MetadataObjects.parse(valid, MetadataObject.Type.KNOWLEDGE_BASE_ARTICLE).name());
    String maxName = StringUtils.repeat("𠀀", 128);
    assertEquals(maxName, builder.withName(maxName).build().name());
    assertThrows(IllegalArgumentException.class, () -> builder.withName(maxName + "𠀀").build());
    for (String invalid : Arrays.asList("", "a/b", "a b", "_-", "a.b", "\u0301")) {
      assertThrows(IllegalArgumentException.class, () -> builder.withName(invalid).build());
    }
  }

  @Test
  public void testMixedBindingChanges() {
    ArticleObjectIdentifier table =
        ArticleObjectIdentifier.of(MetadataObject.Type.TABLE, ImmutableList.of("c", "s"), "t");
    ArticleObjectIdentifier[] objects = {table};
    String[] names = {"Revenue"};
    KnowledgeBaseArticleChange.AddObjects add = KnowledgeBaseArticleChange.addObjects(objects);
    KnowledgeBaseArticleChange.RemoveTerms remove = KnowledgeBaseArticleChange.removeTerms(names);
    objects[0] = null;
    names[0] = "other";
    assertEquals(ImmutableList.of(table), add.getObjects());
    assertEquals(ImmutableList.of("Revenue"), remove.getTerms());
    assertEquals(
        ImmutableList.of(table), KnowledgeBaseArticleChange.removeObjects(table).getObjects());
    assertEquals(
        ImmutableList.of("Revenue"), KnowledgeBaseArticleChange.addTerms("Revenue").getTerms());
    assertThrows(IllegalArgumentException.class, () -> KnowledgeBaseArticleChange.addTerms(" "));
    assertEquals(
        ImmutableList.of("Revenue"),
        ArticleCreate.builder()
            .withTitle("Guide")
            .withBody(ArticleBody.of(ArticleBody.Format.MARKDOWN, "text"))
            .addTerms("Revenue")
            .build()
            .terms());
  }

  @Test
  public void testUnicodeTitleWhitespace() {
    assertThrows(
        IllegalArgumentException.class, () -> KnowledgeBaseArticleChange.updateTitle("\u3000"));
    assertEquals(
        KnowledgeBaseArticleChange.updateTitle("Guide"),
        KnowledgeBaseArticleChange.updateTitle("\u3000Guide\u3000"));
  }

  @Test
  public void testBindingAndArticlePageContinuation() {
    ArticleObjectIdentifier asset =
        ArticleObjectIdentifier.of(MetadataObject.Type.CATALOG, ImmutableList.of(), "catalog");
    AssetBindingPage bindings = new AssetBindingPage(ImmutableList.of(asset), true);
    assertEquals(ImmutableList.of(asset), bindings.objects());
    assertTrue(bindings.hasMore());
    ArticleSummary summary =
        new ArticleSummary() {
          public String name() {
            return "guide";
          }

          public String title() {
            return "Guide";
          }

          public ArticleStatus status() {
            return ArticleStatus.PUBLISHED;
          }

          public String[] domains() {
            return new String[0];
          }

          public Owner owner() {
            return null;
          }

          public Audit auditInfo() {
            return null;
          }
        };
    assertTrue(ArticlePage.of(ImmutableList.of(summary), true).hasMore());
    assertFalse(ArticlePage.of(ImmutableList.of(summary), false).hasMore());
    assertFalse(ArticlePage.of(ImmutableList.of(), false).hasMore());
  }

  @Test
  public void testSynonymCountLimit() {
    ArticleCreate.Builder builder =
        ArticleCreate.builder()
            .withTitle("Title")
            .withBody(ArticleBody.of(ArticleBody.Format.HTML, "text"));
    for (int i = 0; i < KnowledgeBaseArticle.SYNONYMS_MAX_COUNT; i++) {
      builder.addSynonym("synonym-" + i);
    }
    assertEquals(KnowledgeBaseArticle.SYNONYMS_MAX_COUNT, builder.build().synonyms().size());

    String[] twentyOne = new String[KnowledgeBaseArticle.SYNONYMS_MAX_COUNT + 1];
    for (int i = 0; i < twentyOne.length; i++) {
      twentyOne[i] = "synonym-" + i;
    }
    assertThrows(
        IllegalArgumentException.class, () -> KnowledgeBaseArticleChange.updateSynonyms(twentyOne));

    // A case-insensitive duplicate counts once toward the limit: 21 entries, 20 distinct.
    twentyOne[KnowledgeBaseArticle.SYNONYMS_MAX_COUNT] = "SYNONYM-0";
    assertEquals(
        KnowledgeBaseArticle.SYNONYMS_MAX_COUNT,
        ((KnowledgeBaseArticleChange.UpdateSynonyms)
                KnowledgeBaseArticleChange.updateSynonyms(twentyOne))
            .getNewSynonyms()
            .length);
  }

  @Test
  public void testRejectsBlankAndNullCollectionEntries() {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            ArticleCreate.builder()
                .withTitle("Title")
                .withBody(ArticleBody.of(ArticleBody.Format.HTML, "text"))
                .addSynonym("  ")
                .build());
    assertThrows(
        IllegalArgumentException.class,
        () -> KnowledgeBaseArticleChange.updateSynonyms(new String[] {null}));
    assertThrows(
        IllegalArgumentException.class,
        () -> KnowledgeBaseArticleChange.updateDomains(new String[] {" "}));
    assertThrows(
        IllegalArgumentException.class,
        () -> KnowledgeBaseArticleChange.updateDomains(new String[] {null}));
    assertThrows(NullPointerException.class, () -> KnowledgeBaseArticleChange.updateDomains(null));
    assertThrows(NullPointerException.class, () -> KnowledgeBaseArticleChange.updateSynonyms(null));
  }

  @Test
  public void testIdentifierDepthPerType() {
    assertEquals(
        0,
        ArticleObjectIdentifier.of(MetadataObject.Type.CATALOG, ImmutableList.of(), "c")
            .namespace()
            .size());
    assertEquals(
        1,
        ArticleObjectIdentifier.of(MetadataObject.Type.SCHEMA, ImmutableList.of("c"), "s")
            .namespace()
            .size());
    for (MetadataObject.Type type :
        ImmutableList.of(
            MetadataObject.Type.TABLE,
            MetadataObject.Type.VIEW,
            MetadataObject.Type.FILESET,
            MetadataObject.Type.TOPIC,
            MetadataObject.Type.MODEL,
            MetadataObject.Type.SEMANTIC_MODEL)) {
      assertEquals(
          2, ArticleObjectIdentifier.of(type, ImmutableList.of("c", "s"), "o").namespace().size());
      assertThrows(
          IllegalArgumentException.class,
          () -> ArticleObjectIdentifier.of(type, ImmutableList.of("c"), "o"));
    }
    assertEquals(
        3,
        ArticleObjectIdentifier.of(MetadataObject.Type.COLUMN, ImmutableList.of("c", "s", "t"), "x")
            .namespace()
            .size());
    assertThrows(
        IllegalArgumentException.class,
        () ->
            ArticleObjectIdentifier.of(
                MetadataObject.Type.COLUMN, ImmutableList.of("c", "s"), "x"));
    assertThrows(
        IllegalArgumentException.class,
        () ->
            ArticleObjectIdentifier.of(
                MetadataObject.Type.COLUMN, ImmutableList.of("c", "s", "t", "u"), "x"));
  }

  @Test
  public void testLimitsCountUnicodeCodePoints() {
    // One astral letter is two UTF-16 chars; limits count code points.
    assertEquals(
        StringUtils.repeat("𝕏", 512),
        ((KnowledgeBaseArticleChange.UpdateTitle)
                KnowledgeBaseArticleChange.updateTitle(StringUtils.repeat("𝕏", 512)))
            .getNewTitle());
    assertThrows(
        IllegalArgumentException.class,
        () -> KnowledgeBaseArticleChange.updateTitle(StringUtils.repeat("𝕏", 513)));
  }

  @Test
  public void testPageAndHashCodes() {
    ArticlePage page = ArticlePage.of((ArticleSummary[]) null, false);
    assertTrue(page.articles().isEmpty());
    assertFalse(page.hasMore());
    assertTrue(ArticlePage.of((List<ArticleSummary>) null, false).articles().isEmpty());

    ArticleBody body = ArticleBody.of(ArticleBody.Format.MARKDOWN, "# Revenue");
    assertEquals(
        body.hashCode(), ArticleBody.of(ArticleBody.Format.MARKDOWN, "# Revenue").hashCode());
    assertEquals(
        KnowledgeBaseArticleChange.updateTitle("New").hashCode(),
        KnowledgeBaseArticleChange.updateTitle("New").hashCode());
    ArticleObjectIdentifier column =
        ArticleObjectIdentifier.of(
            MetadataObject.Type.COLUMN, ImmutableList.of("c", "s", "t"), "total.amount");
    assertEquals(
        column.hashCode(),
        ArticleObjectIdentifier.of(
                MetadataObject.Type.COLUMN, new String[] {"c", "s", "t"}, "total.amount")
            .hashCode());
  }
}
