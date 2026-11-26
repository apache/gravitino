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
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.google.common.collect.ImmutableList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.MetadataObjects;
import org.apache.gravitino.domain.DefinitionMember;
import org.apache.gravitino.domain.DefinitionMemberPage;
import org.apache.gravitino.domain.Domain;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

public class TestKnowledgeBaseArticleContract {

  @Test
  public void testArticleBody() {
    ArticleBody body = ArticleBody.of(ArticleBody.Format.MARKDOWN, "# Revenue");
    assertEquals(ArticleBody.Format.MARKDOWN, body.format());
    assertEquals("# Revenue", body.content());
    ArticleContent content = new ArticleContent(body, "revision-1");
    assertEquals(body, content.body());
    assertEquals("revision-1", content.revisionToken());
    assertEquals(body, ArticleBody.of(ArticleBody.Format.MARKDOWN, "# Revenue"));
    assertFalse(body.equals(ArticleBody.of(ArticleBody.Format.HTML, "# Revenue")));
    assertThrows(IllegalArgumentException.class, () -> ArticleBody.of(null, "x"));
    assertThrows(
        IllegalArgumentException.class, () -> ArticleBody.of(ArticleBody.Format.HTML, null));
    assertThrows(
        IllegalArgumentException.class, () -> ArticleBody.of(ArticleBody.Format.MARKDOWN, ""));
  }

  @Test
  public void testArticleBodyRejectsNullCharacter() {
    for (ArticleBody.Format format : ArticleBody.Format.values()) {
      assertThrows(IllegalArgumentException.class, () -> ArticleBody.of(format, "a\0b"));
      String text = "a\n\\u0000\tb";
      assertEquals(text, ArticleBody.of(format, text).content());
    }
  }

  @Test
  public void testArticleBodyRequiresPairedSurrogates() {
    for (ArticleBody.Format format : ArticleBody.Format.values()) {
      assertThrows(IllegalArgumentException.class, () -> ArticleBody.of(format, "a\uD800b"));
      assertThrows(IllegalArgumentException.class, () -> ArticleBody.of(format, "a\uDC00b"));
      assertEquals("a😀b", ArticleBody.of(format, "a😀b").content());
    }
  }

  @Test
  public void testCreateAndEditRejectNullCharacterInMetadata() {
    String invalid = "a\0b";
    var builder =
        ArticleCreate.builder()
            .withTitle("Guide")
            .withBody(ArticleBody.of(ArticleBody.Format.MARKDOWN, "# Guide"));
    assertThrows(IllegalArgumentException.class, () -> builder.withTitle(invalid).build());
    assertThrows(
        IllegalArgumentException.class,
        () -> builder.withTitle("Guide").withComment(invalid).build());
    assertThrows(
        IllegalArgumentException.class,
        () -> builder.withComment(null).withSourceReference(invalid).build());
    assertThrows(
        IllegalArgumentException.class, () -> KnowledgeBaseArticleChange.updateTitle(invalid));
    assertThrows(
        IllegalArgumentException.class, () -> KnowledgeBaseArticleChange.updateComment(invalid));
    assertThrows(
        IllegalArgumentException.class,
        () -> KnowledgeBaseArticleChange.updateSourceReference(invalid));
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
    ArticleSummary summary = mock(ArticleSummary.class);
    assertTrue(ArticlePage.of(ImmutableList.of(summary), true).hasMore());
    assertFalse(ArticlePage.of(ImmutableList.of(summary), false).hasMore());
    assertFalse(ArticlePage.of(ImmutableList.of(), false).hasMore());
  }

  @Test
  public void testReverseLookupsUseArticleListFilters() {
    KnowledgeBaseArticleOperations operations =
        mock(KnowledgeBaseArticleOperations.class, CALLS_REAL_METHODS);
    ArticleObjectIdentifier target =
        ArticleObjectIdentifier.of(
            MetadataObject.Type.COLUMN, ImmutableList.of("c", "s", "t"), "a.b");
    operations.listArticlesForObject(target, true, 5, 10);
    operations.listArticlesForTerm("Revenue");
    ArgumentCaptor<ArticleListFilter> capture = ArgumentCaptor.forClass(ArticleListFilter.class);
    verify(operations, times(2)).listArticles(capture.capture());
    ArticleListFilter asset = capture.getAllValues().get(0);
    assertEquals(target, asset.object());
    assertNull(asset.term());
    assertTrue(asset.includeDeprecated());
    assertEquals(5, asset.limit());
    assertEquals(10, asset.offset());
    ArticleListFilter term = capture.getAllValues().get(1);
    assertEquals("Revenue", term.term());
    assertNull(term.object());
    assertFalse(term.includeDeprecated());
    assertEquals(20, term.limit());
    assertEquals(0, term.offset());
    ArticleListFilter combined =
        ArticleListFilter.builder()
            .withDomain("Finance")
            .withTerm("Revenue")
            .withObject(target)
            .build();
    assertEquals("Finance", combined.domain());
    assertEquals("Revenue", combined.term());
    assertEquals(target, combined.object());
    assertThrows(IllegalArgumentException.class, () -> operations.listArticlesForTerm(" "));
    assertThrows(
        IllegalArgumentException.class,
        () -> operations.listArticlesForObject(target, false, 1001, 0));
  }

  @Test
  public void testOrdinaryAlterOmitsRevision() {
    KnowledgeBaseArticleOperations operations =
        mock(KnowledgeBaseArticleOperations.class, CALLS_REAL_METHODS);
    KnowledgeBaseArticleChange[] changes = {
      KnowledgeBaseArticleChange.updateTitle("Shared title"),
      KnowledgeBaseArticleChange.addTerms("Revenue")
    };
    operations.alterArticle("guide", changes);
    verify(operations).alterArticle("guide", changes, null);
  }

  @Test
  public void testBindingAndDefinitionMemberPages() {
    KnowledgeBaseArticleOperations operations =
        mock(KnowledgeBaseArticleOperations.class, CALLS_REAL_METHODS);
    operations.listTermBindings("guide");
    verify(operations).listTermBindings("guide", 20, 0);
    ArticleTermBinding term = mock(ArticleTermBinding.class);
    TermBindingPage terms = new TermBindingPage(ImmutableList.of(term), true);
    assertEquals(ImmutableList.of(term), terms.terms());
    assertTrue(terms.hasMore());

    Domain domain = mock(Domain.class, CALLS_REAL_METHODS);
    DefinitionMember member = mock(DefinitionMember.class);
    DefinitionMemberPage members = new DefinitionMemberPage(ImmutableList.of(member), false);
    doReturn(members).when(domain).listDefinitionMembers(20, 0);
    assertEquals(ImmutableList.of(member), domain.listDefinitionMembers().members());
    assertFalse(members.hasMore());
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

  @Test
  public void testPropertyChangesRejectInvalidInput() {
    assertThrows(
        IllegalArgumentException.class, () -> KnowledgeBaseArticleChange.setProperty(null, "v"));
    assertThrows(
        IllegalArgumentException.class, () -> KnowledgeBaseArticleChange.setProperty(" ", "v"));
    assertThrows(
        IllegalArgumentException.class, () -> KnowledgeBaseArticleChange.setProperty("k", null));
    assertThrows(
        IllegalArgumentException.class, () -> KnowledgeBaseArticleChange.removeProperty(null));
    assertThrows(
        IllegalArgumentException.class, () -> KnowledgeBaseArticleChange.removeProperty(" "));
  }

  @Test
  public void testNullClearsCommentAndSourceReference() {
    KnowledgeBaseArticleChange.UpdateComment clearComment =
        (KnowledgeBaseArticleChange.UpdateComment) KnowledgeBaseArticleChange.updateComment(null);
    assertNull(clearComment.getNewComment());
    assertEquals(clearComment, KnowledgeBaseArticleChange.updateComment(null));
    assertEquals(
        clearComment.hashCode(), KnowledgeBaseArticleChange.updateComment(null).hashCode());
    assertFalse(clearComment.equals(KnowledgeBaseArticleChange.updateComment("comment")));

    KnowledgeBaseArticleChange.UpdateSourceReference clearSource =
        (KnowledgeBaseArticleChange.UpdateSourceReference)
            KnowledgeBaseArticleChange.updateSourceReference(null);
    assertNull(clearSource.getNewSourceReference());
    assertEquals(clearSource, KnowledgeBaseArticleChange.updateSourceReference(null));
    assertEquals(
        clearSource.hashCode(), KnowledgeBaseArticleChange.updateSourceReference(null).hashCode());
    assertFalse(clearSource.equals(KnowledgeBaseArticleChange.updateSourceReference("source")));
  }

  @Test
  public void testArrayChangeValueEqualityAndDefensiveCopy() {
    KnowledgeBaseArticleChange.UpdateDomains domains =
        (KnowledgeBaseArticleChange.UpdateDomains)
            KnowledgeBaseArticleChange.updateDomains(new String[] {"Finance", "Tax"});
    assertEquals(
        domains, KnowledgeBaseArticleChange.updateDomains(new String[] {"Finance", "Tax"}));
    assertEquals(
        domains.hashCode(),
        KnowledgeBaseArticleChange.updateDomains(new String[] {"Finance", "Tax"}).hashCode());
    assertFalse(
        domains.equals(KnowledgeBaseArticleChange.updateDomains(new String[] {"Tax", "Finance"})));
    assertFalse(
        domains.equals(KnowledgeBaseArticleChange.updateSynonyms(new String[] {"Finance", "Tax"})));

    String[] input = {"Finance", "Tax"};
    KnowledgeBaseArticleChange.UpdateDomains defensive =
        (KnowledgeBaseArticleChange.UpdateDomains) KnowledgeBaseArticleChange.updateDomains(input);
    input[0] = "Mutated";
    assertArrayEquals(new String[] {"Finance", "Tax"}, defensive.getNewDomains());
    String[] out = defensive.getNewDomains();
    out[0] = "Mutated";
    assertArrayEquals(new String[] {"Finance", "Tax"}, defensive.getNewDomains());

    KnowledgeBaseArticleChange.UpdateSynonyms synonyms =
        (KnowledgeBaseArticleChange.UpdateSynonyms)
            KnowledgeBaseArticleChange.updateSynonyms(new String[] {"a", "b"});
    assertEquals(synonyms, KnowledgeBaseArticleChange.updateSynonyms(new String[] {"a", "b"}));
    assertEquals(
        synonyms.hashCode(),
        KnowledgeBaseArticleChange.updateSynonyms(new String[] {"a", "b"}).hashCode());
    assertFalse(
        synonyms.equals(KnowledgeBaseArticleChange.updateSynonyms(new String[] {"b", "a"})));

    KnowledgeBaseArticleChange.UpdateStatus status =
        (KnowledgeBaseArticleChange.UpdateStatus)
            KnowledgeBaseArticleChange.updateStatus(ArticleStatus.DRAFT);
    assertEquals(status, KnowledgeBaseArticleChange.updateStatus(ArticleStatus.DRAFT));
    assertFalse(status.equals(KnowledgeBaseArticleChange.updateStatus(ArticleStatus.PUBLISHED)));
    assertFalse(status.equals(KnowledgeBaseArticleChange.updateTitle("DRAFT")));

    KnowledgeBaseArticleChange.SetProperty setProperty =
        (KnowledgeBaseArticleChange.SetProperty) KnowledgeBaseArticleChange.setProperty("k", "v");
    assertEquals(setProperty, KnowledgeBaseArticleChange.setProperty("k", "v"));
    assertFalse(setProperty.equals(KnowledgeBaseArticleChange.setProperty("k", "w")));
    assertFalse(setProperty.equals(KnowledgeBaseArticleChange.removeProperty("k")));

    KnowledgeBaseArticleChange.UpdateBody body =
        (KnowledgeBaseArticleChange.UpdateBody)
            KnowledgeBaseArticleChange.updateBody(ArticleBody.of(ArticleBody.Format.HTML, "x"));
    assertEquals(
        body, KnowledgeBaseArticleChange.updateBody(ArticleBody.of(ArticleBody.Format.HTML, "x")));
    assertFalse(
        body.equals(
            KnowledgeBaseArticleChange.updateBody(
                ArticleBody.of(ArticleBody.Format.MARKDOWN, "x"))));
  }

  @Test
  public void testNfcMetadataObjectEquality() {
    MetadataObject.Type type = MetadataObject.Type.KNOWLEDGE_BASE_ARTICLE;
    MetadataObject composed = MetadataObjects.of(null, "café", type);
    MetadataObject decomposed = MetadataObjects.of(null, "café", type);
    assertEquals(composed, decomposed);
    assertEquals(composed.hashCode(), decomposed.hashCode());
    assertEquals(1, new HashSet<>(Arrays.asList(composed, decomposed)).size());
  }

  @Test
  public void testCatalogAndSchemaDepthValidation() {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            ArticleObjectIdentifier.of(
                MetadataObject.Type.CATALOG, ImmutableList.of("catalog"), "catalog"));
    assertThrows(
        IllegalArgumentException.class,
        () -> ArticleObjectIdentifier.of(MetadataObject.Type.SCHEMA, ImmutableList.of(), "schema"));
    assertThrows(
        IllegalArgumentException.class,
        () ->
            ArticleObjectIdentifier.of(
                MetadataObject.Type.SCHEMA, ImmutableList.of("c", "s"), "schema"));
    assertThrows(
        IllegalArgumentException.class,
        () -> ArticleObjectIdentifier.of(MetadataObject.Type.TABLE, (List<String>) null, "table"));
  }
}
