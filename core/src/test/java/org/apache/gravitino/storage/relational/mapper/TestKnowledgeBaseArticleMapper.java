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

package org.apache.gravitino.storage.relational.mapper;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import org.apache.gravitino.authorization.GravitinoAuthorizer.ArticleOwnerIds;
import org.apache.gravitino.kb.ArticleListFilter;
import org.apache.gravitino.storage.relational.po.KnowledgeBaseArticlePO;
import org.apache.ibatis.session.Configuration;
import org.junit.jupiter.api.Test;

class TestKnowledgeBaseArticleMapper {
  @Test
  void searchReadsBodyOnlyInTheKeywordPredicate() {
    for (String dialect : List.of("h2", "mysql", "postgresql")) {
      Configuration configuration = new Configuration();
      configuration.setDatabaseId(dialect);
      configuration.addMapper(KnowledgeBaseArticleMapper.class);
      var statement =
          configuration.getMappedStatement(KnowledgeBaseArticleMapper.class.getName() + ".search");
      var params = new HashMap<String, Object>();
      params.put("lake", 1L);
      params.put("filter", ArticleListFilter.builder().build());
      params.put("targets", List.of());
      params.put("allDrafts", true);
      params.put("ownerIds", ArticleOwnerIds.NONE);
      String listSql = statement.getBoundSql(params).getSql().toLowerCase(Locale.ROOT);
      assertFalse(listSql.contains("body_content"), dialect);
      params.put("filter", ArticleListFilter.builder().withQuery("refund").build());
      params.put("pattern", "%refund%");
      String searchSql = statement.getBoundSql(params).getSql().toLowerCase(Locale.ROOT);
      assertTrue(searchSql.contains("body_content"), dialect);
      assertFalse(
          searchSql.substring(0, searchSql.indexOf("from")).contains("body_content"), dialect);
      assertFalse(searchSql.contains("body_format"), dialect);
    }
  }

  @Test
  void metadataQueriesExcludeContent() {
    Configuration configuration = new Configuration();
    configuration.addMapper(KnowledgeBaseArticleMapper.class);
    var before = new KnowledgeBaseArticlePO();
    var after = new KnowledgeBaseArticlePO();
    after.setTitle("New title");
    var params =
        Map.of("name", "guide", "names", List.of("guide"), "before", before, "after", after);
    String prefix = KnowledgeBaseArticleMapper.class.getName() + ".";
    for (String method :
        List.of(
            "get",
            "list",
            "joined",
            "joinedBatch",
            "lock",
            "summaries",
            "ids",
            "bindings",
            "listBindings",
            "update")) {
      String sql = configuration.getMappedStatement(prefix + method).getBoundSql(params).getSql();
      assertFalse(sql.contains("body_content"), method);
      assertFalse(sql.contains("body_format"), method);
    }
    String content =
        configuration.getMappedStatement(prefix + "content").getBoundSql(params).getSql();
    assertTrue(content.contains("body_content"));
    assertTrue(content.contains("revision"));
    assertFalse(content.contains("kb_article_relation_meta"));
    assertFalse(content.contains("domain_member_rel"));
  }
}
