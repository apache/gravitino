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
package org.apache.gravitino.storage.relational;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import org.apache.gravitino.Entity;
import org.apache.gravitino.EntityAlreadyExistsException;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.exceptions.NoSuchEntityException;
import org.apache.gravitino.meta.SemanticModelEntity;
import org.apache.gravitino.semantic.AIContext;
import org.apache.gravitino.semantic.AIContextObject;
import org.apache.gravitino.semantic.CustomExtension;
import org.apache.gravitino.semantic.DataType;
import org.apache.gravitino.semantic.Dataset;
import org.apache.gravitino.semantic.DialectExpression;
import org.apache.gravitino.semantic.Dimension;
import org.apache.gravitino.semantic.Expression;
import org.apache.gravitino.semantic.Field;
import org.apache.gravitino.semantic.Metric;
import org.apache.gravitino.semantic.Relationship;
import org.apache.gravitino.semantic.SemanticModelDefinition;
import org.apache.gravitino.storage.RandomIdGenerator;
import org.apache.gravitino.storage.relational.mapper.SemanticModelMetaMapper;
import org.apache.gravitino.storage.relational.mapper.SemanticModelVersionInfoMapper;
import org.apache.gravitino.storage.relational.po.SemanticModelPO;
import org.apache.gravitino.storage.relational.session.SqlSessionFactoryHelper;
import org.apache.gravitino.storage.relational.utils.SessionUtils;
import org.apache.gravitino.utils.NamespaceUtil;
import org.apache.ibatis.session.SqlSession;
import org.junit.jupiter.api.TestTemplate;

/** Tests Semantic Model create and load persistence through {@link JDBCBackend}. */
public class TestSemanticModelJDBCBackend extends TestJDBCBackend {

  @TestTemplate
  public void testCreateAndLoadRoundTrip() throws IOException {
    Namespace namespace = createParents("round_trip");
    SemanticModelEntity absent =
        semanticModel(
            RandomIdGenerator.INSTANCE.nextId(),
            namespace,
            "absent_optional_model",
            false,
            ImmutableMap.of("domain", "sales"));
    SemanticModelEntity empty =
        semanticModel(
            RandomIdGenerator.INSTANCE.nextId(),
            namespace,
            "empty_optional_model",
            true,
            ImmutableMap.of());

    backend.insert(absent, false);
    backend.insert(empty, false);

    SemanticModelEntity loadedAbsent =
        backend.get(absent.nameIdentifier(), Entity.EntityType.SEMANTIC_MODEL);
    SemanticModelEntity loadedEmpty =
        backend.get(empty.nameIdentifier(), Entity.EntityType.SEMANTIC_MODEL);

    assertEquals(absent, loadedAbsent);
    assertEquals(empty, loadedEmpty);
    assertNull(loadedAbsent.definition().relationships());
    assertNull(loadedAbsent.definition().metrics());
    assertEquals(0, loadedEmpty.definition().relationships().length);
    assertEquals(0, loadedEmpty.definition().metrics().length);
    assertTrue(loadedEmpty.properties().isEmpty());
    assertEquals(
        new BigDecimal("1.50"),
        loadedAbsent.definition().aiContext().object().additionalProperties().get("threshold"));
    assertNull(loadedAbsent.definition().aiContext().object().examples());
    assertEquals(0, loadedAbsent.definition().aiContext().object().synonyms().length);
  }

  @TestTemplate
  public void testDuplicateAndMissingEntities() throws IOException {
    Namespace namespace = createParents("duplicate");
    SemanticModelEntity original =
        semanticModel(
            RandomIdGenerator.INSTANCE.nextId(),
            namespace,
            "sales_model",
            false,
            ImmutableMap.of("owner", "analytics"));
    backend.insert(original, false);

    SemanticModelEntity duplicate =
        semanticModel(
            RandomIdGenerator.INSTANCE.nextId(),
            namespace,
            original.name(),
            true,
            ImmutableMap.of());
    assertThrows(EntityAlreadyExistsException.class, () -> backend.insert(duplicate, false));
    assertEquals(
        original, backend.get(original.nameIdentifier(), Entity.EntityType.SEMANTIC_MODEL));

    NameIdentifier missingModel = NameIdentifier.of(namespace, "missing_model");
    assertThrows(
        NoSuchEntityException.class,
        () -> backend.get(missingModel, Entity.EntityType.SEMANTIC_MODEL));

    String suffix = Long.toUnsignedString(RandomIdGenerator.INSTANCE.nextId());
    String metalakeName = "missing_parent_metalake_" + suffix;
    String catalogName = "missing_parent_catalog_" + suffix;
    createAndInsertMakeLake(metalakeName);
    createAndInsertCatalog(metalakeName, catalogName);
    Namespace missingParentNamespace =
        NamespaceUtil.ofSemanticModel(metalakeName, catalogName, "missing_schema");
    SemanticModelEntity missingParent =
        semanticModel(
            RandomIdGenerator.INSTANCE.nextId(),
            missingParentNamespace,
            "orphan_model",
            false,
            ImmutableMap.of());

    assertThrows(NoSuchEntityException.class, () -> backend.insert(missingParent, false));
    assertEquals(0, countRows(SemanticModelMetaMapper.TABLE_NAME, missingParent.id()));
    assertEquals(0, countRows(SemanticModelVersionInfoMapper.TABLE_NAME, missingParent.id()));
  }

  @TestTemplate
  public void testCreateRollsBackIdentityWhenSnapshotInsertFails() throws IOException {
    Namespace namespace = createParents("transaction");
    SemanticModelEntity semanticModel =
        semanticModel(
            RandomIdGenerator.INSTANCE.nextId(),
            namespace,
            "transaction_model",
            false,
            ImmutableMap.of("domain", "finance"));
    SemanticModelPO po =
        SemanticModelPO.initializeSemanticModelPO(semanticModel, SemanticModelPO.builder());
    SessionUtils.doWithCommit(
        SemanticModelVersionInfoMapper.class,
        mapper -> mapper.insertSemanticModelVersionInfo(po.getSemanticModelVersionInfoPO()));

    assertThrows(EntityAlreadyExistsException.class, () -> backend.insert(semanticModel, false));
    assertEquals(0, countRows(SemanticModelMetaMapper.TABLE_NAME, semanticModel.id()));
    assertEquals(1, countRows(SemanticModelVersionInfoMapper.TABLE_NAME, semanticModel.id()));
  }

  private Namespace createParents(String prefix) throws IOException {
    String suffix = Long.toUnsignedString(RandomIdGenerator.INSTANCE.nextId());
    String metalakeName = prefix + "_metalake_" + suffix;
    String catalogName = prefix + "_catalog_" + suffix;
    String schemaName = prefix + "_schema_" + suffix;
    createAndInsertMakeLake(metalakeName);
    createAndInsertCatalog(metalakeName, catalogName);
    createAndInsertSchema(metalakeName, catalogName, schemaName);
    return NamespaceUtil.ofSemanticModel(metalakeName, catalogName, schemaName);
  }

  private SemanticModelEntity semanticModel(
      Long id,
      Namespace namespace,
      String name,
      boolean explicitEmpty,
      Map<String, String> properties) {
    AIContextObject context =
        AIContextObject.builder()
            .withInstructions("Use certified sales definitions")
            .withSynonyms(new String[0])
            .withAdditionalProperties(
                Map.of("threshold", new BigDecimal("1.50"), "nested", List.of(new BigInteger("3"))))
            .build();
    Field field =
        Field.builder()
            .withName("ordered_at")
            .withExpression(
                Expression.builder()
                    .withDialects(
                        new DialectExpression[] {
                          DialectExpression.builder()
                              .withDialect("ansi")
                              .withExpression("ordered_at")
                              .build()
                        })
                    .build())
            .withDimension(Dimension.builder().withIsTime(true).build())
            .withDatatype(DataType.DATE_TIME_TZ)
            .build();
    Dataset dataset =
        Dataset.builder()
            .withName("orders")
            .withSource(NameIdentifier.of("sales", "mart", "orders"))
            .withPrimaryKey(new String[0])
            .withFields(new Field[] {field})
            .build();
    SemanticModelDefinition.Builder definitionBuilder =
        SemanticModelDefinition.builder()
            .withAIContext(AIContext.of(context))
            .withDatasets(new Dataset[] {dataset});
    if (explicitEmpty) {
      definitionBuilder
          .withRelationships(new Relationship[0])
          .withMetrics(new Metric[0])
          .withCustomExtensions(new CustomExtension[0]);
    }

    return SemanticModelEntity.builder()
        .withId(id)
        .withName(name)
        .withNamespace(namespace)
        .withComment(explicitEmpty ? null : "Governed sales definitions")
        .withDefinition(definitionBuilder.build())
        .withProperties(properties)
        .withAuditInfo(AUDIT_INFO)
        .build();
  }

  private int countRows(String tableName, Long semanticModelId) {
    if (!SemanticModelMetaMapper.TABLE_NAME.equals(tableName)
        && !SemanticModelVersionInfoMapper.TABLE_NAME.equals(tableName)) {
      throw new IllegalArgumentException("Unsupported Semantic Model table: " + tableName);
    }
    String sql = String.format("SELECT count(*) FROM %s WHERE semantic_model_id = ?", tableName);
    try (SqlSession sqlSession =
            SqlSessionFactoryHelper.getInstance().getSqlSessionFactory().openSession(true);
        Connection connection = sqlSession.getConnection();
        PreparedStatement statement = connection.prepareStatement(sql)) {
      statement.setLong(1, semanticModelId);
      try (ResultSet resultSet = statement.executeQuery()) {
        assertTrue(resultSet.next());
        return resultSet.getInt(1);
      }
    } catch (SQLException e) {
      throw new RuntimeException("Failed to count Semantic Model rows", e);
    }
  }
}
