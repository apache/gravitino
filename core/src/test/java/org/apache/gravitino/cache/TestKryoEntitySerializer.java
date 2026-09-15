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
package org.apache.gravitino.cache;

import com.esotericsoftware.kryo.KryoException;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.gravitino.Entity;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.file.Fileset;
import org.apache.gravitino.job.JobHandle;
import org.apache.gravitino.meta.AuditInfo;
import org.apache.gravitino.meta.BaseMetalake;
import org.apache.gravitino.meta.CatalogEntity;
import org.apache.gravitino.meta.ColumnEntity;
import org.apache.gravitino.meta.FilesetEntity;
import org.apache.gravitino.meta.JobEntity;
import org.apache.gravitino.meta.PolicyEntity;
import org.apache.gravitino.meta.SchemaEntity;
import org.apache.gravitino.meta.TableEntity;
import org.apache.gravitino.meta.TagEntity;
import org.apache.gravitino.meta.TopicEntity;
import org.apache.gravitino.meta.ViewEntity;
import org.apache.gravitino.policy.Policy;
import org.apache.gravitino.policy.PolicyContents;
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.Representation;
import org.apache.gravitino.rel.SQLRepresentation;
import org.apache.gravitino.rel.expressions.NamedReference;
import org.apache.gravitino.rel.expressions.distributions.Distribution;
import org.apache.gravitino.rel.expressions.distributions.Distributions;
import org.apache.gravitino.rel.expressions.literals.Literals;
import org.apache.gravitino.rel.expressions.sorts.SortOrder;
import org.apache.gravitino.rel.expressions.sorts.SortOrders;
import org.apache.gravitino.rel.expressions.transforms.Transform;
import org.apache.gravitino.rel.expressions.transforms.Transforms;
import org.apache.gravitino.rel.indexes.Index;
import org.apache.gravitino.rel.indexes.Indexes;
import org.apache.gravitino.rel.types.Types;
import org.apache.gravitino.utils.NamespaceUtil;
import org.apache.gravitino.utils.TestUtil;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/** Round-trips every cacheable entity type through {@link KryoEntitySerializer}. */
public class TestKryoEntitySerializer {

  private final KryoEntitySerializer serializer = new KryoEntitySerializer();

  private static AuditInfo audit() {
    return AuditInfo.builder()
        .withCreator("creator")
        .withCreateTime(Instant.ofEpochMilli(1_700_000_000_000L))
        .withLastModifier("modifier")
        .withLastModifiedTime(Instant.ofEpochMilli(1_700_000_001_000L))
        .build();
  }

  private <E extends Entity> E roundTrip(E entity) {
    byte[] bytes = serializer.serialize(entity);
    Entity back = serializer.deserialize(bytes);
    Assertions.assertEquals(entity.getClass(), back.getClass());
    Assertions.assertEquals(entity, back);
    @SuppressWarnings("unchecked")
    E typed = (E) back;
    return typed;
  }

  @Test
  void testMetalake() {
    BaseMetalake metalake = TestUtil.getTestMetalake(1L, "m1", "comment");
    BaseMetalake back = roundTrip(metalake);
    Assertions.assertEquals(metalake.nameIdentifier(), back.nameIdentifier());
    Assertions.assertEquals(metalake.getVersion(), back.getVersion());
  }

  @Test
  void testCatalog() {
    CatalogEntity catalog =
        TestUtil.getTestCatalogEntity(2L, "c1", Namespace.of("m1"), "hive", "comment");
    CatalogEntity back = roundTrip(catalog);
    Assertions.assertEquals(catalog.getProvider(), back.getProvider());
    Assertions.assertEquals(catalog.getType(), back.getType());
    Assertions.assertEquals(catalog.getProperties(), back.getProperties());
  }

  @Test
  void testSchemaWithProperties() {
    SchemaEntity schema =
        SchemaEntity.builder()
            .withId(3L)
            .withName("s1")
            .withNamespace(Namespace.of("m1", "c1"))
            .withComment("comment")
            .withProperties(ImmutableMap.of("k1", "v1", "k2", "v2"))
            .withAuditInfo(audit())
            .build();
    SchemaEntity back = roundTrip(schema);
    Assertions.assertEquals(ImmutableMap.of("k1", "v1", "k2", "v2"), back.properties());
    Assertions.assertEquals(audit(), back.auditInfo());
  }

  @Test
  void testTableWithColumnsPartitioningSortOrdersDistributionAndIndexes() {
    ColumnEntity id =
        ColumnEntity.builder()
            .withId(10L)
            .withName("id")
            .withPosition(0)
            .withDataType(Types.LongType.get())
            .withNullable(false)
            .withAutoIncrement(true)
            .withDefaultValue(Column.DEFAULT_VALUE_NOT_SET)
            .withAuditInfo(audit())
            .build();
    ColumnEntity amount =
        ColumnEntity.builder()
            .withId(11L)
            .withName("amount")
            .withPosition(1)
            .withDataType(Types.DecimalType.of(10, 2))
            .withComment("money")
            .withNullable(true)
            .withAutoIncrement(false)
            .withDefaultValue(Literals.integerLiteral(0))
            .withAuditInfo(audit())
            .build();
    ColumnEntity tags =
        ColumnEntity.builder()
            .withId(12L)
            .withName("tags")
            .withPosition(2)
            .withDataType(
                Types.StructType.of(
                    Types.StructType.Field.notNullField("name", Types.StringType.get()),
                    Types.StructType.Field.nullableField(
                        "values", Types.ListType.nullable(Types.IntegerType.get()))))
            .withNullable(true)
            .withAutoIncrement(false)
            .withDefaultValue(Column.DEFAULT_VALUE_NOT_SET)
            .withAuditInfo(audit())
            .build();
    Transform[] partitioning = {
      Transforms.identity("id"), Transforms.bucket(4, new String[] {"amount"})
    };
    SortOrder[] sortOrders = {SortOrders.ascending(NamedReference.field("amount"))};
    Distribution distribution = Distributions.hash(8, NamedReference.field("id"));
    Index[] indexes = {Indexes.primary("pk", new String[][] {{"id"}})};
    TableEntity table =
        TableEntity.builder()
            .withId(4L)
            .withName("t1")
            .withNamespace(Namespace.of("m1", "c1", "s1"))
            .withComment("comment")
            .withColumns(ImmutableList.of(id, amount, tags))
            .withProperties(ImmutableMap.of("format", "parquet"))
            .withPartitioning(partitioning)
            .withSortOrders(sortOrders)
            .withDistribution(distribution)
            .withIndexes(indexes)
            .withAuditInfo(audit())
            .build();

    TableEntity back = roundTrip(table);

    List<ColumnEntity> columns = back.columns();
    Assertions.assertEquals(3, columns.size());
    // Primitive types come back as the shared singletons, so identity comparisons still hold.
    Assertions.assertSame(Types.LongType.get(), columns.get(0).dataType());
    Assertions.assertEquals(Types.DecimalType.of(10, 2), columns.get(1).dataType());
    Assertions.assertEquals(Literals.integerLiteral(0), columns.get(1).defaultValue());
    Assertions.assertEquals(tags.dataType(), columns.get(2).dataType());
    Assertions.assertTrue(columns.get(0).autoIncrement());
    Assertions.assertArrayEquals(partitioning, back.partitioning());
    Assertions.assertArrayEquals(sortOrders, back.sortOrders());
    Assertions.assertEquals(distribution, back.distribution());
    Assertions.assertArrayEquals(indexes, back.indexes());
    Assertions.assertEquals(ImmutableMap.of("format", "parquet"), back.properties());
  }

  @Test
  void testTopic() {
    TopicEntity topic =
        TopicEntity.builder()
            .withId(5L)
            .withName("topic1")
            .withNamespace(NamespaceUtil.ofTopic("m1", "c1", "s1"))
            .withComment("comment")
            .withProperties(ImmutableMap.of("partitions", "3"))
            .withAuditInfo(audit())
            .build();
    roundTrip(topic);
  }

  @Test
  void testView() {
    Column[] columns = {
      Column.of("id", Types.IntegerType.get(), "identifier"),
      Column.of("name", Types.VarCharType.of(64), "name", Literals.stringLiteral("n/a"))
    };
    Representation[] representations = {
      SQLRepresentation.builder().withDialect("spark").withSql("select 1").build()
    };
    ViewEntity view =
        ViewEntity.builder()
            .withId(6L)
            .withName("v1")
            .withNamespace(NamespaceUtil.ofView("m1", "c1", "s1"))
            .withComment("comment")
            .withColumns(columns)
            .withRepresentations(representations)
            .withDefaultCatalog("c1")
            .withDefaultSchema("s1")
            .withProperties(ImmutableMap.of("owner", "tester"))
            .withAuditInfo(audit())
            .build();
    ViewEntity back = roundTrip(view);
    Assertions.assertArrayEquals(columns, back.columns());
    Assertions.assertArrayEquals(representations, back.representations());
    Assertions.assertSame(Types.IntegerType.get(), back.columns()[0].dataType());
  }

  @Test
  void testFilesetWithImmutableStorageLocations() {
    FilesetEntity fileset =
        TestUtil.getTestFileSetEntity(
            7L,
            "f1",
            "hdfs://tmp/f1",
            NamespaceUtil.ofFileset("m1", "c1", "s1"),
            "c",
            Fileset.Type.MANAGED);
    FilesetEntity back = roundTrip(fileset);
    Assertions.assertEquals(fileset.storageLocations(), back.storageLocations());
    Assertions.assertEquals(fileset.filesetType(), back.filesetType());
  }

  @Test
  void testTagWithAllowedValues() {
    TagEntity tag =
        TagEntity.builder()
            .withId(8L)
            .withName("tag1")
            .withNamespace(NamespaceUtil.ofTag("m1"))
            .withComment("comment")
            .withProperties(ImmutableMap.of("color", "red"))
            .withAllowedValues(new String[] {"a", "b"})
            .withAuditInfo(audit())
            .build();
    TagEntity back = roundTrip(tag);
    Assertions.assertEquals(tag.valueConstraint(), back.valueConstraint());
  }

  @Test
  void testPolicyWithCustomContent() {
    PolicyEntity policy =
        PolicyEntity.builder()
            .withId(9L)
            .withName("p1")
            .withNamespace(NamespaceUtil.ofPolicy("m1"))
            .withPolicyType(Policy.BuiltInType.CUSTOM)
            .withComment("comment")
            .withEnabled(true)
            .withContent(
                PolicyContents.custom(
                    ImmutableMap.of("rule", 1, "nested", ImmutableList.of("x", "y")),
                    ImmutableSet.of(MetadataObject.Type.TABLE, MetadataObject.Type.SCHEMA),
                    ImmutableMap.of("k", "v")))
            .withAuditInfo(audit())
            .build();
    PolicyEntity back = roundTrip(policy);
    Assertions.assertEquals(policy.content(), back.content());
    Assertions.assertTrue(back.enabled());
  }

  @Test
  void testJob() {
    JobEntity job =
        JobEntity.builder()
            .withId(13L)
            .withJobExecutionId("exec-1")
            .withNamespace(NamespaceUtil.ofJob("m1"))
            .withStatus(JobHandle.Status.QUEUED)
            .withJobTemplateName("template")
            .withStartedAt(1L)
            .withFinishedAt(2L)
            .withRuntimeJobTemplate("{}")
            .withAuditInfo(audit())
            .build();
    JobEntity back = roundTrip(job);
    Assertions.assertEquals(JobHandle.Status.QUEUED, back.status());
  }

  @Test
  void testEveryCacheableTypeHasARoundTripTest() {
    // Guards the allowlist: a newly cacheable type needs its own round-trip test above.
    List<Entity.EntityType> cacheable =
        Arrays.stream(Entity.EntityType.values())
            .filter(BaseEntityCache::isCacheable)
            .collect(Collectors.toList());
    Assertions.assertEquals(
        ImmutableList.of(
            Entity.EntityType.METALAKE,
            Entity.EntityType.CATALOG,
            Entity.EntityType.SCHEMA,
            Entity.EntityType.TABLE,
            Entity.EntityType.VIEW,
            Entity.EntityType.FILESET,
            Entity.EntityType.TOPIC,
            Entity.EntityType.TAG,
            Entity.EntityType.POLICY,
            Entity.EntityType.JOB),
        cacheable);
  }

  @Test
  void testGarbageIsRejectedNotMisread() {
    byte[] garbage = "definitely not kryo".getBytes(StandardCharsets.UTF_8);
    Assertions.assertThrows(KryoException.class, () -> serializer.deserialize(garbage));
  }

  @Test
  void testNonEntityPayloadIsRejected() {
    Map<String, String> notAnEntity = ImmutableMap.of("a", "b");
    byte[] bytes;
    com.esotericsoftware.kryo.Kryo kryo = new com.esotericsoftware.kryo.Kryo();
    kryo.setRegistrationRequired(false);
    try (com.esotericsoftware.kryo.io.Output output =
        new com.esotericsoftware.kryo.io.Output(64, -1)) {
      kryo.writeClassAndObject(output, new java.util.HashMap<>(notAnEntity));
      bytes = output.toBytes();
    }
    Assertions.assertThrows(KryoException.class, () -> serializer.deserialize(bytes));
  }
}
