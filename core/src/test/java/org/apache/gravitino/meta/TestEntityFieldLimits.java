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
package org.apache.gravitino.meta;

import static org.apache.gravitino.file.Fileset.LOCATION_NAME_UNKNOWN;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import java.util.function.Function;
import java.util.stream.Stream;
import org.apache.commons.lang3.StringUtils;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.Entity;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.authorization.Privileges;
import org.apache.gravitino.authorization.SecurableObjects;
import org.apache.gravitino.file.Fileset;
import org.apache.gravitino.job.ShellJobTemplate;
import org.apache.gravitino.model.ModelVersion;
import org.apache.gravitino.policy.Policy;
import org.apache.gravitino.policy.PolicyContents;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

public class TestEntityFieldLimits {

  private static final Namespace NAMESPACE = Namespace.of("metalake");

  @ParameterizedTest(name = "{0}")
  @MethodSource("nameBuilders")
  public void testNameLength(String entityType, Function<String, Entity> builder) {
    assertLengthLimit(builder, "name", entityType, EntityFieldLimits.MAX_NAME_LENGTH);
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("commentBuilders")
  public void testCommentLength(String entityType, Function<String, Entity> builder) {
    assertLengthLimit(builder, "comment", entityType, EntityFieldLimits.MAX_COMMENT_LENGTH);
  }

  @Test
  public void testModelVersionAliasLength() {
    Function<String, Entity> builder =
        alias ->
            ModelVersionEntity.builder()
                .withModelIdentifier(NameIdentifier.of("m1", "c1", "s1", "model1"))
                .withVersion(1)
                .withAliases(Lists.newArrayList("alias", alias))
                .withUris(ImmutableMap.of(ModelVersion.URI_NAME_UNKNOWN, "test_uri"))
                .withAuditInfo(AuditInfo.EMPTY)
                .build();
    assertLengthLimit(builder, "alias", "model version", EntityFieldLimits.MAX_NAME_LENGTH);
  }

  @Test
  public void testLengthCountsCodePoints() {
    // An emoji is two UTF-16 chars but one character for MySQL (utf8mb4) and PostgreSQL, so a
    // name of 128 emojis can be stored and must still pass validation when it is read back.
    String emoji = new String(Character.toChars(0x1F600));
    String maxLengthName = StringUtils.repeat(emoji, EntityFieldLimits.MAX_NAME_LENGTH);
    Assertions.assertDoesNotThrow(() -> tagBuilder(maxLengthName, null));

    IllegalArgumentException exception =
        Assertions.assertThrows(
            IllegalArgumentException.class, () -> tagBuilder(maxLengthName + emoji, null));
    Assertions.assertEquals(
        "The name of the tag must not exceed 128 characters", exception.getMessage());
  }

  @Test
  public void testCheckMaxLength() {
    Assertions.assertDoesNotThrow(() -> EntityFieldLimits.checkMaxLength(null, 1, "name", null));
    Assertions.assertDoesNotThrow(() -> EntityFieldLimits.checkMaxLength("a", 1, "name", null));

    IllegalArgumentException exception =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () -> EntityFieldLimits.checkMaxLength("ab", 1, "name", null));
    Assertions.assertEquals("Field name must not exceed 1 characters", exception.getMessage());
  }

  private static void assertLengthLimit(
      Function<String, Entity> builder, String fieldName, String entityType, int maxLength) {
    Assertions.assertDoesNotThrow(() -> builder.apply(StringUtils.repeat("a", maxLength)));

    String tooLong = StringUtils.repeat("a", maxLength + 1);
    IllegalArgumentException exception =
        Assertions.assertThrows(IllegalArgumentException.class, () -> builder.apply(tooLong));
    Assertions.assertEquals(
        String.format(
            "The %s of the %s must not exceed %d characters", fieldName, entityType, maxLength),
        exception.getMessage());
  }

  private static Stream<Arguments> nameBuilders() {
    return Stream.of(
        Arguments.of("tag", (Function<String, Entity>) name -> tagBuilder(name, null)),
        Arguments.of(
            "policy",
            (Function<String, Entity>)
                name ->
                    PolicyEntity.builder()
                        .withId(1L)
                        .withName(name)
                        .withNamespace(NAMESPACE)
                        .withPolicyType(Policy.BuiltInType.CUSTOM)
                        .withEnabled(false)
                        .withContent(
                            PolicyContents.custom(
                                ImmutableMap.of("k", "v"),
                                ImmutableSet.of(MetadataObject.Type.TABLE),
                                ImmutableMap.of()))
                        .withAuditInfo(AuditInfo.EMPTY)
                        .build()),
        Arguments.of(
            "role",
            (Function<String, Entity>)
                name ->
                    RoleEntity.builder()
                        .withId(1L)
                        .withName(name)
                        .withAuditInfo(AuditInfo.EMPTY)
                        .withSecurableObjects(
                            Lists.newArrayList(
                                SecurableObjects.ofCatalog(
                                    "catalog", Lists.newArrayList(Privileges.UseCatalog.allow()))))
                        .build()),
        Arguments.of(
            "user",
            (Function<String, Entity>)
                name ->
                    UserEntity.builder()
                        .withId(1L)
                        .withName(name)
                        .withAuditInfo(AuditInfo.EMPTY)
                        .build()),
        Arguments.of(
            "group",
            (Function<String, Entity>)
                name ->
                    GroupEntity.builder()
                        .withId(1L)
                        .withName(name)
                        .withAuditInfo(AuditInfo.EMPTY)
                        .build()),
        Arguments.of(
            "job template",
            (Function<String, Entity>)
                name ->
                    JobTemplateEntity.builder()
                        .withId(1L)
                        .withName(name)
                        .withNamespace(NAMESPACE)
                        .withTemplateContent(
                            JobTemplateEntity.TemplateContent.fromJobTemplate(
                                ShellJobTemplate.builder()
                                    .withName("template")
                                    .withExecutable("/bin/echo")
                                    .build()))
                        .withAuditInfo(AuditInfo.EMPTY)
                        .build()));
  }

  private static Stream<Arguments> commentBuilders() {
    return Stream.of(
        Arguments.of("tag", (Function<String, Entity>) comment -> tagBuilder("tag", comment)),
        Arguments.of(
            "metalake",
            (Function<String, Entity>)
                comment ->
                    BaseMetalake.builder()
                        .withId(1L)
                        .withName("metalake")
                        .withComment(comment)
                        .withAuditInfo(AuditInfo.EMPTY)
                        .withVersion(SchemaVersion.V_0_1)
                        .build()),
        Arguments.of(
            "catalog",
            (Function<String, Entity>)
                comment ->
                    CatalogEntity.builder()
                        .withId(1L)
                        .withName("catalog")
                        .withComment(comment)
                        .withType(Catalog.Type.RELATIONAL)
                        .withProvider("test")
                        .withAuditInfo(AuditInfo.EMPTY)
                        .build()),
        Arguments.of(
            "schema",
            (Function<String, Entity>)
                comment ->
                    SchemaEntity.builder()
                        .withId(1L)
                        .withName("schema")
                        .withComment(comment)
                        .withAuditInfo(AuditInfo.EMPTY)
                        .build()),
        Arguments.of(
            "fileset",
            (Function<String, Entity>)
                comment ->
                    FilesetEntity.builder()
                        .withId(1L)
                        .withName("fileset")
                        .withComment(comment)
                        .withFilesetType(Fileset.Type.MANAGED)
                        .withStorageLocations(ImmutableMap.of(LOCATION_NAME_UNKNOWN, "location"))
                        .withAuditInfo(AuditInfo.EMPTY)
                        .build()),
        Arguments.of(
            "topic",
            (Function<String, Entity>)
                comment ->
                    TopicEntity.builder()
                        .withId(1L)
                        .withName("topic")
                        .withComment(comment)
                        .withAuditInfo(AuditInfo.EMPTY)
                        .build()));
  }

  private static TagEntity tagBuilder(String name, String comment) {
    return TagEntity.builder()
        .withId(1L)
        .withName(name)
        .withNamespace(NAMESPACE)
        .withComment(comment)
        .withAuditInfo(AuditInfo.EMPTY)
        .build();
  }
}
