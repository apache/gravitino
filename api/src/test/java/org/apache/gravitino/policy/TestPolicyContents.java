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

package org.apache.gravitino.policy;

import com.google.common.collect.ImmutableSet;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.authorization.Privilege;
import org.apache.gravitino.authorization.Privileges;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestPolicyContents {

  @Test
  void testIcebergCompactionContentUsesDefaults() {
    IcebergDataCompactionContent content =
        (IcebergDataCompactionContent) PolicyContents.icebergDataCompaction();

    Assertions.assertEquals(
        IcebergDataCompactionContent.DEFAULT_MIN_DATA_FILE_MSE,
        content.rules().get("minDataFileMse"));
    Assertions.assertEquals(
        IcebergDataCompactionContent.DEFAULT_MIN_DELETE_FILE_NUMBER,
        content.rules().get("minDeleteFileNumber"));
    Assertions.assertEquals(1L, content.rules().get("dataFileMseWeight"));
    Assertions.assertEquals(100L, content.rules().get("deleteFileNumberWeight"));
    Assertions.assertEquals(50L, content.rules().get("max-partition-num"));
    Assertions.assertNull(content.rules().get("job.options.target-file-size-bytes"));
    Assertions.assertNull(content.rules().get("job.options.min-input-files"));
    Assertions.assertNull(content.rules().get("job.options.delete-file-threshold"));
  }

  @Test
  void testIcebergCompactionContentGeneratesOptimizerFields() {
    IcebergDataCompactionContent content =
        (IcebergDataCompactionContent)
            PolicyContents.icebergDataCompaction(
                1000L, 1L, mapOf("target-file-size-bytes", "1048576", "min-input-files", "1"));

    Assertions.assertEquals("iceberg-data-compaction", content.properties().get("strategy.type"));
    Assertions.assertEquals(
        "builtin-iceberg-rewrite-data-files", content.properties().get("job.template-name"));
    Assertions.assertEquals(1000L, content.rules().get("minDataFileMse"));
    Assertions.assertEquals(1L, content.rules().get("minDeleteFileNumber"));
    Assertions.assertEquals(1L, content.rules().get("dataFileMseWeight"));
    Assertions.assertEquals(100L, content.rules().get("deleteFileNumberWeight"));
    Assertions.assertEquals(50L, content.rules().get("max-partition-num"));
    Assertions.assertEquals(
        "custom-data-file-mse >= minDataFileMse || custom-delete-file-number >= minDeleteFileNumber",
        content.rules().get("trigger-expr"));
    Assertions.assertEquals(
        "custom-data-file-mse * dataFileMseWeight"
            + " + custom-delete-file-number * deleteFileNumberWeight",
        content.rules().get("score-expr"));
    Assertions.assertEquals("1048576", content.rules().get("job.options.target-file-size-bytes"));
    Assertions.assertEquals("1", content.rules().get("job.options.min-input-files"));
    Assertions.assertEquals(
        ImmutableSet.of(
            MetadataObject.Type.CATALOG, MetadataObject.Type.SCHEMA, MetadataObject.Type.TABLE),
        content.supportedObjectTypes());
  }

  @Test
  void testIcebergCompactionContentSupportsCustomWeights() {
    IcebergDataCompactionContent content =
        (IcebergDataCompactionContent)
            PolicyContents.icebergDataCompaction(
                1000L,
                1L,
                3L,
                200L,
                88L,
                mapOf("target-file-size-bytes", "1048576", "min-input-files", "1"));

    Assertions.assertEquals(3L, content.rules().get("dataFileMseWeight"));
    Assertions.assertEquals(200L, content.rules().get("deleteFileNumberWeight"));
    Assertions.assertEquals(88L, content.rules().get("max-partition-num"));
    Assertions.assertDoesNotThrow(content::validate);
  }

  @Test
  void testIcebergCompactionContentRejectsInvalidRewriteOptionKey() {
    IcebergDataCompactionContent content =
        (IcebergDataCompactionContent)
            PolicyContents.icebergDataCompaction(
                1000L, 1L, mapOf("job.options.target-file-size-bytes", "1048576"));

    IllegalArgumentException exception =
        Assertions.assertThrows(IllegalArgumentException.class, content::validate);
    Assertions.assertTrue(exception.getMessage().contains("must not start with"));
  }

  @Test
  void testIcebergCompactionContentRejectsInvalidMaxPartitionNum() {
    IcebergDataCompactionContent content =
        (IcebergDataCompactionContent)
            PolicyContents.icebergDataCompaction(
                1000L, 1L, 2L, 10L, 0L, mapOf("target-file-size-bytes", "1048576"));

    IllegalArgumentException exception =
        Assertions.assertThrows(IllegalArgumentException.class, content::validate);
    Assertions.assertTrue(exception.getMessage().contains("maxPartitionNum"));
  }

  @Test
  void testAccessControlContent() {
    AccessControlContent content =
        (AccessControlContent)
            PolicyContents.accessControl(
                Arrays.asList(Privilege.Name.SELECT_TABLE, Privilege.Name.MODIFY_TABLE),
                Arrays.asList("analyst", "data_engineer"));

    Assertions.assertDoesNotThrow(content::validate);
    Assertions.assertEquals(
        Arrays.asList(Privilege.Name.SELECT_TABLE, Privilege.Name.MODIFY_TABLE),
        content.privileges());
    Assertions.assertEquals(Arrays.asList("analyst", "data_engineer"), content.applicableRoles());
    Assertions.assertEquals(
        Arrays.asList("SELECT_TABLE", "MODIFY_TABLE"),
        content.rules().get(AccessControlContent.PRIVILEGES_KEY));
    Assertions.assertEquals(
        Arrays.asList("analyst", "data_engineer"),
        content.rules().get(AccessControlContent.APPLICABLE_ROLES_KEY));
    Assertions.assertTrue(content.properties().isEmpty());
  }

  @Test
  void testAccessControlContentSupportedObjectTypes() {
    Assertions.assertEquals(
        ImmutableSet.of(
            MetadataObject.Type.CATALOG,
            MetadataObject.Type.SCHEMA,
            MetadataObject.Type.TABLE,
            MetadataObject.Type.VIEW,
            MetadataObject.Type.FILESET,
            MetadataObject.Type.TOPIC,
            MetadataObject.Type.MODEL,
            MetadataObject.Type.FUNCTION),
        PolicyContents.accessControl(
                Collections.singletonList(Privilege.Name.SELECT_TABLE),
                Collections.singletonList("analyst"))
            .supportedObjectTypes());
  }

  @Test
  void testAccessControlContentSupportedObjectTypesTrackPermittedPrivileges() {
    Set<MetadataObject.Type> union =
        Arrays.stream(MetadataObject.Type.values())
            .filter(type -> type != MetadataObject.Type.METALAKE)
            .filter(
                type ->
                    AccessControlContent.PERMITTED_PRIVILEGES.stream()
                        .anyMatch(name -> Privileges.allow(name).canBindTo(type)))
            .collect(Collectors.toSet());

    Assertions.assertEquals(
        union,
        PolicyContents.accessControl(
                Collections.singletonList(Privilege.Name.SELECT_TABLE),
                Collections.singletonList("analyst"))
            .supportedObjectTypes(),
        "supportedObjectTypes must stay the union of canBindTo over PERMITTED_PRIVILEGES, "
            + "minus METALAKE, which a tag cannot be applied to");
  }

  @Test
  void testAccessControlContentAcceptsEveryPermittedPrivilege() {
    for (Privilege.Name privilege : AccessControlContent.PERMITTED_PRIVILEGES) {
      PolicyContent content =
          PolicyContents.accessControl(
              Collections.singletonList(privilege), Collections.singletonList("analyst"));
      Assertions.assertDoesNotThrow(content::validate, privilege + " should be permitted");
    }
  }

  @Test
  void testAccessControlContentRejectsPrivilegesOutsideAllowlist() {
    for (Privilege.Name privilege : Privilege.Name.values()) {
      if (AccessControlContent.PERMITTED_PRIVILEGES.contains(privilege)) {
        continue;
      }

      PolicyContent content =
          PolicyContents.accessControl(
              Collections.singletonList(privilege), Collections.singletonList("analyst"));
      IllegalArgumentException exception =
          Assertions.assertThrows(
              IllegalArgumentException.class,
              content::validate,
              privilege + " should not be conferrable by a tag");
      Assertions.assertTrue(exception.getMessage().contains(privilege.name()));
    }
  }

  @Test
  void testAccessControlContentRejectsEmptyPrivileges() {
    List<String> roles = Collections.singletonList("analyst");
    Assertions.assertTrue(
        Assertions.assertThrows(
                IllegalArgumentException.class,
                () -> PolicyContents.accessControl(null, roles).validate())
            .getMessage()
            .contains("privileges cannot be empty"));
    Assertions.assertTrue(
        Assertions.assertThrows(
                IllegalArgumentException.class,
                () -> PolicyContents.accessControl(Collections.emptyList(), roles).validate())
            .getMessage()
            .contains("privileges cannot be empty"));
  }

  @Test
  void testAccessControlContentRejectsInvalidApplicableRoles() {
    List<Privilege.Name> privileges = Collections.singletonList(Privilege.Name.SELECT_TABLE);
    Assertions.assertTrue(
        Assertions.assertThrows(
                IllegalArgumentException.class,
                () -> PolicyContents.accessControl(privileges, Collections.emptyList()).validate())
            .getMessage()
            .contains("applicableRoles cannot be empty"));
    Assertions.assertTrue(
        Assertions.assertThrows(
                IllegalArgumentException.class,
                () -> PolicyContents.accessControl(privileges, null).validate())
            .getMessage()
            .contains("applicableRoles cannot be empty"));

    for (List<String> roles :
        Arrays.asList(
            Arrays.asList("analyst", " "),
            Arrays.asList("analyst", ""),
            Arrays.asList("analyst", (String) null))) {
      Assertions.assertTrue(
          Assertions.assertThrows(
                  IllegalArgumentException.class,
                  () -> PolicyContents.accessControl(privileges, roles).validate())
              .getMessage()
              .contains("applicable role name cannot be blank"),
          roles + " should be rejected");
    }
  }

  @Test
  void testAccessControlContentEquality() {
    List<Privilege.Name> privileges = Collections.singletonList(Privilege.Name.SELECT_TABLE);
    PolicyContent content =
        PolicyContents.accessControl(privileges, Collections.singletonList("analyst"));

    Assertions.assertEquals(
        content, PolicyContents.accessControl(privileges, Collections.singletonList("analyst")));
    Assertions.assertEquals(
        content.hashCode(),
        PolicyContents.accessControl(privileges, Collections.singletonList("analyst")).hashCode());
    Assertions.assertNotEquals(
        content,
        PolicyContents.accessControl(privileges, Collections.singletonList("data_engineer")));
  }

  private static Map<String, String> mapOf(String key, String value) {
    Map<String, String> map = new HashMap<>();
    map.put(key, value);
    return map;
  }

  private static Map<String, String> mapOf(String key1, String value1, String key2, String value2) {
    Map<String, String> map = new HashMap<>();
    map.put(key1, value1);
    map.put(key2, value2);
    return map;
  }
}
