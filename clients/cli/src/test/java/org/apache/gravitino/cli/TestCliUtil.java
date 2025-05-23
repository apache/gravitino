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

package org.apache.gravitino.cli;

import static org.apache.gravitino.rel.Column.DEFAULT_VALUE_NOT_SET;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import java.util.List;
import java.util.Map;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.MetadataObject;
import org.apache.gravitino.Metalake;
import org.apache.gravitino.Schema;
import org.apache.gravitino.authorization.Group;
import org.apache.gravitino.authorization.Privileges;
import org.apache.gravitino.authorization.Role;
import org.apache.gravitino.authorization.SecurableObject;
import org.apache.gravitino.authorization.User;
import org.apache.gravitino.file.Fileset;
import org.apache.gravitino.messaging.Topic;
import org.apache.gravitino.model.Model;
import org.apache.gravitino.rel.Table;
import org.apache.gravitino.rel.expressions.Expression;
import org.apache.gravitino.rel.types.Type;
import org.apache.gravitino.rel.types.Types;
import org.apache.gravitino.tag.Tag;
import org.testcontainers.shaded.com.google.common.collect.ImmutableList;

public class TestCliUtil {
  private TestCliUtil() {
    // Utility class, no public constructor
  }

  /**
   * Creates a mock CommandContext object.
   *
   * @return A mock CommandContext object
   */
  public static CommandContext getMockContext() {
    CommandContext mockContext = mock(CommandContext.class);

    return mockContext;
  }

  /**
   * Create a mock Metalake object with the given name and comment.
   *
   * @param name The name of the Metalake object
   * @param comment The comment of the Metalake object
   * @return A mock Metalake object
   */
  public static Metalake getMockMetalake(String name, String comment) {
    Metalake mockMetalake = mock(Metalake.class);
    when(mockMetalake.name()).thenReturn(name);
    when(mockMetalake.comment()).thenReturn(comment);

    return mockMetalake;
  }

  /**
   * Create a mock catalog object with the given name, type, provider, and comment.
   *
   * @param name The name of the catalog object
   * @param type The type of the catalog object
   * @param provider The provider of the catalog object
   * @param comment The comment of the catalog object
   * @return A mock catalog object
   */
  public static Catalog getMockCatalog(
      String name, Catalog.Type type, String provider, String comment) {
    Catalog mockCatalog = mock(Catalog.class);
    when(mockCatalog.name()).thenReturn(name);
    when(mockCatalog.type()).thenReturn(type);
    when(mockCatalog.provider()).thenReturn(provider);
    when(mockCatalog.comment()).thenReturn(comment);

    return mockCatalog;
  }

  /**
   * Create a mock schema object with the given name and comment.
   *
   * @param name The name of the schema object
   * @param comment The comment of the schema object
   * @return A mock schema object
   */
  public static Schema getMockSchema(String name, String comment) {
    Schema mockSchema = mock(Schema.class);
    when(mockSchema.name()).thenReturn(name);
    when(mockSchema.comment()).thenReturn(comment);

    return mockSchema;
  }

  /**
   * Create a mock table object with the given name and comment.
   *
   * @param name The name of the table object
   * @param comment The comment of the table object
   * @return A mock table object.
   */
  public static Table getMockTable(String name, String comment) {
    Table mockTable = mock(Table.class);
    org.apache.gravitino.rel.Column mockColumnInt =
        getMockColumn(
            "id",
            Types.IntegerType.get(),
            "This is a int column",
            false,
            true,
            DEFAULT_VALUE_NOT_SET);
    org.apache.gravitino.rel.Column mockColumnString =
        getMockColumn(
            "name",
            Types.StringType.get(),
            "This is a string column",
            true,
            false,
            DEFAULT_VALUE_NOT_SET);

    when(mockTable.name()).thenReturn(name);
    when(mockTable.comment()).thenReturn(comment);
    when(mockTable.columns())
        .thenReturn(new org.apache.gravitino.rel.Column[] {mockColumnInt, mockColumnString});

    return mockTable;
  }

  /**
   * Create a new mock column object with the given name, data type, comment, nullable,
   * auto-increment, and default value.
   *
   * @param name The name of the column object
   * @param dataType The data type of the column object
   * @param comment The comment of the column object
   * @param nullable Whether the column is nullable or not
   * @param autoIncrement Whether the column is auto-increment or not
   * @param defaultValue The default value of the column object
   * @return A new mock column object
   */
  public static org.apache.gravitino.rel.Column getMockColumn(
      String name,
      Type dataType,
      String comment,
      boolean nullable,
      boolean autoIncrement,
      Expression defaultValue) {

    org.apache.gravitino.rel.Column mockColumn = mock(org.apache.gravitino.rel.Column.class);
    when(mockColumn.name()).thenReturn(name);
    when(mockColumn.dataType()).thenReturn(dataType);
    when(mockColumn.comment()).thenReturn(comment);
    when(mockColumn.nullable()).thenReturn(nullable);
    when(mockColumn.defaultValue()).thenReturn(defaultValue);
    when(mockColumn.autoIncrement()).thenReturn(autoIncrement);

    return mockColumn;
  }

  /**
   * Create a mock model object with the given name, comment, and last version.
   *
   * @param name The name of the model object
   * @param comment The comment of the model object
   * @param lastVersion The last version of the model object
   * @return A mock model object
   */
  public static Model getMockModel(String name, String comment, int lastVersion) {
    Model mockModel = mock(Model.class);
    when(mockModel.name()).thenReturn(name);
    when(mockModel.comment()).thenReturn(comment);
    when(mockModel.latestVersion()).thenReturn(lastVersion);

    return mockModel;
  }

  /**
   * Create a mock user object with the given name and roles.
   *
   * @param name The name of the user object
   * @param roles The roles of the user object
   * @return A mock user object
   */
  public static User getMockUser(String name, List<String> roles) {
    User mockUser = mock(User.class);
    when(mockUser.name()).thenReturn(name);
    when(mockUser.roles()).thenReturn(roles);

    return mockUser;
  }

  /**
   * Create a mock group object with the given name and roles.
   *
   * @param name The name of the group object
   * @param roles The roles belonging to the group object
   * @return A mock group object
   */
  public static Group getMockGroup(String name, List<String> roles) {
    Group mockGroup = mock(Group.class);
    when(mockGroup.name()).thenReturn(name);
    when(mockGroup.roles()).thenReturn(roles);

    return mockGroup;
  }

  /**
   * Create a mock Topic object with the given name and comment.
   *
   * @param name The name of the topic object
   * @param comment The comment of the topic object
   * @return A mock topic object
   */
  public static Topic getMockTopic(String name, String comment) {
    Topic mockTopic = mock(Topic.class);
    when(mockTopic.name()).thenReturn(name);
    when(mockTopic.comment()).thenReturn(comment);

    return mockTopic;
  }

  /**
   * Create a mock Fileset object with the given name, type, comment, and location.
   *
   * @param name The name of the fileset object
   * @param type The type of the fileset object
   * @param comment The comment of the fileset object
   * @param location The location of the fileset object
   * @return A mock fileset object
   */
  public static Fileset getMockFileset(
      String name, Fileset.Type type, String comment, String location) {
    Fileset mockFileset = mock(Fileset.class);
    when(mockFileset.name()).thenReturn(name);
    when(mockFileset.type()).thenReturn(type);
    when(mockFileset.comment()).thenReturn(comment);
    when(mockFileset.storageLocation()).thenReturn(location);

    return mockFileset;
  }

  /**
   * Create a mock Role object with the given name and securable objects.
   *
   * @param name The name of the role object
   * @return A mock role object
   */
  public static Role getMockRole(String name) {
    Role mockRole = mock(Role.class);
    ImmutableList<SecurableObject> securableObjects =
        ImmutableList.of(
            getMockSecurableObject("demo_table", MetadataObject.Type.TABLE),
            getMockSecurableObject("demo_fileset", MetadataObject.Type.FILESET));

    when(mockRole.name()).thenReturn(name);
    when(mockRole.securableObjects()).thenReturn(securableObjects);

    return mockRole;
  }

  /**
   * Create a mock SecurableObject object with the given name and type.
   *
   * @param name The name of the securable object
   * @param type The type of the securable object
   * @return A mock securable object
   */
  public static SecurableObject getMockSecurableObject(String name, MetadataObject.Type type) {
    SecurableObject mockObject = mock(SecurableObject.class);
    when(mockObject.name()).thenReturn(name);
    when(mockObject.type()).thenReturn(type);
    if (type == MetadataObject.Type.TABLE) {
      when(mockObject.privileges())
          .thenReturn(
              ImmutableList.of(Privileges.CreateTable.allow(), Privileges.SelectTable.allow()));
    } else if (type == MetadataObject.Type.FILESET) {
      when(mockObject.privileges())
          .thenReturn(
              ImmutableList.of(Privileges.CreateFileset.allow(), Privileges.ReadFileset.allow()));
    } else {
      throw new IllegalArgumentException("Unsupported type: " + type);
    }

    return mockObject;
  }

  /**
   * Create a mock Tag object with the given name and comment.
   *
   * @param name The name of the tag object
   * @param comment The comment of the tag object
   * @return A mock tag object
   */
  public static Tag getMockTag(String name, String comment) {
    return getMockTag(name, comment, ImmutableMap.of("k1", "v2", "k2", "v2"));
  }

  /**
   * Create a mock Tag object with the given name, comment, and properties.
   *
   * @param name The name of the tag object
   * @param comment The comment of the tag object
   * @param properties The properties of the tag object
   * @return A mock tag object
   */
  public static Tag getMockTag(String name, String comment, Map<String, String> properties) {
    Tag mockTag = mock(Tag.class);
    when(mockTag.name()).thenReturn(name);
    when(mockTag.comment()).thenReturn(comment);
    when(mockTag.properties()).thenReturn(properties);

    return mockTag;
  }
}
