/* Methods used for testing - basically a seam where we can see what it passed and use mocks to see if the correct command is created. */
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

import java.util.Map;
import org.apache.gravitino.cli.commands.AddColumn;
import org.apache.gravitino.cli.commands.AddRoleToGroup;
import org.apache.gravitino.cli.commands.AddRoleToUser;
import org.apache.gravitino.cli.commands.CatalogAudit;
import org.apache.gravitino.cli.commands.CatalogDetails;
import org.apache.gravitino.cli.commands.CatalogDisable;
import org.apache.gravitino.cli.commands.CatalogEnable;
import org.apache.gravitino.cli.commands.ClientVersion;
import org.apache.gravitino.cli.commands.ColumnAudit;
import org.apache.gravitino.cli.commands.CreateCatalog;
import org.apache.gravitino.cli.commands.CreateFileset;
import org.apache.gravitino.cli.commands.CreateGroup;
import org.apache.gravitino.cli.commands.CreateMetalake;
import org.apache.gravitino.cli.commands.CreateRole;
import org.apache.gravitino.cli.commands.CreateSchema;
import org.apache.gravitino.cli.commands.CreateTable;
import org.apache.gravitino.cli.commands.CreateTag;
import org.apache.gravitino.cli.commands.CreateTopic;
import org.apache.gravitino.cli.commands.CreateUser;
import org.apache.gravitino.cli.commands.DeleteCatalog;
import org.apache.gravitino.cli.commands.DeleteColumn;
import org.apache.gravitino.cli.commands.DeleteFileset;
import org.apache.gravitino.cli.commands.DeleteGroup;
import org.apache.gravitino.cli.commands.DeleteMetalake;
import org.apache.gravitino.cli.commands.DeleteModel;
import org.apache.gravitino.cli.commands.DeleteRole;
import org.apache.gravitino.cli.commands.DeleteSchema;
import org.apache.gravitino.cli.commands.DeleteTable;
import org.apache.gravitino.cli.commands.DeleteTag;
import org.apache.gravitino.cli.commands.DeleteTopic;
import org.apache.gravitino.cli.commands.DeleteUser;
import org.apache.gravitino.cli.commands.FilesetDetails;
import org.apache.gravitino.cli.commands.GrantPrivilegesToRole;
import org.apache.gravitino.cli.commands.GroupAudit;
import org.apache.gravitino.cli.commands.GroupDetails;
import org.apache.gravitino.cli.commands.LinkModel;
import org.apache.gravitino.cli.commands.ListAllTags;
import org.apache.gravitino.cli.commands.ListCatalogProperties;
import org.apache.gravitino.cli.commands.ListCatalogs;
import org.apache.gravitino.cli.commands.ListColumns;
import org.apache.gravitino.cli.commands.ListEntityTags;
import org.apache.gravitino.cli.commands.ListFilesetProperties;
import org.apache.gravitino.cli.commands.ListFilesets;
import org.apache.gravitino.cli.commands.ListGroups;
import org.apache.gravitino.cli.commands.ListIndexes;
import org.apache.gravitino.cli.commands.ListMetalakeProperties;
import org.apache.gravitino.cli.commands.ListMetalakes;
import org.apache.gravitino.cli.commands.ListModel;
import org.apache.gravitino.cli.commands.ListRoles;
import org.apache.gravitino.cli.commands.ListSchema;
import org.apache.gravitino.cli.commands.ListSchemaProperties;
import org.apache.gravitino.cli.commands.ListTableProperties;
import org.apache.gravitino.cli.commands.ListTables;
import org.apache.gravitino.cli.commands.ListTagProperties;
import org.apache.gravitino.cli.commands.ListTopicProperties;
import org.apache.gravitino.cli.commands.ListTopics;
import org.apache.gravitino.cli.commands.ListUsers;
import org.apache.gravitino.cli.commands.MetalakeAudit;
import org.apache.gravitino.cli.commands.MetalakeDetails;
import org.apache.gravitino.cli.commands.MetalakeDisable;
import org.apache.gravitino.cli.commands.MetalakeEnable;
import org.apache.gravitino.cli.commands.ModelAudit;
import org.apache.gravitino.cli.commands.ModelDetails;
import org.apache.gravitino.cli.commands.OwnerDetails;
import org.apache.gravitino.cli.commands.RegisterModel;
import org.apache.gravitino.cli.commands.RemoveAllRoles;
import org.apache.gravitino.cli.commands.RemoveAllTags;
import org.apache.gravitino.cli.commands.RemoveCatalogProperty;
import org.apache.gravitino.cli.commands.RemoveFilesetProperty;
import org.apache.gravitino.cli.commands.RemoveMetalakeProperty;
import org.apache.gravitino.cli.commands.RemoveRoleFromGroup;
import org.apache.gravitino.cli.commands.RemoveRoleFromUser;
import org.apache.gravitino.cli.commands.RemoveSchemaProperty;
import org.apache.gravitino.cli.commands.RemoveTableProperty;
import org.apache.gravitino.cli.commands.RemoveTagProperty;
import org.apache.gravitino.cli.commands.RemoveTopicProperty;
import org.apache.gravitino.cli.commands.RevokeAllPrivileges;
import org.apache.gravitino.cli.commands.RevokePrivilegesFromRole;
import org.apache.gravitino.cli.commands.RoleAudit;
import org.apache.gravitino.cli.commands.RoleDetails;
import org.apache.gravitino.cli.commands.SchemaAudit;
import org.apache.gravitino.cli.commands.SchemaDetails;
import org.apache.gravitino.cli.commands.ServerVersion;
import org.apache.gravitino.cli.commands.SetCatalogProperty;
import org.apache.gravitino.cli.commands.SetFilesetProperty;
import org.apache.gravitino.cli.commands.SetMetalakeProperty;
import org.apache.gravitino.cli.commands.SetOwner;
import org.apache.gravitino.cli.commands.SetSchemaProperty;
import org.apache.gravitino.cli.commands.SetTableProperty;
import org.apache.gravitino.cli.commands.SetTagProperty;
import org.apache.gravitino.cli.commands.SetTopicProperty;
import org.apache.gravitino.cli.commands.TableAudit;
import org.apache.gravitino.cli.commands.TableDetails;
import org.apache.gravitino.cli.commands.TableDistribution;
import org.apache.gravitino.cli.commands.TablePartition;
import org.apache.gravitino.cli.commands.TableSortOrder;
import org.apache.gravitino.cli.commands.TagDetails;
import org.apache.gravitino.cli.commands.TagEntity;
import org.apache.gravitino.cli.commands.TopicDetails;
import org.apache.gravitino.cli.commands.UntagEntity;
import org.apache.gravitino.cli.commands.UpdateCatalogComment;
import org.apache.gravitino.cli.commands.UpdateCatalogName;
import org.apache.gravitino.cli.commands.UpdateColumnAutoIncrement;
import org.apache.gravitino.cli.commands.UpdateColumnComment;
import org.apache.gravitino.cli.commands.UpdateColumnDatatype;
import org.apache.gravitino.cli.commands.UpdateColumnDefault;
import org.apache.gravitino.cli.commands.UpdateColumnName;
import org.apache.gravitino.cli.commands.UpdateColumnNullability;
import org.apache.gravitino.cli.commands.UpdateColumnPosition;
import org.apache.gravitino.cli.commands.UpdateFilesetComment;
import org.apache.gravitino.cli.commands.UpdateFilesetName;
import org.apache.gravitino.cli.commands.UpdateMetalakeComment;
import org.apache.gravitino.cli.commands.UpdateMetalakeName;
import org.apache.gravitino.cli.commands.UpdateTableComment;
import org.apache.gravitino.cli.commands.UpdateTableName;
import org.apache.gravitino.cli.commands.UpdateTagComment;
import org.apache.gravitino.cli.commands.UpdateTagName;
import org.apache.gravitino.cli.commands.UpdateTopicComment;
import org.apache.gravitino.cli.commands.UserAudit;
import org.apache.gravitino.cli.commands.UserDetails;

/*
 * Methods used for testing
 *
 * Basically a seam where we can see what is passed and use mocks to see if the correct command is created.
 */
public class TestableCommandLine {

  protected ClientVersion newClientVersion(String url, boolean ignore) {
    return new ClientVersion(url, ignore);
  }

  protected ServerVersion newServerVersion(String url, boolean ignore) {
    return new ServerVersion(url, ignore);
  }

  protected MetalakeAudit newMetalakeAudit(CommandContext context, String metalake) {
    return new MetalakeAudit(context, metalake);
  }

  protected MetalakeDetails newMetalakeDetails(CommandContext context, String metalake) {
    return new MetalakeDetails(context, metalake);
  }

  protected ListMetalakes newListMetalakes(CommandContext context) {
    return new ListMetalakes(context);
  }

  protected CreateMetalake newCreateMetalake(
      CommandContext context, String metalake, String comment) {
    return new CreateMetalake(context, metalake, comment);
  }

  protected DeleteMetalake newDeleteMetalake(CommandContext context, String metalake) {
    return new DeleteMetalake(context, metalake);
  }

  protected SetMetalakeProperty newSetMetalakeProperty(
      CommandContext context, String metalake, String property, String value) {
    return new SetMetalakeProperty(context, metalake, property, value);
  }

  protected RemoveMetalakeProperty newRemoveMetalakeProperty(
      CommandContext context, String metalake, String property) {
    return new RemoveMetalakeProperty(context, metalake, property);
  }

  protected ListMetalakeProperties newListMetalakeProperties(
      CommandContext context, String metalake) {
    return new ListMetalakeProperties(context, metalake);
  }

  protected UpdateMetalakeComment newUpdateMetalakeComment(
      CommandContext context, String metalake, String comment) {
    return new UpdateMetalakeComment(context, metalake, comment);
  }

  protected UpdateMetalakeName newUpdateMetalakeName(
      CommandContext context, String metalake, String newName) {
    return new UpdateMetalakeName(context, metalake, newName);
  }

  protected CatalogAudit newCatalogAudit(
      String url, boolean ignore, String metalake, String catalog) {
    return new CatalogAudit(url, ignore, metalake, catalog);
  }

  protected CatalogDetails newCatalogDetails(
      String url, boolean ignore, String outputFormat, String metalake, String catalog) {
    return new CatalogDetails(url, ignore, outputFormat, metalake, catalog);
  }

  protected ListCatalogs newListCatalogs(
      String url, boolean ignore, String outputFormat, String metalake) {
    return new ListCatalogs(url, ignore, outputFormat, metalake);
  }

  protected CreateCatalog newCreateCatalog(
      String url,
      boolean ignore,
      String metalake,
      String catalog,
      String provider,
      String comment,
      Map<String, String> properties) {
    return new CreateCatalog(url, ignore, metalake, catalog, provider, comment, properties);
  }

  protected DeleteCatalog newDeleteCatalog(
      String url, boolean ignore, boolean force, String metalake, String catalog) {
    return new DeleteCatalog(url, ignore, force, metalake, catalog);
  }

  protected SetCatalogProperty newSetCatalogProperty(
      String url, boolean ignore, String metalake, String catalog, String property, String value) {
    return new SetCatalogProperty(url, ignore, metalake, catalog, property, value);
  }

  protected RemoveCatalogProperty newRemoveCatalogProperty(
      String url, boolean ignore, String metalake, String catalog, String property) {
    return new RemoveCatalogProperty(url, ignore, metalake, catalog, property);
  }

  protected ListCatalogProperties newListCatalogProperties(
      String url, boolean ignore, String metalake, String catalog) {
    return new ListCatalogProperties(url, ignore, metalake, catalog);
  }

  protected UpdateCatalogComment newUpdateCatalogComment(
      String url, boolean ignore, String metalake, String catalog, String comment) {
    return new UpdateCatalogComment(url, ignore, metalake, catalog, comment);
  }

  protected UpdateCatalogName newUpdateCatalogName(
      String url, boolean ignore, String metalake, String catalog, String newName) {
    return new UpdateCatalogName(url, ignore, metalake, catalog, newName);
  }

  protected SchemaAudit newSchemaAudit(
      CommandContext context, String metalake, String catalog, String schema) {
    return new SchemaAudit(context, metalake, catalog, schema);
  }

  protected SchemaDetails newSchemaDetails(
      CommandContext context, String metalake, String catalog, String schema) {
    return new SchemaDetails(context, metalake, catalog, schema);
  }

  protected ListSchema newListSchema(CommandContext context, String metalake, String catalog) {
    return new ListSchema(context, metalake, catalog);
  }

  protected CreateSchema newCreateSchema(
      CommandContext context, String metalake, String catalog, String schema, String comment) {
    return new CreateSchema(context, metalake, catalog, schema, comment);
  }

  protected DeleteSchema newDeleteSchema(
      CommandContext context, String metalake, String catalog, String schema) {
    return new DeleteSchema(context, metalake, catalog, schema);
  }

  protected SetSchemaProperty newSetSchemaProperty(
      CommandContext context,
      String metalake,
      String catalog,
      String schema,
      String property,
      String value) {
    return new SetSchemaProperty(context, metalake, catalog, schema, property, value);
  }

  protected RemoveSchemaProperty newRemoveSchemaProperty(
      CommandContext context, String metalake, String catalog, String schema, String property) {
    return new RemoveSchemaProperty(context, metalake, catalog, schema, property);
  }

  protected ListSchemaProperties newListSchemaProperties(
      CommandContext context, String metalake, String catalog, String schema) {
    return new ListSchemaProperties(context, metalake, catalog, schema);
  }

  protected TableAudit newTableAudit(
      CommandContext context, String metalake, String catalog, String schema, String table) {
    return new TableAudit(context, metalake, catalog, schema, table);
  }

  protected TableDetails newTableDetails(
      CommandContext context, String metalake, String catalog, String schema, String table) {
    return new TableDetails(context, metalake, catalog, schema, table);
  }

  protected ListTables newListTables(
      CommandContext context, String metalake, String catalog, String schema) {
    return new ListTables(context, metalake, catalog, schema);
  }

  protected DeleteTable newDeleteTable(
      CommandContext context, String metalake, String catalog, String schema, String table) {
    return new DeleteTable(context, metalake, catalog, schema, table);
  }

  protected ListIndexes newListIndexes(
      CommandContext context, String metalake, String catalog, String schema, String table) {
    return new ListIndexes(context, metalake, catalog, schema, table);
  }

  protected TablePartition newTablePartition(
      CommandContext context, String metalake, String catalog, String schema, String table) {
    return new TablePartition(context, metalake, catalog, schema, table);
  }

  protected TableDistribution newTableDistribution(
      CommandContext context, String metalake, String catalog, String schema, String table) {
    return new TableDistribution(context, metalake, catalog, schema, table);
  }

  protected TableSortOrder newTableSortOrder(
      CommandContext context, String metalake, String catalog, String schema, String table) {
    return new TableSortOrder(context, metalake, catalog, schema, table);
  }

  protected UpdateTableComment newUpdateTableComment(
      CommandContext context,
      String metalake,
      String catalog,
      String schema,
      String table,
      String comment) {
    return new UpdateTableComment(context, metalake, catalog, schema, table, comment);
  }

  protected UpdateTableName newUpdateTableName(
      CommandContext context,
      String metalake,
      String catalog,
      String schema,
      String table,
      String rename) {
    return new UpdateTableName(context, metalake, catalog, schema, table, rename);
  }

  protected SetTableProperty newSetTableProperty(
      CommandContext context,
      String metalake,
      String catalog,
      String schema,
      String table,
      String property,
      String value) {
    return new SetTableProperty(context, metalake, catalog, schema, table, property, value);
  }

  protected RemoveTableProperty newRemoveTableProperty(
      CommandContext context,
      String metalake,
      String catalog,
      String schema,
      String table,
      String property) {
    return new RemoveTableProperty(context, metalake, catalog, schema, table, property);
  }

  protected ListTableProperties newListTableProperties(
      CommandContext context, String metalake, String catalog, String schema, String table) {
    return new ListTableProperties(context, metalake, catalog, schema, table);
  }

  protected UserDetails newUserDetails(String url, boolean ignore, String metalake, String user) {
    return new UserDetails(url, ignore, metalake, user);
  }

  protected ListUsers newListUsers(String url, boolean ignore, String metalake) {
    return new ListUsers(url, ignore, metalake);
  }

  protected UserAudit newUserAudit(String url, boolean ignore, String metalake, String user) {
    return new UserAudit(url, ignore, metalake, user);
  }

  protected CreateUser newCreateUser(String url, boolean ignore, String metalake, String user) {
    return new CreateUser(url, ignore, metalake, user);
  }

  protected DeleteUser newDeleteUser(
      String url, boolean ignore, boolean force, String metalake, String user) {
    return new DeleteUser(url, ignore, force, metalake, user);
  }

  protected RemoveRoleFromUser newRemoveRoleFromUser(
      String url, boolean ignore, String metalake, String user, String role) {
    return new RemoveRoleFromUser(url, ignore, metalake, user, role);
  }

  protected AddRoleToUser newAddRoleToUser(
      String url, boolean ignore, String metalake, String user, String role) {
    return new AddRoleToUser(url, ignore, metalake, user, role);
  }

  protected GroupDetails newGroupDetails(String url, boolean ignore, String metalake, String user) {
    return new GroupDetails(url, ignore, metalake, user);
  }

  protected ListGroups newListGroups(String url, boolean ignore, String metalake) {
    return new ListGroups(url, ignore, metalake);
  }

  protected GroupAudit newGroupAudit(String url, boolean ignore, String metalake, String group) {
    return new GroupAudit(url, ignore, metalake, group);
  }

  protected CreateGroup newCreateGroup(String url, boolean ignore, String metalake, String user) {
    return new CreateGroup(url, ignore, metalake, user);
  }

  protected DeleteGroup newDeleteGroup(
      String url, boolean ignore, boolean force, String metalake, String user) {
    return new DeleteGroup(url, ignore, force, metalake, user);
  }

  protected RemoveRoleFromGroup newRemoveRoleFromGroup(
      String url, boolean ignore, String metalake, String group, String role) {
    return new RemoveRoleFromGroup(url, ignore, metalake, group, role);
  }

  protected RemoveAllRoles newRemoveAllRoles(
      String url, boolean ignore, String metalake, String entity, String entityType) {
    return new RemoveAllRoles(url, ignore, metalake, entity, entityType);
  }

  protected AddRoleToGroup newAddRoleToGroup(
      String url, boolean ignore, String metalake, String group, String role) {
    return new AddRoleToGroup(url, ignore, metalake, group, role);
  }

  protected RoleDetails newRoleDetails(String url, boolean ignore, String metalake, String role) {
    return new RoleDetails(url, ignore, metalake, role);
  }

  protected ListRoles newListRoles(String url, boolean ignore, String metalake) {
    return new ListRoles(url, ignore, metalake);
  }

  protected RoleAudit newRoleAudit(String url, boolean ignore, String metalake, String role) {
    return new RoleAudit(url, ignore, metalake, role);
  }

  protected CreateRole newCreateRole(String url, boolean ignore, String metalake, String[] roles) {
    return new CreateRole(url, ignore, metalake, roles);
  }

  protected DeleteRole newDeleteRole(
      String url, boolean ignore, boolean force, String metalake, String[] roles) {
    return new DeleteRole(url, ignore, force, metalake, roles);
  }

  protected TagDetails newTagDetails(CommandContext context, String metalake, String tag) {
    return new TagDetails(context, metalake, tag);
  }

  protected ListAllTags newListTags(CommandContext context, String metalake) {
    return new ListAllTags(context, metalake);
  }

  protected CreateTag newCreateTags(
      CommandContext context, String metalake, String[] tags, String comment) {
    return new CreateTag(context, metalake, tags, comment);
  }

  protected DeleteTag newDeleteTag(CommandContext context, String metalake, String[] tags) {
    return new DeleteTag(context, metalake, tags);
  }

  protected SetTagProperty newSetTagProperty(
      CommandContext context, String metalake, String tag, String property, String value) {
    return new SetTagProperty(context, metalake, tag, property, value);
  }

  protected RemoveTagProperty newRemoveTagProperty(
      CommandContext context, String metalake, String tag, String property) {
    return new RemoveTagProperty(context, metalake, tag, property);
  }

  protected RemoveAllTags newRemoveAllTags(CommandContext context, String metalake, FullName name) {
    return new RemoveAllTags(context, metalake, name);
  }

  protected ListTagProperties newListTagProperties(
      CommandContext context, String metalake, String tag) {
    return new ListTagProperties(context, metalake, tag);
  }

  protected UpdateTagComment newUpdateTagComment(
      CommandContext context, String metalake, String tag, String comment) {
    return new UpdateTagComment(context, metalake, tag, comment);
  }

  protected UpdateTagName newUpdateTagName(
      CommandContext context, String metalake, String tag, String newName) {
    return new UpdateTagName(context, metalake, tag, newName);
  }

  protected ListEntityTags newListEntityTags(
      CommandContext context, String metalake, FullName name) {
    return new ListEntityTags(context, metalake, name);
  }

  protected TagEntity newTagEntity(
      CommandContext context, String metalake, FullName name, String[] tags) {
    return new TagEntity(context, metalake, name, tags);
  }

  protected UntagEntity newUntagEntity(
      CommandContext context, String metalake, FullName name, String[] tags) {
    return new UntagEntity(context, metalake, name, tags);
  }

  protected ColumnAudit newColumnAudit(
      String url,
      boolean ignore,
      String metalake,
      String catalog,
      String schema,
      String table,
      String column) {
    return new ColumnAudit(url, ignore, metalake, catalog, schema, table, column);
  }

  protected ListColumns newListColumns(
      String url, boolean ignore, String metalake, String catalog, String schema, String table) {
    return new ListColumns(url, ignore, metalake, catalog, schema, table);
  }

  protected SetOwner newSetOwner(
      String url,
      boolean ignore,
      String metalake,
      String entity,
      String entityType,
      String owner,
      boolean isGroup) {
    return new SetOwner(url, ignore, metalake, entity, entityType, owner, isGroup);
  }

  protected OwnerDetails newOwnerDetails(
      String url, boolean ignore, String metalake, String entity, String entityType) {
    return new OwnerDetails(url, ignore, metalake, entity, entityType);
  }

  protected ListTopics newListTopics(
      String url, boolean ignore, String metalake, String catalog, String schema) {
    return new ListTopics(url, ignore, metalake, catalog, schema);
  }

  protected TopicDetails newTopicDetails(
      String url, boolean ignore, String metalake, String catalog, String schema, String topic) {
    return new TopicDetails(url, ignore, metalake, catalog, schema, topic);
  }

  protected CreateTopic newCreateTopic(
      String url,
      boolean ignore,
      String metalake,
      String catalog,
      String schema,
      String topic,
      String comment) {
    return new CreateTopic(url, ignore, metalake, catalog, schema, topic, comment);
  }

  protected DeleteTopic newDeleteTopic(
      String url,
      boolean ignore,
      boolean force,
      String metalake,
      String catalog,
      String schema,
      String topic) {
    return new DeleteTopic(url, ignore, force, metalake, catalog, schema, topic);
  }

  protected UpdateTopicComment newUpdateTopicComment(
      String url,
      boolean ignore,
      String metalake,
      String catalog,
      String schema,
      String topic,
      String comment) {
    return new UpdateTopicComment(url, ignore, metalake, catalog, schema, topic, comment);
  }

  protected SetTopicProperty newSetTopicProperty(
      String url,
      boolean ignore,
      String metalake,
      String catalog,
      String schema,
      String topic,
      String property,
      String value) {
    return new SetTopicProperty(url, ignore, metalake, catalog, schema, topic, property, value);
  }

  protected RemoveTopicProperty newRemoveTopicProperty(
      String url,
      boolean ignore,
      String metalake,
      String catalog,
      String schema,
      String topic,
      String property) {
    return new RemoveTopicProperty(url, ignore, metalake, catalog, schema, topic, property);
  }

  protected ListTopicProperties newListTopicProperties(
      String url, boolean ignore, String metalake, String catalog, String schema, String topic) {
    return new ListTopicProperties(url, ignore, metalake, catalog, schema, topic);
  }

  protected FilesetDetails newFilesetDetails(
      CommandContext context, String metalake, String catalog, String schema, String fileset) {
    return new FilesetDetails(context, metalake, catalog, schema, fileset);
  }

  protected ListFilesets newListFilesets(
      CommandContext context, String metalake, String catalog, String schema) {
    return new ListFilesets(context, metalake, catalog, schema);
  }

  protected CreateFileset newCreateFileset(
      CommandContext context,
      String metalake,
      String catalog,
      String schema,
      String fileset,
      String comment,
      Map<String, String> propertyMap) {
    return new CreateFileset(context, metalake, catalog, schema, fileset, comment, propertyMap);
  }

  protected DeleteFileset newDeleteFileset(
      CommandContext context, String metalake, String catalog, String schema, String fileset) {
    return new DeleteFileset(context, metalake, catalog, schema, fileset);
  }

  protected UpdateFilesetComment newUpdateFilesetComment(
      CommandContext context,
      String metalake,
      String catalog,
      String schema,
      String fileset,
      String comment) {
    return new UpdateFilesetComment(context, metalake, catalog, schema, fileset, comment);
  }

  protected UpdateFilesetName newUpdateFilesetName(
      CommandContext context,
      String metalake,
      String catalog,
      String schema,
      String fileset,
      String rename) {
    return new UpdateFilesetName(context, metalake, catalog, schema, fileset, rename);
  }

  protected ListFilesetProperties newListFilesetProperties(
      CommandContext context, String metalake, String catalog, String schema, String fileset) {
    return new ListFilesetProperties(context, metalake, catalog, schema, fileset);
  }

  protected SetFilesetProperty newSetFilesetProperty(
      CommandContext context,
      String metalake,
      String catalog,
      String schema,
      String fileset,
      String property,
      String value) {
    return new SetFilesetProperty(context, metalake, catalog, schema, fileset, property, value);
  }

  protected RemoveFilesetProperty newRemoveFilesetProperty(
      CommandContext context,
      String metalake,
      String catalog,
      String schema,
      String fileset,
      String property) {
    return new RemoveFilesetProperty(context, metalake, catalog, schema, fileset, property);
  }

  protected AddColumn newAddColumn(
      String url,
      boolean ignore,
      String metalake,
      String catalog,
      String schema,
      String table,
      String column,
      String datatype,
      String comment,
      String position,
      boolean nullable,
      boolean autoIncrement,
      String defaultValue) {
    return new AddColumn(
        url,
        ignore,
        metalake,
        catalog,
        schema,
        table,
        column,
        datatype,
        comment,
        position,
        nullable,
        autoIncrement,
        defaultValue);
  }

  protected DeleteColumn newDeleteColumn(
      String url,
      boolean ignore,
      String metalake,
      String catalog,
      String schema,
      String table,
      String column) {
    return new DeleteColumn(url, ignore, metalake, catalog, schema, table, column);
  }

  protected UpdateColumnComment newUpdateColumnComment(
      String url,
      boolean ignore,
      String metalake,
      String catalog,
      String schema,
      String table,
      String column,
      String comment) {
    return new UpdateColumnComment(url, ignore, metalake, catalog, schema, table, column, comment);
  }

  protected UpdateColumnName newUpdateColumnName(
      String url,
      boolean ignore,
      String metalake,
      String catalog,
      String schema,
      String table,
      String column,
      String rename) {
    return new UpdateColumnName(url, ignore, metalake, catalog, schema, table, column, rename);
  }

  protected UpdateColumnDatatype newUpdateColumnDatatype(
      String url,
      boolean ignore,
      String metalake,
      String catalog,
      String schema,
      String table,
      String column,
      String datatype) {
    return new UpdateColumnDatatype(
        url, ignore, metalake, catalog, schema, table, column, datatype);
  }

  protected UpdateColumnPosition newUpdateColumnPosition(
      String url,
      boolean ignore,
      String metalake,
      String catalog,
      String schema,
      String table,
      String column,
      String position) {
    return new UpdateColumnPosition(
        url, ignore, metalake, catalog, schema, table, column, position);
  }

  protected UpdateColumnNullability newUpdateColumnNullability(
      String url,
      boolean ignore,
      String metalake,
      String catalog,
      String schema,
      String table,
      String column,
      boolean nullable) {
    return new UpdateColumnNullability(
        url, ignore, metalake, catalog, schema, table, column, nullable);
  }

  protected UpdateColumnAutoIncrement newUpdateColumnAutoIncrement(
      String url,
      boolean ignore,
      String metalake,
      String catalog,
      String schema,
      String table,
      String column,
      boolean autoIncrement) {
    return new UpdateColumnAutoIncrement(
        url, ignore, metalake, catalog, schema, table, column, autoIncrement);
  }

  protected UpdateColumnDefault newUpdateColumnDefault(
      String url,
      boolean ignore,
      String metalake,
      String catalog,
      String schema,
      String table,
      String column,
      String defaultValue,
      String dataType) {
    return new UpdateColumnDefault(
        url, ignore, metalake, catalog, schema, table, column, defaultValue, dataType);
  }

  protected CreateTable newCreateTable(
      CommandContext context,
      String metalake,
      String catalog,
      String schema,
      String table,
      String columnFile,
      String comment) {
    return new CreateTable(context, metalake, catalog, schema, table, columnFile, comment);
  }

  protected GrantPrivilegesToRole newGrantPrivilegesToRole(
      String url,
      boolean ignore,
      String metalake,
      String role,
      FullName entity,
      String[] privileges) {
    return new GrantPrivilegesToRole(url, ignore, metalake, role, entity, privileges);
  }

  protected RevokePrivilegesFromRole newRevokePrivilegesFromRole(
      String url,
      boolean ignore,
      String metalake,
      String role,
      FullName entity,
      String[] privileges) {
    return new RevokePrivilegesFromRole(url, ignore, metalake, role, entity, privileges);
  }

  protected RevokeAllPrivileges newRevokeAllPrivileges(
      String url, boolean ignore, String metalake, String role, FullName entity) {
    return new RevokeAllPrivileges(url, ignore, metalake, role, entity);
  }

  protected MetalakeEnable newMetalakeEnable(
      CommandContext context, String metalake, boolean enableAllCatalogs) {
    return new MetalakeEnable(context, metalake, enableAllCatalogs);
  }

  protected MetalakeDisable newMetalakeDisable(CommandContext context, String metalake) {
    return new MetalakeDisable(context, metalake);
  }

  protected CatalogEnable newCatalogEnable(
      String url, boolean ignore, String metalake, String catalog, boolean enableMetalake) {
    return new CatalogEnable(url, ignore, metalake, catalog, enableMetalake);
  }

  protected CatalogDisable newCatalogDisable(
      String url, boolean ignore, String metalake, String catalog) {
    return new CatalogDisable(url, ignore, metalake, catalog);
  }

  protected ListModel newListModel(
      CommandContext context, String metalake, String catalog, String schema) {
    return new ListModel(context, metalake, catalog, schema);
  }

  protected ModelAudit newModelAudit(
      CommandContext context, String metalake, String catalog, String schema, String model) {
    return new ModelAudit(context, metalake, catalog, schema, model);
  }

  protected ModelDetails newModelDetails(
      CommandContext context, String metalake, String catalog, String schema, String model) {
    return new ModelDetails(context, metalake, catalog, schema, model);
  }

  protected RegisterModel newCreateModel(
      CommandContext context,
      String metalake,
      String catalog,
      String schema,
      String model,
      String comment,
      Map<String, String> properties) {
    return new RegisterModel(context, metalake, catalog, schema, model, comment, properties);
  }

  protected DeleteModel newDeleteModel(
      CommandContext context, String metalake, String catalog, String schema, String model) {
    return new DeleteModel(context, metalake, catalog, schema, model);
  }

  protected LinkModel newLinkModel(
      CommandContext context,
      String metalake,
      String catalog,
      String schema,
      String model,
      String uri,
      String[] alias,
      String comment,
      Map<String, String> properties) {
    return new LinkModel(
        context, metalake, catalog, schema, model, uri, alias, comment, properties);
  }
}
