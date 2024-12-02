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
import org.apache.gravitino.cli.commands.AddRoleToGroup;
import org.apache.gravitino.cli.commands.AddRoleToUser;
import org.apache.gravitino.cli.commands.CatalogAudit;
import org.apache.gravitino.cli.commands.CatalogDetails;
import org.apache.gravitino.cli.commands.ClientVersion;
import org.apache.gravitino.cli.commands.CreateCatalog;
import org.apache.gravitino.cli.commands.CreateFileset;
import org.apache.gravitino.cli.commands.CreateGroup;
import org.apache.gravitino.cli.commands.CreateMetalake;
import org.apache.gravitino.cli.commands.CreateRole;
import org.apache.gravitino.cli.commands.CreateSchema;
import org.apache.gravitino.cli.commands.CreateTag;
import org.apache.gravitino.cli.commands.CreateTopic;
import org.apache.gravitino.cli.commands.CreateUser;
import org.apache.gravitino.cli.commands.DeleteCatalog;
import org.apache.gravitino.cli.commands.DeleteFileset;
import org.apache.gravitino.cli.commands.DeleteGroup;
import org.apache.gravitino.cli.commands.DeleteMetalake;
import org.apache.gravitino.cli.commands.DeleteRole;
import org.apache.gravitino.cli.commands.DeleteSchema;
import org.apache.gravitino.cli.commands.DeleteTable;
import org.apache.gravitino.cli.commands.DeleteTag;
import org.apache.gravitino.cli.commands.DeleteTopic;
import org.apache.gravitino.cli.commands.DeleteUser;
import org.apache.gravitino.cli.commands.FilesetDetails;
import org.apache.gravitino.cli.commands.GroupDetails;
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
import org.apache.gravitino.cli.commands.OwnerDetails;
import org.apache.gravitino.cli.commands.RemoveCatalogProperty;
import org.apache.gravitino.cli.commands.RemoveFilesetProperty;
import org.apache.gravitino.cli.commands.RemoveMetalakeProperty;
import org.apache.gravitino.cli.commands.RemoveRoleFromGroup;
import org.apache.gravitino.cli.commands.RemoveRoleFromUser;
import org.apache.gravitino.cli.commands.RemoveSchemaProperty;
import org.apache.gravitino.cli.commands.RemoveTableProperty;
import org.apache.gravitino.cli.commands.RemoveTagProperty;
import org.apache.gravitino.cli.commands.RemoveTopicProperty;
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
import org.apache.gravitino.cli.commands.TagDetails;
import org.apache.gravitino.cli.commands.TagEntity;
import org.apache.gravitino.cli.commands.TopicDetails;
import org.apache.gravitino.cli.commands.UntagEntity;
import org.apache.gravitino.cli.commands.UpdateCatalogComment;
import org.apache.gravitino.cli.commands.UpdateCatalogName;
import org.apache.gravitino.cli.commands.UpdateFilesetComment;
import org.apache.gravitino.cli.commands.UpdateFilesetName;
import org.apache.gravitino.cli.commands.UpdateMetalakeComment;
import org.apache.gravitino.cli.commands.UpdateMetalakeName;
import org.apache.gravitino.cli.commands.UpdateTagComment;
import org.apache.gravitino.cli.commands.UpdateTagName;
import org.apache.gravitino.cli.commands.UpdateTopicComment;
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

  protected MetalakeAudit newMetalakeAudit(
      String url, boolean ignore, String auth, String userName, String metalake) {
    return new MetalakeAudit(url, ignore, auth, userName, metalake);
  }

  protected MetalakeDetails newMetalakeDetails(
      String url,
      boolean ignore,
      String auth,
      String userName,
      String outputFormat,
      String metalake) {
    return new MetalakeDetails(url, ignore, auth, userName, outputFormat, metalake);
  }

  protected ListMetalakes newListMetalakes(
      String url, boolean ignore, String auth, String userName, String outputFormat) {
    return new ListMetalakes(url, ignore, auth, userName, outputFormat);
  }

  protected CreateMetalake newCreateMetalake(
      String url, boolean ignore, String auth, String userName, String metalake, String comment) {
    return new CreateMetalake(url, ignore, auth, userName, metalake, comment);
  }

  protected DeleteMetalake newDeleteMetalake(
      String url, boolean ignore, boolean force, String auth, String userName, String metalake) {
    return new DeleteMetalake(url, ignore, force, auth, userName, metalake);
  }

  protected SetMetalakeProperty newSetMetalakeProperty(
      String url,
      boolean ignore,
      String auth,
      String userName,
      String metalake,
      String property,
      String value) {
    return new SetMetalakeProperty(url, ignore, auth, userName, metalake, property, value);
  }

  protected RemoveMetalakeProperty newRemoveMetalakeProperty(
      String url, boolean ignore, String auth, String userName, String metalake, String property) {
    return new RemoveMetalakeProperty(url, ignore, auth, userName, metalake, property);
  }

  protected ListMetalakeProperties newListMetalakeProperties(
      String url, boolean ignore, String auth, String userName, String metalake) {
    return new ListMetalakeProperties(url, ignore, auth, userName, metalake);
  }

  protected UpdateMetalakeComment newUpdateMetalakeComment(
      String url, boolean ignore, String auth, String userName, String metalake, String comment) {
    return new UpdateMetalakeComment(url, ignore, auth, userName, metalake, comment);
  }

  protected UpdateMetalakeName newUpdateMetalakeName(
      String url,
      boolean ignore,
      boolean force,
      String auth,
      String userName,
      String metalake,
      String newName) {
    return new UpdateMetalakeName(url, ignore, force, auth, userName, metalake, newName);
  }

  protected CatalogAudit newCatalogAudit(
      String url, boolean ignore, String auth, String userName, String metalake, String catalog) {
    return new CatalogAudit(url, ignore, auth, userName, metalake, catalog);
  }

  protected CatalogDetails newCatalogDetails(
      String url,
      boolean ignore,
      String auth,
      String userName,
      String outputFormat,
      String metalake,
      String catalog) {
    return new CatalogDetails(url, ignore, auth, userName, outputFormat, metalake, catalog);
  }

  protected ListCatalogs newListCatalogs(
      String url, boolean ignore, String auth, String userName, String metalake) {
    return new ListCatalogs(url, ignore, auth, userName, metalake);
  }

  protected CreateCatalog newCreateCatalog(
      String url,
      boolean ignore,
      String auth,
      String userName,
      String metalake,
      String catalog,
      String provider,
      String comment,
      Map<String, String> properties) {
    return new CreateCatalog(
        url, ignore, auth, userName, metalake, catalog, provider, comment, properties);
  }

  protected DeleteCatalog newDeleteCatalog(
      String url,
      boolean ignore,
      boolean force,
      String auth,
      String userName,
      String metalake,
      String catalog) {
    return new DeleteCatalog(url, ignore, force, auth, userName, metalake, catalog);
  }

  protected SetCatalogProperty newSetCatalogProperty(
      String url,
      boolean ignore,
      String auth,
      String userName,
      String metalake,
      String catalog,
      String property,
      String value) {
    return new SetCatalogProperty(url, ignore, auth, userName, metalake, catalog, property, value);
  }

  protected RemoveCatalogProperty newRemoveCatalogProperty(
      String url,
      boolean ignore,
      String auth,
      String userName,
      String metalake,
      String catalog,
      String property) {
    return new RemoveCatalogProperty(url, ignore, auth, userName, metalake, catalog, property);
  }

  protected ListCatalogProperties newListCatalogProperties(
      String url, boolean ignore, String auth, String userName, String metalake, String catalog) {
    return new ListCatalogProperties(url, ignore, auth, userName, metalake, catalog);
  }

  protected UpdateCatalogComment newUpdateCatalogComment(
      String url,
      boolean ignore,
      String auth,
      String userName,
      String metalake,
      String catalog,
      String comment) {
    return new UpdateCatalogComment(url, ignore, auth, userName, metalake, catalog, comment);
  }

  protected UpdateCatalogName newUpdateCatalogName(
      String url,
      boolean ignore,
      String auth,
      String userName,
      String metalake,
      String catalog,
      String newName) {
    return new UpdateCatalogName(url, ignore, auth, userName, metalake, catalog, newName);
  }

  protected SchemaAudit newSchemaAudit(
      String url,
      boolean ignore,
      String auth,
      String userName,
      String metalake,
      String catalog,
      String schema) {
    return new SchemaAudit(url, ignore, auth, userName, metalake, catalog, schema);
  }

  protected SchemaDetails newSchemaDetails(
      String url,
      boolean ignore,
      String auth,
      String userName,
      String metalake,
      String catalog,
      String schema) {
    return new SchemaDetails(url, ignore, auth, userName, metalake, catalog, schema);
  }

  protected ListSchema newListSchema(
      String url, boolean ignore, String auth, String userName, String metalake, String catalog) {
    return new ListSchema(url, ignore, auth, userName, metalake, catalog);
  }

  protected CreateSchema newCreateSchema(
      String url,
      boolean ignore,
      String auth,
      String userName,
      String metalake,
      String catalog,
      String schema,
      String comment) {
    return new CreateSchema(url, ignore, auth, userName, metalake, catalog, schema, comment);
  }

  protected DeleteSchema newDeleteSchema(
      String url,
      boolean ignore,
      boolean force,
      String auth,
      String userName,
      String metalake,
      String catalog,
      String schema) {
    return new DeleteSchema(url, ignore, force, auth, userName, metalake, catalog, schema);
  }

  protected SetSchemaProperty newSetSchemaProperty(
      String url,
      boolean ignore,
      String auth,
      String userName,
      String metalake,
      String catalog,
      String schema,
      String property,
      String value) {
    return new SetSchemaProperty(
        url, ignore, auth, userName, metalake, catalog, schema, property, value);
  }

  protected RemoveSchemaProperty newRemoveSchemaProperty(
      String url,
      boolean ignore,
      String auth,
      String userName,
      String metalake,
      String catalog,
      String schema,
      String property) {
    return new RemoveSchemaProperty(
        url, ignore, auth, userName, metalake, catalog, schema, property);
  }

  protected ListSchemaProperties newListSchemaProperties(
      String url,
      boolean ignore,
      String auth,
      String userName,
      String metalake,
      String catalog,
      String schema) {
    return new ListSchemaProperties(url, ignore, auth, userName, metalake, catalog, schema);
  }

  protected TableAudit newTableAudit(
      String url,
      boolean ignore,
      String auth,
      String userName,
      String metalake,
      String catalog,
      String schema,
      String table) {
    return new TableAudit(url, ignore, auth, userName, metalake, catalog, schema, table);
  }

  protected TableDetails newTableDetails(
      String url,
      boolean ignore,
      String auth,
      String userName,
      String metalake,
      String catalog,
      String schema,
      String table) {
    return new TableDetails(url, ignore, auth, userName, metalake, catalog, schema, table);
  }

  protected ListTables newListTables(
      String url,
      boolean ignore,
      String auth,
      String userName,
      String metalake,
      String catalog,
      String table) {
    return new ListTables(url, ignore, auth, userName, metalake, catalog, table);
  }

  protected DeleteTable newDeleteTable(
      String url,
      boolean ignore,
      boolean force,
      String auth,
      String userName,
      String metalake,
      String catalog,
      String schema,
      String table) {
    return new DeleteTable(url, ignore, force, auth, userName, metalake, catalog, schema, table);
  }

  protected ListIndexes newListIndexes(
      String url,
      boolean ignore,
      String auth,
      String userName,
      String metalake,
      String catalog,
      String schema,
      String table) {
    return new ListIndexes(url, ignore, auth, userName, metalake, catalog, schema, table);
  }

  protected TablePartition newTablePartition(
      String url,
      boolean ignore,
      String auth,
      String userName,
      String metalake,
      String catalog,
      String schema,
      String table) {
    return new TablePartition(url, ignore, auth, userName, metalake, catalog, schema, table);
  }

  protected TableDistribution newTableDistribution(
      String url,
      boolean ignore,
      String auth,
      String userName,
      String metalake,
      String catalog,
      String schema,
      String table) {
    return new TableDistribution(url, ignore, auth, userName, metalake, catalog, schema, table);
  }

  protected SetTableProperty newSetTableProperty(
      String url,
      boolean ignore,
      String auth,
      String userName,
      String metalake,
      String catalog,
      String schema,
      String table,
      String property,
      String value) {
    return new SetTableProperty(
        url, ignore, auth, userName, metalake, catalog, schema, table, property, value);
  }

  protected RemoveTableProperty newRemoveTableProperty(
      String url,
      boolean ignore,
      String auth,
      String userName,
      String metalake,
      String catalog,
      String schema,
      String table,
      String property) {
    return new RemoveTableProperty(
        url, ignore, auth, userName, metalake, catalog, schema, table, property);
  }

  protected ListTableProperties newListTableProperties(
      String url,
      boolean ignore,
      String auth,
      String userName,
      String metalake,
      String catalog,
      String schema,
      String table) {
    return new ListTableProperties(url, ignore, auth, userName, metalake, catalog, schema, table);
  }

  protected UserDetails newUserDetails(
      String url, boolean ignore, String auth, String userName, String metalake, String user) {
    return new UserDetails(url, ignore, auth, userName, metalake, user);
  }

  protected ListUsers newListUsers(
      String url, boolean ignore, String auth, String userName, String metalake) {
    return new ListUsers(url, ignore, auth, userName, metalake);
  }

  protected CreateUser newCreateUser(
      String url, boolean ignore, String auth, String userName, String metalake, String user) {
    return new CreateUser(url, ignore, auth, userName, metalake, user);
  }

  protected DeleteUser newDeleteUser(
      String url,
      boolean ignore,
      boolean force,
      String auth,
      String userName,
      String metalake,
      String user) {
    return new DeleteUser(url, ignore, force, auth, userName, metalake, user);
  }

  protected RemoveRoleFromUser newRemoveRoleFromUser(
      String url,
      boolean ignore,
      String auth,
      String userName,
      String metalake,
      String user,
      String role) {
    return new RemoveRoleFromUser(url, ignore, auth, userName, metalake, user, role);
  }

  protected AddRoleToUser newAddRoleToUser(
      String url,
      boolean ignore,
      String auth,
      String userName,
      String metalake,
      String user,
      String role) {
    return new AddRoleToUser(url, ignore, auth, userName, metalake, user, role);
  }

  protected GroupDetails newGroupDetails(
      String url, boolean ignore, String auth, String userName, String metalake, String user) {
    return new GroupDetails(url, ignore, auth, userName, metalake, user);
  }

  protected ListGroups newListGroups(
      String url, boolean ignore, String auth, String userName, String metalake) {
    return new ListGroups(url, ignore, auth, userName, metalake);
  }

  protected CreateGroup newCreateGroup(
      String url, boolean ignore, String auth, String userName, String metalake, String user) {
    return new CreateGroup(url, ignore, auth, userName, metalake, user);
  }

  protected DeleteGroup newDeleteGroup(
      String url,
      boolean ignore,
      boolean force,
      String auth,
      String userName,
      String metalake,
      String user) {
    return new DeleteGroup(url, ignore, force, auth, userName, metalake, user);
  }

  protected RemoveRoleFromGroup newRemoveRoleFromGroup(
      String url,
      boolean ignore,
      String auth,
      String userName,
      String metalake,
      String user,
      String role) {
    return new RemoveRoleFromGroup(url, ignore, auth, userName, metalake, user, role);
  }

  protected AddRoleToGroup newAddRoleToGroup(
      String url,
      boolean ignore,
      String auth,
      String userName,
      String metalake,
      String user,
      String role) {
    return new AddRoleToGroup(url, ignore, auth, userName, metalake, user, role);
  }

  protected RoleDetails newRoleDetails(
      String url, boolean ignore, String auth, String userName, String metalake, String role) {
    return new RoleDetails(url, ignore, auth, userName, metalake, role);
  }

  protected ListRoles newListRoles(
      String url, boolean ignore, String auth, String userName, String metalake) {
    return new ListRoles(url, ignore, auth, userName, metalake);
  }

  protected CreateRole newCreateRole(
      String url, boolean ignore, String auth, String userName, String metalake, String role) {
    return new CreateRole(url, ignore, auth, userName, metalake, role);
  }

  protected DeleteRole newDeleteRole(
      String url,
      boolean ignore,
      boolean force,
      String auth,
      String userName,
      String metalake,
      String role) {
    return new DeleteRole(url, ignore, force, auth, userName, metalake, role);
  }

  protected TagDetails newTagDetails(
      String url, boolean ignore, String auth, String userName, String metalake, String tag) {
    return new TagDetails(url, ignore, auth, userName, metalake, tag);
  }

  protected ListAllTags newListTags(
      String url, boolean ignore, String auth, String userName, String metalake) {
    return new ListAllTags(url, ignore, auth, userName, metalake);
  }

  protected CreateTag newCreateTags(
      String url,
      boolean ignore,
      String auth,
      String userName,
      String metalake,
      String[] tags,
      String comment) {
    return new CreateTag(url, ignore, auth, userName, metalake, tags, comment);
  }

  protected DeleteTag newDeleteTag(
      String url,
      boolean ignore,
      boolean force,
      String auth,
      String userName,
      String metalake,
      String[] tags) {
    return new DeleteTag(url, ignore, force, auth, userName, metalake, tags);
  }

  protected SetTagProperty newSetTagProperty(
      String url,
      boolean ignore,
      String auth,
      String userName,
      String metalake,
      String tag,
      String property,
      String value) {
    return new SetTagProperty(url, ignore, auth, userName, metalake, tag, property, value);
  }

  protected RemoveTagProperty newRemoveTagProperty(
      String url,
      boolean ignore,
      String auth,
      String userName,
      String metalake,
      String tag,
      String property) {
    return new RemoveTagProperty(url, ignore, auth, userName, metalake, tag, property);
  }

  protected ListTagProperties newListTagProperties(
      String url, boolean ignore, String auth, String userName, String metalake, String tag) {
    return new ListTagProperties(url, ignore, auth, userName, metalake, tag);
  }

  protected UpdateTagComment newUpdateTagComment(
      String url,
      boolean ignore,
      String auth,
      String userName,
      String metalake,
      String tag,
      String comment) {
    return new UpdateTagComment(url, ignore, auth, userName, metalake, tag, comment);
  }

  protected UpdateTagName newUpdateTagName(
      String url,
      boolean ignore,
      String auth,
      String userName,
      String metalake,
      String tag,
      String newName) {
    return new UpdateTagName(url, ignore, auth, userName, metalake, tag, newName);
  }

  protected ListEntityTags newListEntityTags(
      String url, boolean ignore, String auth, String userName, String metalake, FullName name) {
    return new ListEntityTags(url, ignore, auth, userName, metalake, name);
  }

  protected TagEntity newTagEntity(
      String url,
      boolean ignore,
      String auth,
      String userName,
      String metalake,
      FullName name,
      String[] tags) {
    return new TagEntity(url, ignore, auth, userName, metalake, name, tags);
  }

  protected UntagEntity newUntagEntity(
      String url,
      boolean ignore,
      String auth,
      String userName,
      String metalake,
      FullName name,
      String[] tags) {
    return new UntagEntity(url, ignore, auth, userName, metalake, name, tags);
  }

  protected ListColumns newListColumns(
      String url,
      boolean ignore,
      String auth,
      String userName,
      String metalake,
      String catalog,
      String schema,
      String table) {
    return new ListColumns(url, ignore, auth, userName, metalake, catalog, schema, table);
  }

  protected SetOwner newSetOwner(
      String url,
      boolean ignore,
      String auth,
      String userName,
      String metalake,
      String entity,
      String entityType,
      String owner,
      boolean isGroup) {
    return new SetOwner(url, ignore, auth, userName, metalake, entity, entityType, owner, isGroup);
  }

  protected OwnerDetails newOwnerDetails(
      String url,
      boolean ignore,
      String auth,
      String userName,
      String metalake,
      String entity,
      String entityType) {
    return new OwnerDetails(url, ignore, auth, userName, metalake, entity, entityType);
  }

  protected ListTopics newListTopics(
      String url,
      boolean ignore,
      String auth,
      String userName,
      String metalake,
      String catalog,
      String schema) {
    return new ListTopics(url, ignore, auth, userName, metalake, catalog, schema);
  }

  protected TopicDetails newTopicDetails(
      String url,
      boolean ignore,
      String auth,
      String userName,
      String metalake,
      String catalog,
      String schema,
      String topic) {
    return new TopicDetails(url, ignore, auth, userName, metalake, catalog, schema, topic);
  }

  protected CreateTopic newCreateTopic(
      String url,
      boolean ignore,
      String auth,
      String userName,
      String metalake,
      String catalog,
      String schema,
      String topic,
      String comment) {
    return new CreateTopic(url, ignore, auth, userName, metalake, catalog, schema, topic, comment);
  }

  protected DeleteTopic newDeleteTopic(
      String url,
      boolean ignore,
      boolean force,
      String auth,
      String userName,
      String metalake,
      String catalog,
      String schema,
      String topic) {
    return new DeleteTopic(url, ignore, force, auth, userName, metalake, catalog, schema, topic);
  }

  protected UpdateTopicComment newUpdateTopicComment(
      String url,
      boolean ignore,
      String auth,
      String userName,
      String metalake,
      String catalog,
      String schema,
      String topic,
      String comment) {
    return new UpdateTopicComment(
        url, ignore, auth, userName, metalake, catalog, schema, topic, comment);
  }

  protected SetTopicProperty newSetTopicProperty(
      String url,
      boolean ignore,
      String auth,
      String userName,
      String metalake,
      String catalog,
      String schema,
      String topic,
      String property,
      String value) {
    return new SetTopicProperty(
        url, ignore, auth, userName, metalake, catalog, schema, topic, property, value);
  }

  protected RemoveTopicProperty newRemoveTopicProperty(
      String url,
      boolean ignore,
      String auth,
      String userName,
      String metalake,
      String catalog,
      String schema,
      String topic,
      String property) {
    return new RemoveTopicProperty(
        url, ignore, auth, userName, metalake, catalog, schema, topic, property);
  }

  protected ListTopicProperties newListTopicProperties(
      String url,
      boolean ignore,
      String auth,
      String userName,
      String metalake,
      String catalog,
      String schema,
      String topic) {
    return new ListTopicProperties(url, ignore, auth, userName, metalake, catalog, schema, topic);
  }

  protected FilesetDetails newFilesetDetails(
      String url,
      boolean ignore,
      String auth,
      String userName,
      String metalake,
      String catalog,
      String schema,
      String fileset) {
    return new FilesetDetails(url, ignore, auth, userName, metalake, catalog, schema, fileset);
  }

  protected ListFilesets newListFilesets(
      String url,
      boolean ignore,
      String auth,
      String userName,
      String metalake,
      String catalog,
      String schema) {
    return new ListFilesets(url, ignore, auth, userName, metalake, catalog, schema);
  }

  protected CreateFileset newCreateFileset(
      String url,
      boolean ignore,
      String auth,
      String userName,
      String metalake,
      String catalog,
      String schema,
      String fileset,
      String comment,
      Map<String, String> propertyMap) {
    return new CreateFileset(
        url, ignore, auth, userName, metalake, catalog, schema, fileset, comment, propertyMap);
  }

  protected DeleteFileset newDeleteFileset(
      String url,
      boolean ignore,
      boolean force,
      String auth,
      String userName,
      String metalake,
      String catalog,
      String schema,
      String fileset) {
    return new DeleteFileset(
        url, ignore, force, auth, userName, metalake, catalog, schema, fileset);
  }

  protected UpdateFilesetComment newUpdateFilesetComment(
      String url,
      boolean ignore,
      String auth,
      String userName,
      String metalake,
      String catalog,
      String schema,
      String fileset,
      String comment) {
    return new UpdateFilesetComment(
        url, ignore, auth, userName, metalake, catalog, schema, fileset, comment);
  }

  protected UpdateFilesetName newUpdateFilesetName(
      String url,
      boolean ignore,
      String auth,
      String userName,
      String metalake,
      String catalog,
      String schema,
      String fileset,
      String rename) {
    return new UpdateFilesetName(
        url, ignore, auth, userName, metalake, catalog, schema, fileset, rename);
  }

  protected ListFilesetProperties newListFilesetProperties(
      String url,
      boolean ignore,
      String auth,
      String userName,
      String metalake,
      String catalog,
      String schema,
      String fileset) {
    return new ListFilesetProperties(
        url, ignore, auth, userName, metalake, catalog, schema, fileset);
  }

  protected SetFilesetProperty newSetFilesetProperty(
      String url,
      boolean ignore,
      String auth,
      String userName,
      String metalake,
      String catalog,
      String schema,
      String fileset,
      String property,
      String value) {
    return new SetFilesetProperty(
        url, ignore, auth, userName, metalake, catalog, schema, fileset, property, value);
  }

  protected RemoveFilesetProperty newRemoveFilesetProperty(
      String url,
      boolean ignore,
      String auth,
      String userName,
      String metalake,
      String catalog,
      String schema,
      String fileset,
      String property) {
    return new RemoveFilesetProperty(
        url, ignore, auth, userName, metalake, catalog, schema, fileset, property);
  }
}
