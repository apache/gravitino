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
import org.apache.gravitino.cli.commands.CreateGroup;
import org.apache.gravitino.cli.commands.CreateMetalake;
import org.apache.gravitino.cli.commands.CreateRole;
import org.apache.gravitino.cli.commands.CreateSchema;
import org.apache.gravitino.cli.commands.CreateTag;
import org.apache.gravitino.cli.commands.CreateUser;
import org.apache.gravitino.cli.commands.DeleteCatalog;
import org.apache.gravitino.cli.commands.DeleteGroup;
import org.apache.gravitino.cli.commands.DeleteMetalake;
import org.apache.gravitino.cli.commands.DeleteRole;
import org.apache.gravitino.cli.commands.DeleteSchema;
import org.apache.gravitino.cli.commands.DeleteTable;
import org.apache.gravitino.cli.commands.DeleteTag;
import org.apache.gravitino.cli.commands.DeleteUser;
import org.apache.gravitino.cli.commands.GroupDetails;
import org.apache.gravitino.cli.commands.ListAllTags;
import org.apache.gravitino.cli.commands.ListCatalogProperties;
import org.apache.gravitino.cli.commands.ListCatalogs;
import org.apache.gravitino.cli.commands.ListColumns;
import org.apache.gravitino.cli.commands.ListEntityTags;
import org.apache.gravitino.cli.commands.ListGroups;
import org.apache.gravitino.cli.commands.ListIndexes;
import org.apache.gravitino.cli.commands.ListMetalakeProperties;
import org.apache.gravitino.cli.commands.ListMetalakes;
import org.apache.gravitino.cli.commands.ListRoles;
import org.apache.gravitino.cli.commands.ListSchema;
import org.apache.gravitino.cli.commands.ListSchemaProperties;
import org.apache.gravitino.cli.commands.ListTables;
import org.apache.gravitino.cli.commands.ListTagProperties;
import org.apache.gravitino.cli.commands.ListUsers;
import org.apache.gravitino.cli.commands.MetalakeAudit;
import org.apache.gravitino.cli.commands.MetalakeDetails;
import org.apache.gravitino.cli.commands.RemoveCatalogProperty;
import org.apache.gravitino.cli.commands.RemoveMetalakeProperty;
import org.apache.gravitino.cli.commands.RemoveRoleFromGroup;
import org.apache.gravitino.cli.commands.RemoveRoleFromUser;
import org.apache.gravitino.cli.commands.RemoveSchemaProperty;
import org.apache.gravitino.cli.commands.RemoveTagProperty;
import org.apache.gravitino.cli.commands.RoleDetails;
import org.apache.gravitino.cli.commands.SchemaAudit;
import org.apache.gravitino.cli.commands.SchemaDetails;
import org.apache.gravitino.cli.commands.ServerVersion;
import org.apache.gravitino.cli.commands.SetCatalogProperty;
import org.apache.gravitino.cli.commands.SetMetalakeProperty;
import org.apache.gravitino.cli.commands.SetSchemaProperty;
import org.apache.gravitino.cli.commands.SetTagProperty;
import org.apache.gravitino.cli.commands.TableAudit;
import org.apache.gravitino.cli.commands.TableDetails;
import org.apache.gravitino.cli.commands.TableDistribution;
import org.apache.gravitino.cli.commands.TablePartition;
import org.apache.gravitino.cli.commands.TagDetails;
import org.apache.gravitino.cli.commands.TagEntity;
import org.apache.gravitino.cli.commands.UntagEntity;
import org.apache.gravitino.cli.commands.UpdateCatalogComment;
import org.apache.gravitino.cli.commands.UpdateCatalogName;
import org.apache.gravitino.cli.commands.UpdateMetalakeComment;
import org.apache.gravitino.cli.commands.UpdateMetalakeName;
import org.apache.gravitino.cli.commands.UpdateTagComment;
import org.apache.gravitino.cli.commands.UpdateTagName;
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

  protected MetalakeAudit newMetalakeAudit(String url, boolean ignore, String metalake) {
    return new MetalakeAudit(url, ignore, metalake);
  }

  protected MetalakeDetails newMetalakeDetails(String url, boolean ignore, String metalake) {
    return new MetalakeDetails(url, ignore, metalake);
  }

  protected ListMetalakes newListMetalakes(String url, boolean ignore) {
    return new ListMetalakes(url, ignore);
  }

  protected CreateMetalake newCreateMetalake(
      String url, boolean ignore, String metalake, String comment) {
    return new CreateMetalake(url, ignore, metalake, comment);
  }

  protected DeleteMetalake newDeleteMetalake(
      String url, boolean ignore, boolean force, String metalake) {
    return new DeleteMetalake(url, ignore, force, metalake);
  }

  protected SetMetalakeProperty newSetMetalakeProperty(
      String url, boolean ignore, String metalake, String property, String value) {
    return new SetMetalakeProperty(url, ignore, metalake, property, value);
  }

  protected RemoveMetalakeProperty newRemoveMetalakeProperty(
      String url, boolean ignore, String metalake, String property) {
    return new RemoveMetalakeProperty(url, ignore, metalake, property);
  }

  protected ListMetalakeProperties newListMetalakeProperties(
      String url, boolean ignore, String metalake) {
    return new ListMetalakeProperties(url, ignore, metalake);
  }

  protected UpdateMetalakeComment newUpdateMetalakeComment(
      String url, boolean ignore, String metalake, String comment) {
    return new UpdateMetalakeComment(url, ignore, metalake, comment);
  }

  protected UpdateMetalakeName newUpdateMetalakeName(
      String url, boolean ignore, boolean force, String metalake, String newName) {
    return new UpdateMetalakeName(url, ignore, force, metalake, newName);
  }

  protected CatalogAudit newCatalogAudit(
      String url, boolean ignore, String metalake, String catalog) {
    return new CatalogAudit(url, ignore, metalake, catalog);
  }

  protected CatalogDetails newCatalogDetails(
      String url, boolean ignore, String metalake, String catalog) {
    return new CatalogDetails(url, ignore, metalake, catalog);
  }

  protected ListCatalogs newListCatalogs(String url, boolean ignore, String metalake) {
    return new ListCatalogs(url, ignore, metalake);
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
      String url, boolean ignore, String metalake, String catalog, String schema) {
    return new SchemaAudit(url, ignore, metalake, catalog, schema);
  }

  protected SchemaDetails newSchemaDetails(
      String url, boolean ignore, String metalake, String catalog, String schema) {
    return new SchemaDetails(url, ignore, metalake, catalog, schema);
  }

  protected ListSchema newListSchema(String url, boolean ignore, String metalake, String catalog) {
    return new ListSchema(url, ignore, metalake, catalog);
  }

  protected CreateSchema newCreateSchema(
      String url, boolean ignore, String metalake, String catalog, String schema, String comment) {
    return new CreateSchema(url, ignore, metalake, catalog, schema, comment);
  }

  protected DeleteSchema newDeleteSchema(
      String url, boolean ignore, boolean force, String metalake, String catalog, String schema) {
    return new DeleteSchema(url, ignore, force, metalake, catalog, schema);
  }

  protected SetSchemaProperty newSetSchemaProperty(
      String url,
      boolean ignore,
      String metalake,
      String catalog,
      String schema,
      String property,
      String value) {
    return new SetSchemaProperty(url, ignore, metalake, catalog, schema, property, value);
  }

  protected RemoveSchemaProperty newRemoveSchemaProperty(
      String url, boolean ignore, String metalake, String catalog, String schema, String property) {
    return new RemoveSchemaProperty(url, ignore, metalake, catalog, schema, property);
  }

  protected ListSchemaProperties newListSchemaProperties(
      String url, boolean ignore, String metalake, String catalog, String schema) {
    return new ListSchemaProperties(url, ignore, metalake, catalog, schema);
  }

  protected TableAudit newTableAudit(
      String url, boolean ignore, String metalake, String catalog, String schema, String table) {
    return new TableAudit(url, ignore, metalake, catalog, schema, table);
  }

  protected TableDetails newTableDetails(
      String url, boolean ignore, String metalake, String catalog, String schema, String table) {
    return new TableDetails(url, ignore, metalake, catalog, schema, table);
  }

  protected ListTables newListTables(
      String url, boolean ignore, String metalake, String catalog, String table) {
    return new ListTables(url, ignore, metalake, catalog, table);
  }

  protected DeleteTable newDeleteTable(
      String url,
      boolean ignore,
      boolean force,
      String metalake,
      String catalog,
      String schema,
      String table) {
    return new DeleteTable(url, ignore, force, metalake, catalog, schema, table);
  }

  protected ListIndexes newListIndexes(
      String url, boolean ignore, String metalake, String catalog, String schema, String table) {
    return new ListIndexes(url, ignore, metalake, catalog, schema, table);
  }

  protected TablePartition newTablePartition(
      String url, boolean ignore, String metalake, String catalog, String schema, String table) {
    return new TablePartition(url, ignore, metalake, catalog, schema, table);
  }

  protected TableDistribution newTableDistribution(
      String url, boolean ignore, String metalake, String catalog, String schema, String table) {
    return new TableDistribution(url, ignore, metalake, catalog, schema, table);
  }

  protected UserDetails newUserDetails(String url, boolean ignore, String metalake, String user) {
    return new UserDetails(url, ignore, metalake, user);
  }

  protected ListUsers newListUsers(String url, boolean ignore, String metalake) {
    return new ListUsers(url, ignore, metalake);
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

  protected CreateGroup newCreateGroup(String url, boolean ignore, String metalake, String user) {
    return new CreateGroup(url, ignore, metalake, user);
  }

  protected DeleteGroup newDeleteGroup(
      String url, boolean ignore, boolean force, String metalake, String user) {
    return new DeleteGroup(url, ignore, force, metalake, user);
  }

  protected RemoveRoleFromGroup newRemoveRoleFromGroup(
      String url, boolean ignore, String metalake, String user, String role) {
    return new RemoveRoleFromGroup(url, ignore, metalake, user, role);
  }

  protected AddRoleToGroup newAddRoleToGroup(
      String url, boolean ignore, String metalake, String user, String role) {
    return new AddRoleToGroup(url, ignore, metalake, user, role);
  }

  protected RoleDetails newRoleDetails(String url, boolean ignore, String metalake, String role) {
    return new RoleDetails(url, ignore, metalake, role);
  }

  protected ListRoles newListRoles(String url, boolean ignore, String metalake) {
    return new ListRoles(url, ignore, metalake);
  }

  protected CreateRole newCreateRole(String url, boolean ignore, String metalake, String role) {
    return new CreateRole(url, ignore, metalake, role);
  }

  protected DeleteRole newDeleteRole(
      String url, boolean ignore, boolean force, String metalake, String role) {
    return new DeleteRole(url, ignore, force, metalake, role);
  }

  protected TagDetails newTagDetails(String url, boolean ignore, String metalake, String tag) {
    return new TagDetails(url, ignore, metalake, tag);
  }

  protected ListAllTags newListTags(String url, boolean ignore, String metalake) {
    return new ListAllTags(url, ignore, metalake);
  }

  protected CreateTag newCreateTag(
      String url, boolean ignore, String metalake, String tag, String comment) {
    return new CreateTag(url, ignore, metalake, tag, comment);
  }

  protected DeleteTag newDeleteTag(
      String url, boolean ignore, boolean force, String metalake, String tag) {
    return new DeleteTag(url, ignore, force, metalake, tag);
  }

  protected SetTagProperty newSetTagProperty(
      String url, boolean ignore, String metalake, String tag, String property, String value) {
    return new SetTagProperty(url, ignore, metalake, tag, property, value);
  }

  protected RemoveTagProperty newRemoveTagProperty(
      String url, boolean ignore, String metalake, String tag, String property) {
    return new RemoveTagProperty(url, ignore, metalake, tag, property);
  }

  protected ListTagProperties newListTagProperties(
      String url, boolean ignore, String metalake, String tag) {
    return new ListTagProperties(url, ignore, metalake, tag);
  }

  protected UpdateTagComment newUpdateTagComment(
      String url, boolean ignore, String metalake, String tag, String comment) {
    return new UpdateTagComment(url, ignore, metalake, tag, comment);
  }

  protected UpdateTagName newUpdateTagName(
      String url, boolean ignore, String metalake, String tag, String newName) {
    return new UpdateTagName(url, ignore, metalake, tag, newName);
  }

  protected ListEntityTags newListEntityTags(
      String url, boolean ignore, String metalake, FullName name) {
    return new ListEntityTags(url, ignore, metalake, name);
  }

  protected TagEntity newTagEntity(
      String url, boolean ignore, String metalake, FullName name, String tag) {
    return new TagEntity(url, ignore, metalake, name, tag);
  }

  protected UntagEntity newUntagEntity(
      String url, boolean ignore, String metalake, FullName name, String tag) {
    return new UntagEntity(url, ignore, metalake, name, tag);
  }

  protected ListColumns newListColumns(
      String url, boolean ignore, String metalake, String catalog, String schema, String table) {
    return new ListColumns(url, ignore, metalake, catalog, schema, table);
  }
}
