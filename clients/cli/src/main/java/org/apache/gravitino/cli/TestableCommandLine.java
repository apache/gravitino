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
import org.apache.gravitino.cli.commands.ManageCatalog;
import org.apache.gravitino.cli.commands.ManageMetalake;
import org.apache.gravitino.cli.commands.MetalakeAudit;
import org.apache.gravitino.cli.commands.MetalakeDetails;
import org.apache.gravitino.cli.commands.ModelAudit;
import org.apache.gravitino.cli.commands.ModelDetails;
import org.apache.gravitino.cli.commands.OwnerDetails;
import org.apache.gravitino.cli.commands.RegisterModel;
import org.apache.gravitino.cli.commands.RemoveAllRoles;
import org.apache.gravitino.cli.commands.RemoveAllTags;
import org.apache.gravitino.cli.commands.RemoveCatalogProperty;
import org.apache.gravitino.cli.commands.RemoveFilesetProperty;
import org.apache.gravitino.cli.commands.RemoveMetalakeProperty;
import org.apache.gravitino.cli.commands.RemoveModelProperty;
import org.apache.gravitino.cli.commands.RemoveModelVersionProperty;
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
import org.apache.gravitino.cli.commands.SetModelProperty;
import org.apache.gravitino.cli.commands.SetModelVersionProperty;
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
import org.apache.gravitino.cli.commands.UpdateModelComment;
import org.apache.gravitino.cli.commands.UpdateModelName;
import org.apache.gravitino.cli.commands.UpdateModelVersionAliases;
import org.apache.gravitino.cli.commands.UpdateModelVersionComment;
import org.apache.gravitino.cli.commands.UpdateModelVersionUri;
import org.apache.gravitino.cli.commands.UpdateTableComment;
import org.apache.gravitino.cli.commands.UpdateTableName;
import org.apache.gravitino.cli.commands.UpdateTagComment;
import org.apache.gravitino.cli.commands.UpdateTagName;
import org.apache.gravitino.cli.commands.UpdateTopicComment;
import org.apache.gravitino.cli.commands.UserAudit;
import org.apache.gravitino.cli.commands.UserDetails;

/**
 * Methods used for testing
 *
 * <p>Basically a seam where we can see what is passed and use mocks to see if the correct command
 * is created.
 */
public class TestableCommandLine {

  /**
   * Get a new instance of AddColumn command.
   *
   * @param context the command context
   * @return a new instance of AddColumn command
   */
  protected ClientVersion newClientVersion(CommandContext context) {
    return new ClientVersion(context);
  }

  /**
   * Get a new instance of ServerVersion command.
   *
   * @param context The command context
   * @return a new instance of ServerVersion command
   */
  protected ServerVersion newServerVersion(CommandContext context) {
    return new ServerVersion(context);
  }

  /**
   * Get a new instance of Metalake audit command.
   *
   * @param context The command context
   * @param metalake The name of the metalake
   * @return a new instance of Metalake audit command
   */
  protected MetalakeAudit newMetalakeAudit(CommandContext context, String metalake) {
    return new MetalakeAudit(context, metalake);
  }

  /**
   * Get a new instance of Metalake details command.
   *
   * @param context The command context
   * @param metalake The name of the metalake
   * @return a new instance of Metalake details command
   */
  protected MetalakeDetails newMetalakeDetails(CommandContext context, String metalake) {
    return new MetalakeDetails(context, metalake);
  }

  /**
   * Get a new instance of Metalake list command.
   *
   * @param context The command context
   * @return a new instance of Metalake list command
   */
  protected ListMetalakes newListMetalakes(CommandContext context) {
    return new ListMetalakes(context);
  }

  /**
   * Get a new instance of Metalake create command.
   *
   * @param context The command context
   * @param metalake The name of the metalake
   * @param comment The comment for the metalake
   * @return a new instance of Metalake create command
   */
  protected CreateMetalake newCreateMetalake(
      CommandContext context, String metalake, String comment) {
    return new CreateMetalake(context, metalake, comment);
  }

  /**
   * Get a new instance of Metalake delete command.
   *
   * @param context The command context
   * @param metalake The name of the metalake
   * @return a new instance of Metalake delete command
   */
  protected DeleteMetalake newDeleteMetalake(CommandContext context, String metalake) {
    return new DeleteMetalake(context, metalake);
  }

  /**
   * Get a new instance of Metalake set property command.
   *
   * @param context The command context
   * @param metalake The name of the metalake
   * @param property The property to set
   * @param value The value to set
   * @return a new instance of Metalake set property command
   */
  protected SetMetalakeProperty newSetMetalakeProperty(
      CommandContext context, String metalake, String property, String value) {
    return new SetMetalakeProperty(context, metalake, property, value);
  }

  /**
   * Get a new instance of Metalake remove property command.
   *
   * @param context The command context
   * @param metalake The name of the metalake
   * @param property The property to remove
   * @return a new instance of Metalake remove property command
   */
  protected RemoveMetalakeProperty newRemoveMetalakeProperty(
      CommandContext context, String metalake, String property) {
    return new RemoveMetalakeProperty(context, metalake, property);
  }

  /**
   * Get a new instance of Metalake list properties command.
   *
   * @param context The command context
   * @param metalake The name of the metalake
   * @return a new instance of Metalake list properties command
   */
  protected ListMetalakeProperties newListMetalakeProperties(
      CommandContext context, String metalake) {
    return new ListMetalakeProperties(context, metalake);
  }

  /**
   * Get a new instance of Metalake update comment command.
   *
   * @param context The command context
   * @param metalake The name of the metalake
   * @param comment The new comment for the metalake
   * @return a new instance of Metalake update comment command
   */
  protected UpdateMetalakeComment newUpdateMetalakeComment(
      CommandContext context, String metalake, String comment) {
    return new UpdateMetalakeComment(context, metalake, comment);
  }

  /**
   * Get a new instance of Metalake update name command.
   *
   * @param context The command context
   * @param metalake The name of the metalake
   * @param newName The new name for the metalake
   * @return a new instance of Metalake update name command
   */
  protected UpdateMetalakeName newUpdateMetalakeName(
      CommandContext context, String metalake, String newName) {
    return new UpdateMetalakeName(context, metalake, newName);
  }

  /**
   * Get a new instance of Metalake audit command.
   *
   * @param context The command context
   * @param metalake The name of the metalake
   * @param catalog The name of the catalog
   * @return a new instance of Metalake audit command
   */
  protected CatalogAudit newCatalogAudit(CommandContext context, String metalake, String catalog) {
    return new CatalogAudit(context, metalake, catalog);
  }

  /**
   * GEt a new instance of Metalake details command.
   *
   * @param context The command context
   * @param metalake The name of the metalake
   * @param catalog The name of the catalog
   * @return a new instance of Metalake details command
   */
  protected CatalogDetails newCatalogDetails(
      CommandContext context, String metalake, String catalog) {
    return new CatalogDetails(context, metalake, catalog);
  }

  /**
   * Get a new instance of Metalake list command.
   *
   * @param context The command context
   * @param metalake The name of the metalake
   * @return a new instance of Metalake list command
   */
  protected ListCatalogs newListCatalogs(CommandContext context, String metalake) {
    return new ListCatalogs(context, metalake);
  }

  /**
   * Get a new instance of Metalake create command.
   *
   * @param context The command context
   * @param metalake The name of the metalake
   * @param catalog The name of the catalog
   * @param provider The provider for the catalog
   * @param comment The comment for the catalog
   * @param properties The properties for the catalog
   * @return a new instance of Metalake create command
   */
  protected CreateCatalog newCreateCatalog(
      CommandContext context,
      String metalake,
      String catalog,
      String provider,
      String comment,
      Map<String, String> properties) {
    return new CreateCatalog(context, metalake, catalog, provider, comment, properties);
  }

  /**
   * Get a new instance of Metalake delete command.
   *
   * @param context The command context
   * @param metalake The name of the metalake
   * @param catalog The name of the catalog
   * @return a new instance of Metalake delete command
   */
  protected DeleteCatalog newDeleteCatalog(
      CommandContext context, String metalake, String catalog) {
    return new DeleteCatalog(context, metalake, catalog);
  }

  /**
   * Get a new instance of Metalake set property command.
   *
   * @param context The command context
   * @param metalake The name of the metalake
   * @param catalog The name of the catalog
   * @param property The property to set
   * @param value The value to set
   * @return a new instance of Metalake set property command
   */
  protected SetCatalogProperty newSetCatalogProperty(
      CommandContext context, String metalake, String catalog, String property, String value) {
    return new SetCatalogProperty(context, metalake, catalog, property, value);
  }

  /**
   * Get a new instance of Metalake remove property command.
   *
   * @param context The command context
   * @param metalake The name of the metalake
   * @param catalog The name of the catalog
   * @param property The property to remove
   * @return a new instance of Metalake remove property command
   */
  protected RemoveCatalogProperty newRemoveCatalogProperty(
      CommandContext context, String metalake, String catalog, String property) {
    return new RemoveCatalogProperty(context, metalake, catalog, property);
  }

  /**
   * Get a new instance of catalog list properties command.
   *
   * @param context The command context
   * @param metalake The name of the metalake
   * @param catalog The name of the catalog
   * @return a new instance of catalog list properties command
   */
  protected ListCatalogProperties newListCatalogProperties(
      CommandContext context, String metalake, String catalog) {
    return new ListCatalogProperties(context, metalake, catalog);
  }

  /**
   * Get a new instance of Catalog update comment command.
   *
   * @param context The command context
   * @param metalake The name of the metalake
   * @param catalog The name of the catalog
   * @param comment The new comment for the catalog
   * @return a new instance of catalog update comment command
   */
  protected UpdateCatalogComment newUpdateCatalogComment(
      CommandContext context, String metalake, String catalog, String comment) {
    return new UpdateCatalogComment(context, metalake, catalog, comment);
  }

  /**
   * Get a new instance of Catalog update name command.
   *
   * @param context The command context
   * @param metalake The name of the metalake
   * @param catalog The name of the catalog
   * @param newName The new name for the catalog
   * @return a new instance of catalog update name command
   */
  protected UpdateCatalogName newUpdateCatalogName(
      CommandContext context, String metalake, String catalog, String newName) {
    return new UpdateCatalogName(context, metalake, catalog, newName);
  }

  /**
   * Get a new instance of schema audit command.
   *
   * @param context The command context
   * @param metalake The name of the metalake
   * @param catalog The name of the catalog
   * @param schema The name of the schema
   * @return a new instance of schema audit command
   */
  protected SchemaAudit newSchemaAudit(
      CommandContext context, String metalake, String catalog, String schema) {
    return new SchemaAudit(context, metalake, catalog, schema);
  }

  /**
   * Get a new instance of schema details command.
   *
   * @param context The command context
   * @param metalake The name of the metalake
   * @param catalog The name of the catalog
   * @param schema The name of the schema
   * @return a new instance of schema details command
   */
  protected SchemaDetails newSchemaDetails(
      CommandContext context, String metalake, String catalog, String schema) {
    return new SchemaDetails(context, metalake, catalog, schema);
  }

  /**
   * Get a new instance of schema list command.
   *
   * @param context The command context
   * @param metalake The name of the metalake
   * @param catalog The name of the catalog
   * @return a new instance of schema list command
   */
  protected ListSchema newListSchema(CommandContext context, String metalake, String catalog) {
    return new ListSchema(context, metalake, catalog);
  }

  /**
   * Get a new instance of schema create command.
   *
   * @param context The command context
   * @param metalake The name of the metalake
   * @param catalog The name of the catalog
   * @param schema The name of the schema
   * @param comment The comment for the schema
   * @return a new instance of schema create command
   */
  protected CreateSchema newCreateSchema(
      CommandContext context, String metalake, String catalog, String schema, String comment) {
    return new CreateSchema(context, metalake, catalog, schema, comment);
  }

  /**
   * Get a new instance of schema delete command.
   *
   * @param context The command context
   * @param metalake The name of the metalake
   * @param catalog The name of the catalog
   * @param schema The name of the schema
   * @return a new instance of schema delete command
   */
  protected DeleteSchema newDeleteSchema(
      CommandContext context, String metalake, String catalog, String schema) {
    return new DeleteSchema(context, metalake, catalog, schema);
  }

  /**
   * Get a new instance of schema set property command.
   *
   * @param context The command context
   * @param metalake The name of the metalake
   * @param catalog The name of the catalog
   * @param schema The name of the schema
   * @param property The property to set
   * @param value The value to set
   * @return a new instance of schema set property command
   */
  protected SetSchemaProperty newSetSchemaProperty(
      CommandContext context,
      String metalake,
      String catalog,
      String schema,
      String property,
      String value) {
    return new SetSchemaProperty(context, metalake, catalog, schema, property, value);
  }

  /**
   * Get a new instance of schema remove property command.
   *
   * @param context The command context
   * @param metalake The name of the metalake
   * @param catalog The name of the catalog
   * @param schema The name of the schema
   * @param property The property to remove
   * @return a new instance of schema remove property command
   */
  protected RemoveSchemaProperty newRemoveSchemaProperty(
      CommandContext context, String metalake, String catalog, String schema, String property) {
    return new RemoveSchemaProperty(context, metalake, catalog, schema, property);
  }

  /**
   * Get a new instance of schema list properties command.
   *
   * @param context The command context
   * @param metalake The name of the metalake
   * @param catalog The name of the catalog
   * @param schema The name of the schema
   * @return a new instance of schema list properties command
   */
  protected ListSchemaProperties newListSchemaProperties(
      CommandContext context, String metalake, String catalog, String schema) {
    return new ListSchemaProperties(context, metalake, catalog, schema);
  }

  /**
   * Get a new instance of schema update comment command.
   *
   * @param context The command context
   * @param metalake The name of the metalake
   * @param catalog The name of the catalog
   * @param schema The name of the schema
   * @param table The name of the table
   * @return a new instance of schema update comment command
   */
  protected TableAudit newTableAudit(
      CommandContext context, String metalake, String catalog, String schema, String table) {
    return new TableAudit(context, metalake, catalog, schema, table);
  }

  /**
   * Get a new instance of table details command.
   *
   * @param context The command context
   * @param metalake The name of the metalake
   * @param catalog The name of the catalog
   * @param schema The name of the schema
   * @param table The name of the table
   * @return a new instance of table details command
   */
  protected TableDetails newTableDetails(
      CommandContext context, String metalake, String catalog, String schema, String table) {
    return new TableDetails(context, metalake, catalog, schema, table);
  }
  /**
   * Get a new instance of table delete command.
   *
   * @param context The command context
   * @param metalake The name of the metalake
   * @param catalog The name of the catalog
   * @param schema The name of the schema
   * @param table The name of the table
   * @return a new instance of table delete command
   */
  protected DeleteTable newDeleteTable(
      CommandContext context, String metalake, String catalog, String schema, String table) {
    return new DeleteTable(context, metalake, catalog, schema, table);
  }

  /**
   * Get a new instance of table index list command.
   *
   * @param context The command context
   * @param metalake The name of the metalake
   * @param catalog The name of the catalog
   * @param schema The name of the schema
   * @param table The name of the table
   * @return a new instance of table index list command
   */
  protected ListIndexes newListIndexes(
      CommandContext context, String metalake, String catalog, String schema, String table) {
    return new ListIndexes(context, metalake, catalog, schema, table);
  }

  /**
   * Get a new instance of table partition command.
   *
   * @param context The command context
   * @param metalake The name of the metalake
   * @param catalog The name of the catalog
   * @param schema The name of the schema
   * @param table The name of the table
   * @return a new instance of table partition command
   */
  protected TablePartition newTablePartition(
      CommandContext context, String metalake, String catalog, String schema, String table) {
    return new TablePartition(context, metalake, catalog, schema, table);
  }

  /**
   * Get a new instance of table distribution command.
   *
   * @param context The command context
   * @param metalake The name of the metalake
   * @param catalog The name of the catalog
   * @param schema The name of the schema
   * @param table The name of the table
   * @return a new instance of table distribution command
   */
  protected TableDistribution newTableDistribution(
      CommandContext context, String metalake, String catalog, String schema, String table) {
    return new TableDistribution(context, metalake, catalog, schema, table);
  }

  /**
   * Get a new instance of table sort order command.
   *
   * @param context The command context
   * @param metalake The name of the metalake
   * @param catalog The name of the catalog
   * @param schema The name of the schema
   * @param table The name of the table
   * @return a new instance of table sort order command
   */
  protected TableSortOrder newTableSortOrder(
      CommandContext context, String metalake, String catalog, String schema, String table) {
    return new TableSortOrder(context, metalake, catalog, schema, table);
  }

  /**
   * Get a new instance of update table comment command.
   *
   * @param context The command context
   * @param metalake The name of the metalake
   * @param catalog The name of the catalog
   * @param schema The name of the schema
   * @param table The name of the table
   * @param comment The new comment for the table
   * @return a new instance of update table comment command
   */
  protected UpdateTableComment newUpdateTableComment(
      CommandContext context,
      String metalake,
      String catalog,
      String schema,
      String table,
      String comment) {
    return new UpdateTableComment(context, metalake, catalog, schema, table, comment);
  }

  /**
   * Get a new instance of update table name command.
   *
   * @param context The command context
   * @param metalake The name of the metalake
   * @param catalog The name of the catalog
   * @param schema The name of the schema
   * @param table The current name of the table
   * @param rename The new name of the table
   * @return a new instance of update table name command
   */
  protected UpdateTableName newUpdateTableName(
      CommandContext context,
      String metalake,
      String catalog,
      String schema,
      String table,
      String rename) {
    return new UpdateTableName(context, metalake, catalog, schema, table, rename);
  }

  /**
   * Get a new instance of set table property command.
   *
   * @param context The command context
   * @param metalake The name of the metalake
   * @param catalog The name of the catalog
   * @param schema The name of the schema
   * @param table The name of the table
   * @param property The property key
   * @param value The property value
   * @return a new instance of set table property command
   */
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

  /**
   * Get a new instance of remove table property command.
   *
   * @param context The command context
   * @param metalake The name of the metalake
   * @param catalog The name of the catalog
   * @param schema The name of the schema
   * @param table The name of the table
   * @param property The property key to remove
   * @return a new instance of remove table property command
   */
  protected RemoveTableProperty newRemoveTableProperty(
      CommandContext context,
      String metalake,
      String catalog,
      String schema,
      String table,
      String property) {
    return new RemoveTableProperty(context, metalake, catalog, schema, table, property);
  }

  /**
   * Get a new instance of list table properties command.
   *
   * @param context The command context
   * @param metalake The name of the metalake
   * @param catalog The name of the catalog
   * @param schema The name of the schema
   * @param table The name of the table
   * @return a new instance of list table properties command
   */
  protected ListTableProperties newListTableProperties(
      CommandContext context, String metalake, String catalog, String schema, String table) {
    return new ListTableProperties(context, metalake, catalog, schema, table);
  }

  /**
   * Get a new instance of user details command.
   *
   * @param context The command context
   * @param metalake The name of the metalake
   * @param user The user name
   * @return a new instance of user details command
   */
  protected UserDetails newUserDetails(CommandContext context, String metalake, String user) {
    return new UserDetails(context, metalake, user);
  }

  /**
   * Get a new instance of list users command.
   *
   * @param context The command context
   * @param metalake The name of the metalake
   * @return a new instance of list users command
   */
  protected ListUsers newListUsers(CommandContext context, String metalake) {
    return new ListUsers(context, metalake);
  }

  /**
   * Get a new instance of user audit command.
   *
   * @param context The command context
   * @param metalake The name of the metalake
   * @param user The user name
   * @return a new instance of user audit command
   */
  protected UserAudit newUserAudit(CommandContext context, String metalake, String user) {
    return new UserAudit(context, metalake, user);
  }

  /**
   * Get a new instance of create user command.
   *
   * @param context The command context
   * @param metalake The name of the metalake
   * @param user The user name
   * @return a new instance of create user command
   */
  protected CreateUser newCreateUser(CommandContext context, String metalake, String user) {
    return new CreateUser(context, metalake, user);
  }

  /**
   * Get a new instance of delete user command.
   *
   * @param context The command context
   * @param metalake The name of the metalake
   * @param user The user name
   * @return a new instance of delete user command
   */
  protected DeleteUser newDeleteUser(CommandContext context, String metalake, String user) {
    return new DeleteUser(context, metalake, user);
  }

  /**
   * Get a new instance of remove role from user command.
   *
   * @param context The command context
   * @param metalake The name of the metalake
   * @param user The user name
   * @param role The role name
   * @return a new instance of remove role from user command
   */
  protected RemoveRoleFromUser newRemoveRoleFromUser(
      CommandContext context, String metalake, String user, String role) {
    return new RemoveRoleFromUser(context, metalake, user, role);
  }

  /**
   * Get a new instance of add role to user command.
   *
   * @param context The command context
   * @param metalake The name of the metalake
   * @param user The user name
   * @param role The role name
   * @return a new instance of add role to user command
   */
  protected AddRoleToUser newAddRoleToUser(
      CommandContext context, String metalake, String user, String role) {
    return new AddRoleToUser(context, metalake, user, role);
  }

  /**
   * Get a new instance of group details command.
   *
   * @param context The command context
   * @param metalake The name of the metalake
   * @param user The group name
   * @return a new instance of group details command
   */
  protected GroupDetails newGroupDetails(CommandContext context, String metalake, String user) {
    return new GroupDetails(context, metalake, user);
  }

  /**
   * Get a new instance of list groups command.
   *
   * @param context The command context
   * @param metalake The name of the metalake
   * @return a new instance of list groups command
   */
  protected ListGroups newListGroups(CommandContext context, String metalake) {
    return new ListGroups(context, metalake);
  }

  /**
   * Get a new instance of group audit command.
   *
   * @param context The command context
   * @param metalake The name of the metalake
   * @param group The name of the group
   * @return a new instance of group audit command
   */
  protected GroupAudit newGroupAudit(CommandContext context, String metalake, String group) {
    return new GroupAudit(context, metalake, group);
  }

  /**
   * Get a new instance of create group command.
   *
   * @param context The command context
   * @param metalake The name of the metalake
   * @param user The name of the user to be added in the group
   * @return a new instance of create group command
   */
  protected CreateGroup newCreateGroup(CommandContext context, String metalake, String user) {
    return new CreateGroup(context, metalake, user);
  }

  /**
   * Get a new instance of delete group command.
   *
   * @param context The command context
   * @param metalake The name of the metalake
   * @param user The name of the user belonging to the group
   * @return a new instance of delete group command
   */
  protected DeleteGroup newDeleteGroup(CommandContext context, String metalake, String user) {
    return new DeleteGroup(context, metalake, user);
  }

  /**
   * Get a new instance of remove role from group command.
   *
   * @param context The command context
   * @param metalake The name of the metalake
   * @param group The name of the group
   * @param role The role to be removed from the group
   * @return a new instance of remove role from group command
   */
  protected RemoveRoleFromGroup newRemoveRoleFromGroup(
      CommandContext context, String metalake, String group, String role) {
    return new RemoveRoleFromGroup(context, metalake, group, role);
  }

  /**
   * Get a new instance of remove all roles command for an entity.
   *
   * @param context The command context
   * @param metalake The name of the metalake
   * @param entity The entity name
   * @param entityType The type of the entity (user or group)
   * @return a new instance of remove all roles command
   */
  protected RemoveAllRoles newRemoveAllRoles(
      CommandContext context, String metalake, String entity, String entityType) {
    return new RemoveAllRoles(context, metalake, entity, entityType);
  }

  /**
   * Get a new instance of add role to group command.
   *
   * @param context The command context
   * @param metalake The name of the metalake
   * @param group The name of the group
   * @param role The role to be added
   * @return a new instance of add role to group command
   */
  protected AddRoleToGroup newAddRoleToGroup(
      CommandContext context, String metalake, String group, String role) {
    return new AddRoleToGroup(context, metalake, group, role);
  }

  /**
   * Get a new instance of role details command.
   *
   * @param context The command context
   * @param metalake The name of the metalake
   * @param role The name of the role
   * @return a new instance of role details command
   */
  protected RoleDetails newRoleDetails(CommandContext context, String metalake, String role) {
    return new RoleDetails(context, metalake, role);
  }

  /**
   * Get a new instance of list roles command.
   *
   * @param context The command context
   * @param metalake The name of the metalake
   * @return a new instance of list roles command
   */
  protected ListRoles newListRoles(CommandContext context, String metalake) {
    return new ListRoles(context, metalake);
  }

  /**
   * Get a new instance of role audit command.
   *
   * @param context The command context
   * @param metalake The name of the metalake
   * @param role The name of the role
   * @return a new instance of role audit command
   */
  protected RoleAudit newRoleAudit(CommandContext context, String metalake, String role) {
    return new RoleAudit(context, metalake, role);
  }

  /**
   * Get a new instance of create role command.
   *
   * @param context The command context
   * @param metalake The name of the metalake
   * @param roles The array of roles to be created
   * @return a new instance of create role command
   */
  protected CreateRole newCreateRole(CommandContext context, String metalake, String[] roles) {
    return new CreateRole(context, metalake, roles);
  }

  /**
   * Get a new instance of delete role command.
   *
   * @param context The command context
   * @param metalake The name of the metalake
   * @param roles The array of roles to be deleted
   * @return a new instance of delete role command
   */
  protected DeleteRole newDeleteRole(CommandContext context, String metalake, String[] roles) {
    return new DeleteRole(context, metalake, roles);
  }

  /**
   * Get a new instance of tag details command.
   *
   * @param context The command context
   * @param metalake The name of the metalake
   * @param tag The name of the tag
   * @return a new instance of tag details command
   */
  protected TagDetails newTagDetails(CommandContext context, String metalake, String tag) {
    return new TagDetails(context, metalake, tag);
  }

  /**
   * Get a new instance of list all tags command.
   *
   * @param context The command context
   * @param metalake The name of the metalake
   * @return a new instance of list all tags command
   */
  protected ListAllTags newListTags(CommandContext context, String metalake) {
    return new ListAllTags(context, metalake);
  }

  /**
   * Get a new instance of create tag command.
   *
   * @param context The command context
   * @param metalake The name of the metalake
   * @param tags The array of tags to be created
   * @param comment The comment associated with the tag
   * @return a new instance of create tag command
   */
  protected CreateTag newCreateTags(
      CommandContext context, String metalake, String[] tags, String comment) {
    return new CreateTag(context, metalake, tags, comment);
  }

  /**
   * Get a new instance of delete tag command.
   *
   * @param context The command context
   * @param metalake The name of the metalake
   * @param tags The array of tags to be deleted
   * @return a new instance of delete tag command
   */
  protected DeleteTag newDeleteTag(CommandContext context, String metalake, String[] tags) {
    return new DeleteTag(context, metalake, tags);
  }

  /**
   * Get a new instance of set tag property command.
   *
   * @param context The command context
   * @param metalake The name of the metalake
   * @param tag The name of the tag
   * @param property The property key
   * @param value The property value
   * @return a new instance of set tag property command
   */
  protected SetTagProperty newSetTagProperty(
      CommandContext context, String metalake, String tag, String property, String value) {
    return new SetTagProperty(context, metalake, tag, property, value);
  }

  /**
   * Get a new instance of remove tag property command.
   *
   * @param context The command context
   * @param metalake The name of the metalake
   * @param tag The name of the tag
   * @param property The property key to be removed
   * @return a new instance of remove tag property command
   */
  protected RemoveTagProperty newRemoveTagProperty(
      CommandContext context, String metalake, String tag, String property) {
    return new RemoveTagProperty(context, metalake, tag, property);
  }

  /**
   * Get a new instance of remove all tags command from an entity.
   *
   * @param context The command context
   * @param metalake The name of the metalake
   * @param name The full name of the entity
   * @return a new instance of remove all tags command
   */
  protected RemoveAllTags newRemoveAllTags(CommandContext context, String metalake, FullName name) {
    return new RemoveAllTags(context, metalake, name);
  }

  /**
   * Get a new instance of list tag properties command.
   *
   * @param context The command context
   * @param metalake The name of the metalake
   * @param tag The name of the tag
   * @return a new instance of list tag properties command
   */
  protected ListTagProperties newListTagProperties(
      CommandContext context, String metalake, String tag) {
    return new ListTagProperties(context, metalake, tag);
  }

  /**
   * Get a new instance of update tag comment command.
   *
   * @param context The command context
   * @param metalake The name of the metalake
   * @param tag The name of the tag
   * @param comment The new comment
   * @return a new instance of update tag comment command
   */
  protected UpdateTagComment newUpdateTagComment(
      CommandContext context, String metalake, String tag, String comment) {
    return new UpdateTagComment(context, metalake, tag, comment);
  }

  /**
   * Get a new instance of update tag name command.
   *
   * @param context The command context
   * @param metalake The name of the metalake
   * @param tag The old tag name
   * @param newName The new tag name
   * @return a new instance of update tag name command
   */
  protected UpdateTagName newUpdateTagName(
      CommandContext context, String metalake, String tag, String newName) {
    return new UpdateTagName(context, metalake, tag, newName);
  }

  /**
   * Get a new instance of list entity tags command.
   *
   * @param context The command context
   * @param metalake The name of the metalake
   * @param name The full name of the entity
   * @return a new instance of list entity tags command
   */
  protected ListEntityTags newListEntityTags(
      CommandContext context, String metalake, FullName name) {
    return new ListEntityTags(context, metalake, name);
  }

  /**
   * Get a new instance of tag entity command.
   *
   * @param context The command context
   * @param metalake The name of the metalake
   * @param name The full name of the entity
   * @param tags The array of tags to be associated
   * @return a new instance of tag entity command
   */
  protected TagEntity newTagEntity(
      CommandContext context, String metalake, FullName name, String[] tags) {
    return new TagEntity(context, metalake, name, tags);
  }

  /**
   * Get a new instance of untag entity command.
   *
   * @param context The command context
   * @param metalake The name of the metalake
   * @param name The full name of the entity
   * @param tags The array of tags to be removed
   * @return a new instance of untag entity command
   */
  protected UntagEntity newUntagEntity(
      CommandContext context, String metalake, FullName name, String[] tags) {
    return new UntagEntity(context, metalake, name, tags);
  }

  /**
   * Create a new {@link ColumnAudit} command to audit a specific column.
   *
   * @param context the command execution context
   * @param metalake the target metalake name
   * @param catalog the target catalog name
   * @param schema the target schema name
   * @param table the target table name
   * @param column the target column name
   * @return a new {@link ColumnAudit} command instance
   */
  protected ColumnAudit newColumnAudit(
      CommandContext context,
      String metalake,
      String catalog,
      String schema,
      String table,
      String column) {
    return new ColumnAudit(context, metalake, catalog, schema, table, column);
  }

  /**
   * Create a new {@link ListColumns} command to list all columns of a table.
   *
   * @param context the command execution context
   * @param metalake the target metalake name
   * @param catalog the target catalog name
   * @param schema the target schema name
   * @param table the target table name
   * @return a new {@link ListColumns} command instance
   */
  protected ListColumns newListColumns(
      CommandContext context, String metalake, String catalog, String schema, String table) {
    return new ListColumns(context, metalake, catalog, schema, table);
  }

  /**
   * Create a new {@link SetOwner} command to set the owner of an entity.
   *
   * @param context the command execution context
   * @param metalake the target metalake name
   * @param entity the entity identifier (e.g., table, schema)
   * @param entityType the entity type (e.g., TABLE, SCHEMA)
   * @param owner the new owner name
   * @param isGroup true if the owner is a group, false if it is a user
   * @return a new {@link SetOwner} command instance
   */
  protected SetOwner newSetOwner(
      CommandContext context,
      String metalake,
      String entity,
      String entityType,
      String owner,
      boolean isGroup) {
    return new SetOwner(context, metalake, entity, entityType, owner, isGroup);
  }

  /**
   * Create a new {@link OwnerDetails} command to fetch the owner details of an entity.
   *
   * @param context the command execution context
   * @param metalake the target metalake name
   * @param entity the entity identifier
   * @param entityType the type of the entity
   * @return a new {@link OwnerDetails} command instance
   */
  protected OwnerDetails newOwnerDetails(
      CommandContext context, String metalake, String entity, String entityType) {
    return new OwnerDetails(context, metalake, entity, entityType);
  }

  /**
   * Create a new {@link ListTopics} command to list topics within a schema.
   *
   * @param context the command execution context
   * @param metalake the target metalake name
   * @param catalog the target catalog name
   * @param schema the target schema name
   * @return a new {@link ListTopics} command instance
   */
  protected ListTopics newListTopics(
      CommandContext context, String metalake, String catalog, String schema) {
    return new ListTopics(context, metalake, catalog, schema);
  }

  /**
   * Create a new {@link TopicDetails} command to fetch details of a specific topic.
   *
   * @param context the command execution context
   * @param metalake the target metalake name
   * @param catalog the target catalog name
   * @param schema the target schema name
   * @param topic the target topic name
   * @return a new {@link TopicDetails} command instance
   */
  protected TopicDetails newTopicDetails(
      CommandContext context, String metalake, String catalog, String schema, String topic) {
    return new TopicDetails(context, metalake, catalog, schema, topic);
  }

  /**
   * Create a new {@link CreateTopic} command to create a topic with an optional comment.
   *
   * @param context the command execution context
   * @param metalake the target metalake name
   * @param catalog the target catalog name
   * @param schema the target schema name
   * @param topic the topic name
   * @param comment an optional comment for the topic
   * @return a new {@link CreateTopic} command instance
   */
  protected CreateTopic newCreateTopic(
      CommandContext context,
      String metalake,
      String catalog,
      String schema,
      String topic,
      String comment) {
    return new CreateTopic(context, metalake, catalog, schema, topic, comment);
  }

  /**
   * Create a new {@link DeleteTopic} command to delete a topic.
   *
   * @param context the command execution context
   * @param metalake the target metalake name
   * @param catalog the target catalog name
   * @param schema the target schema name
   * @param topic the topic to delete
   * @return a new {@link DeleteTopic} command instance
   */
  protected DeleteTopic newDeleteTopic(
      CommandContext context, String metalake, String catalog, String schema, String topic) {
    return new DeleteTopic(context, metalake, catalog, schema, topic);
  }

  /**
   * Create a new {@link UpdateTopicComment} command to update the comment of a topic.
   *
   * @param context the command execution context
   * @param metalake the target metalake name
   * @param catalog the target catalog name
   * @param schema the target schema name
   * @param topic the topic name
   * @param comment the new comment
   * @return a new {@link UpdateTopicComment} command instance
   */
  protected UpdateTopicComment newUpdateTopicComment(
      CommandContext context,
      String metalake,
      String catalog,
      String schema,
      String topic,
      String comment) {
    return new UpdateTopicComment(context, metalake, catalog, schema, topic, comment);
  }

  /**
   * Create a new {@link SetTopicProperty} command to set a property of a topic.
   *
   * @param context the command execution context
   * @param metalake the target metalake name
   * @param catalog the target catalog name
   * @param schema the target schema name
   * @param topic the target topic name
   * @param property the property key
   * @param value the property value
   * @return a new {@link SetTopicProperty} command instance
   */
  protected SetTopicProperty newSetTopicProperty(
      CommandContext context,
      String metalake,
      String catalog,
      String schema,
      String topic,
      String property,
      String value) {
    return new SetTopicProperty(context, metalake, catalog, schema, topic, property, value);
  }

  /**
   * Create a new {@link RemoveTopicProperty} command to remove a property from a topic.
   *
   * @param context the command execution context
   * @param metalake the target metalake name
   * @param catalog the target catalog name
   * @param schema the target schema name
   * @param topic the target topic name
   * @param property the property key to remove
   * @return a new {@link RemoveTopicProperty} command instance
   */
  protected RemoveTopicProperty newRemoveTopicProperty(
      CommandContext context,
      String metalake,
      String catalog,
      String schema,
      String topic,
      String property) {
    return new RemoveTopicProperty(context, metalake, catalog, schema, topic, property);
  }

  /**
   * Create a new {@link ListTopicProperties} command to list all properties of a topic.
   *
   * @param context the command execution context
   * @param metalake the target metalake name
   * @param catalog the target catalog name
   * @param schema the target schema name
   * @param topic the target topic name
   * @return a new {@link ListTopicProperties} command instance
   */
  protected ListTopicProperties newListTopicProperties(
      CommandContext context, String metalake, String catalog, String schema, String topic) {
    return new ListTopicProperties(context, metalake, catalog, schema, topic);
  }

  /**
   * Create a new {@link FilesetDetails} command to fetch details of a fileset.
   *
   * @param context the command execution context
   * @param metalake the target metalake name
   * @param catalog the target catalog name
   * @param schema the target schema name
   * @param fileset the target fileset name
   * @return a new {@link FilesetDetails} command instance
   */
  protected FilesetDetails newFilesetDetails(
      CommandContext context, String metalake, String catalog, String schema, String fileset) {
    return new FilesetDetails(context, metalake, catalog, schema, fileset);
  }

  /**
   * Create a new {@link ListFilesets} command to list filesets in a schema.
   *
   * @param context the command execution context
   * @param metalake the target metalake name
   * @param catalog the target catalog name
   * @param schema the target schema name
   * @return a new {@link ListFilesets} command instance
   */
  protected ListFilesets newListFilesets(
      CommandContext context, String metalake, String catalog, String schema) {
    return new ListFilesets(context, metalake, catalog, schema);
  }

  /**
   * Create a new {@link CreateFileset} command to create a fileset with properties.
   *
   * @param context the command execution context
   * @param metalake the target metalake name
   * @param catalog the target catalog name
   * @param schema the target schema name
   * @param fileset the fileset name
   * @param comment an optional comment
   * @param propertyMap a map of custom properties
   * @return a new {@link CreateFileset} command instance
   */
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

  /**
   * Create a new {@link DeleteFileset} command to delete a fileset.
   *
   * @param context the command execution context
   * @param metalake the target metalake name
   * @param catalog the target catalog name
   * @param schema the target schema name
   * @param fileset the fileset to delete
   * @return a new {@link DeleteFileset} command instance
   */
  protected DeleteFileset newDeleteFileset(
      CommandContext context, String metalake, String catalog, String schema, String fileset) {
    return new DeleteFileset(context, metalake, catalog, schema, fileset);
  }

  /**
   * Create a new {@link UpdateFilesetComment} command to update the comment of a fileset.
   *
   * @param context the command execution context
   * @param metalake the target metalake name
   * @param catalog the target catalog name
   * @param schema the target schema name
   * @param fileset the fileset name
   * @param comment the new comment
   * @return a new {@link UpdateFilesetComment} command instance
   */
  protected UpdateFilesetComment newUpdateFilesetComment(
      CommandContext context,
      String metalake,
      String catalog,
      String schema,
      String fileset,
      String comment) {
    return new UpdateFilesetComment(context, metalake, catalog, schema, fileset, comment);
  }

  /**
   * Create a new {@link UpdateFilesetName} command to rename a fileset.
   *
   * @param context the command execution context
   * @param metalake the target metalake name
   * @param catalog the target catalog name
   * @param schema the target schema name
   * @param fileset the original fileset name
   * @param rename the new fileset name
   * @return a new {@link UpdateFilesetName} command instance
   */
  protected UpdateFilesetName newUpdateFilesetName(
      CommandContext context,
      String metalake,
      String catalog,
      String schema,
      String fileset,
      String rename) {
    return new UpdateFilesetName(context, metalake, catalog, schema, fileset, rename);
  }

  /**
   * Get a new instance of the command to list properties of a fileset.
   *
   * @param context The command context
   * @param metalake The name of the metalake
   * @param catalog The name of the catalog
   * @param schema The name of the schema
   * @param fileset The name of the fileset
   * @return a new instance of the command to list properties of a fileset.
   */
  protected ListFilesetProperties newListFilesetProperties(
      CommandContext context, String metalake, String catalog, String schema, String fileset) {
    return new ListFilesetProperties(context, metalake, catalog, schema, fileset);
  }

  /**
   * Get a new instance of the command to set a property of a fileset.
   *
   * @param context The command context
   * @param metalake The name of the metalake
   * @param catalog The name of the catalog
   * @param schema The name of the schema
   * @param fileset The name of the fileset
   * @param property The property key
   * @param value The property value
   * @return a new instance of the command to set a property of a fileset.
   */
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

  /**
   * Get a new instance of the command to remove a property from a fileset.
   *
   * @param context The command context
   * @param metalake The name of the metalake
   * @param catalog The name of the catalog
   * @param schema The name of the schema
   * @param fileset The name of the fileset
   * @param property The property key to remove
   * @return a new instance of the command to remove a property from a fileset.
   */
  protected RemoveFilesetProperty newRemoveFilesetProperty(
      CommandContext context,
      String metalake,
      String catalog,
      String schema,
      String fileset,
      String property) {
    return new RemoveFilesetProperty(context, metalake, catalog, schema, fileset, property);
  }

  /**
   * Get a new instance of the command to add a column.
   *
   * @param context The command context.
   * @param metalake The name of the metalake instance.
   * @param catalog The name of the catalog.
   * @param schema The name of the schema.
   * @param table The name of the table.
   * @param column The name of the column.
   * @param datatype The data type of the column.
   * @param comment The comment for the column.
   * @param position The position of the column.
   * @param nullable whether the column is nullable.
   * @param autoIncrement whether the column is auto increment.
   * @param defaultValue The default value for the column.
   * @return a new instance of the command.
   */
  protected AddColumn newAddColumn(
      CommandContext context,
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
        context,
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

  /**
   * Get a new instance of the command to delete a column.
   *
   * @param context The command context.
   * @param metalake The name of the metalake instance.
   * @param catalog The name of the catalog.
   * @param schema The name of the schema.
   * @param table The name of the table.
   * @param column The name of the column.
   * @return The instance of the command.
   */
  protected DeleteColumn newDeleteColumn(
      CommandContext context,
      String metalake,
      String catalog,
      String schema,
      String table,
      String column) {
    return new DeleteColumn(context, metalake, catalog, schema, table, column);
  }

  /**
   * Get a new instance of the command to update a column comment.
   *
   * @param context The command context.
   * @param metalake The name of the metalake instance.
   * @param catalog The name of the catalog.
   * @param schema The name of the schema.
   * @param table The name of the table.
   * @param column The name of the column.
   * @param comment The new comment for the column.
   * @return The instance of the command.
   */
  protected UpdateColumnComment newUpdateColumnComment(
      CommandContext context,
      String metalake,
      String catalog,
      String schema,
      String table,
      String column,
      String comment) {
    return new UpdateColumnComment(context, metalake, catalog, schema, table, column, comment);
  }

  /**
   * Get a new instance of the command to update a column name.
   *
   * @param context The command context.
   * @param metalake The name of the metalake instance.
   * @param catalog The name of the catalog.
   * @param schema The name of the schema.
   * @param table The name of the table.
   * @param column The name of the column.
   * @param rename The new name for the column.
   * @return The instance of the command.
   */
  protected UpdateColumnName newUpdateColumnName(
      CommandContext context,
      String metalake,
      String catalog,
      String schema,
      String table,
      String column,
      String rename) {
    return new UpdateColumnName(context, metalake, catalog, schema, table, column, rename);
  }

  /**
   * Get a new instance of the command to update a column datatype.
   *
   * @param context The command context.
   * @param metalake The name of the metalake instance.
   * @param catalog The name of the catalog.
   * @param schema The name of the schema.
   * @param table The name of the table.
   * @param column The name of the column.
   * @param datatype The new data type for the column.
   * @return The instance of the command.
   */
  protected UpdateColumnDatatype newUpdateColumnDatatype(
      CommandContext context,
      String metalake,
      String catalog,
      String schema,
      String table,
      String column,
      String datatype) {
    return new UpdateColumnDatatype(context, metalake, catalog, schema, table, column, datatype);
  }

  /**
   * Get a new instance of the command to update a column position.
   *
   * @param context The command context.
   * @param metalake The name of the metalake instance.
   * @param catalog The name of the catalog.
   * @param schema The name of the schema.
   * @param table The name of the table.
   * @param column The name of the column.
   * @param position The new position for the column.
   * @return The instance of the command.
   */
  protected UpdateColumnPosition newUpdateColumnPosition(
      CommandContext context,
      String metalake,
      String catalog,
      String schema,
      String table,
      String column,
      String position) {
    return new UpdateColumnPosition(context, metalake, catalog, schema, table, column, position);
  }

  /**
   * Get a new instance of the command to update a column nullable.
   *
   * @param context The command context.
   * @param metalake The name of the metalake instance.
   * @param catalog The name of the catalog.
   * @param schema The name of the schema.
   * @param table The name of the table.
   * @param column The name of the column.
   * @param nullable whether the column is nullable.
   * @return The instance of the command.
   */
  protected UpdateColumnNullability newUpdateColumnNullability(
      CommandContext context,
      String metalake,
      String catalog,
      String schema,
      String table,
      String column,
      boolean nullable) {
    return new UpdateColumnNullability(context, metalake, catalog, schema, table, column, nullable);
  }

  /**
   * Get a new instance of the command to update a column auto increment.
   *
   * @param context The command context.
   * @param metalake The name of the metalake instance.
   * @param catalog The name of the catalog.
   * @param schema The name of the schema.
   * @param table The name of the table.
   * @param column The name of the column.
   * @param autoIncrement whether the column is auto increment.
   * @return The instance of the command.
   */
  protected UpdateColumnAutoIncrement newUpdateColumnAutoIncrement(
      CommandContext context,
      String metalake,
      String catalog,
      String schema,
      String table,
      String column,
      boolean autoIncrement) {
    return new UpdateColumnAutoIncrement(
        context, metalake, catalog, schema, table, column, autoIncrement);
  }

  /**
   * Get a new instance of the command to update a column default value.
   *
   * @param context The command context.
   * @param metalake The name of the metalake instance.
   * @param catalog The name of the catalog.
   * @param schema The name of the schema.
   * @param table The name of the table.
   * @param column The name of the column.
   * @param defaultValue The new default value for the column.
   * @param dataType The data type of the column.
   * @return The instance of the command.
   */
  protected UpdateColumnDefault newUpdateColumnDefault(
      CommandContext context,
      String metalake,
      String catalog,
      String schema,
      String table,
      String column,
      String defaultValue,
      String dataType) {
    return new UpdateColumnDefault(
        context, metalake, catalog, schema, table, column, defaultValue, dataType);
  }

  /**
   * Get a new instance of the command to create a new catalog in a metalake instance.
   *
   * @param context The command context.
   * @param metalake The name of the metalake instance.
   * @param catalog The name of the catalog.
   * @param schema The name of the schema.
   * @param table The name of the table.
   * @param columnFile The file containing the column definitions.
   * @param comment The comment for the table.
   * @return The instance of the command.
   */
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

  /**
   * Get a new instance of the command to grant privileges to a role
   *
   * @param context The command context.
   * @param metalake The name of the metalake instance.
   * @param role The name of the role.
   * @param entity The entity to grant privileges on.
   * @param privileges The privileges to grant.
   * @return The instance of the command.
   */
  protected GrantPrivilegesToRole newGrantPrivilegesToRole(
      CommandContext context, String metalake, String role, FullName entity, String[] privileges) {
    return new GrantPrivilegesToRole(context, metalake, role, entity, privileges);
  }

  /**
   * Get a new instance of the command to grant all privileges to a role on an entity.
   *
   * @param context The command context.
   * @param metalake The name of the metalake instance.
   * @param role The name of the role.
   * @param entity The entity to grant privileges on.
   * @param privileges The privileges to grant.
   * @return The instance of the command.
   */
  protected RevokePrivilegesFromRole newRevokePrivilegesFromRole(
      CommandContext context, String metalake, String role, FullName entity, String[] privileges) {
    return new RevokePrivilegesFromRole(context, metalake, role, entity, privileges);
  }

  /**
   * Get a new instance of the command to grant all privileges to a role on an entity.
   *
   * @param context The command context.
   * @param metalake The name of the metalake instance.
   * @param role The name of the role.
   * @param entity The entity to grant privileges on.
   * @return The instance of the command.
   */
  protected RevokeAllPrivileges newRevokeAllPrivileges(
      CommandContext context, String metalake, String role, FullName entity) {
    return new RevokeAllPrivileges(context, metalake, role, entity);
  }

  /**
   * Get a new instance of the command to list the catalogs in a metalake instance.
   *
   * @param context The command context.
   * @param metalake The name of the metalake instance.
   * @return The instance of the command.
   */
  protected ManageMetalake newManageMetalake(CommandContext context, String metalake) {
    return new ManageMetalake(context, metalake);
  }

  /**
   * Get a new instance of the command to list the catalogs in a metalake instance.
   *
   * @param context The command context.
   * @param metalake The name of the metalake instance.
   * @param catalog The name of the catalog.
   * @return The instance of the command.
   */
  protected ManageCatalog newManageCatalog(
      CommandContext context, String metalake, String catalog) {
    return new ManageCatalog(context, metalake, catalog);
  }

  /**
   * Get a new instance of the command to list the tables in a catalog.
   *
   * @param context The command context.
   * @param metalake The name of the metalake instance.
   * @param catalog The name of the catalog.
   * @param schema The name of the schema.
   * @return The instance of the command.
   */
  protected ListModel newListModel(
      CommandContext context, String metalake, String catalog, String schema) {
    return new ListModel(context, metalake, catalog, schema);
  }

  /**
   * Get a new instance of the command to list the tables in a catalog.
   *
   * @param context The command context.
   * @param metalake The name of the metalake instance.
   * @param catalog The name of the catalog.
   * @param schema The name of the schema.
   * @param model The name of the model.
   * @return The instance of the command.
   */
  protected ModelAudit newModelAudit(
      CommandContext context, String metalake, String catalog, String schema, String model) {
    return new ModelAudit(context, metalake, catalog, schema, model);
  }

  /**
   * Get a new instance of the command to list the tables in a catalog.
   *
   * @param context The command context.
   * @param metalake The name of the metalake instance.
   * @param catalog The name of the catalog.
   * @param schema The name of the schema.
   * @param model The name of the model.
   * @return The instance of the command.
   */
  protected ModelDetails newModelDetails(
      CommandContext context, String metalake, String catalog, String schema, String model) {
    return new ModelDetails(context, metalake, catalog, schema, model);
  }

  /**
   * Get a new instance of the command to delete a model.
   *
   * @param context The command context.
   * @param metalake The name of the metalake instance.
   * @param catalog The name of the catalog.
   * @param schema The name of the schema.
   * @param model The name of the model.
   * @param comment The comment for the model.
   * @param properties The properties for the model.
   * @return The instance of the command.
   */
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

  /**
   * Get a new instance of the command to update a model name.
   *
   * @param context The command context.
   * @param metalake The name of the metalake instance.
   * @param catalog The name of the catalog.
   * @param schema The name of the schema.
   * @param model The name of the model.
   * @param rename The new name for the model.
   * @return The instance of the command.
   */
  protected UpdateModelName newUpdateModelName(
      CommandContext context,
      String metalake,
      String catalog,
      String schema,
      String model,
      String rename) {
    return new UpdateModelName(context, metalake, catalog, schema, model, rename);
  }

  /**
   * Get a new instance of the command to update a model comment.
   *
   * @param context The command context.
   * @param metalake The name of the metalake instance.
   * @param catalog The name of the catalog.
   * @param schema The name of the schema.
   * @param model The name of the model.
   * @param comment The new comment for the model.
   * @return The instance of the command.
   */
  protected UpdateModelComment newUpdateModelComment(
      CommandContext context,
      String metalake,
      String catalog,
      String schema,
      String model,
      String comment) {
    return new UpdateModelComment(context, metalake, catalog, schema, model, comment);
  }

  /**
   * Get a new instance of the command to add a model version.
   *
   * @param context The command context.
   * @param metalake The name of the metalake instance.
   * @param catalog The name of the catalog.
   * @param schema The name of the schema.
   * @param model The name of the model.
   * @param version The version of the model.
   * @param alias The alias of the model version.
   * @param comment The comment for the model version.
   * @return The instance of the command.
   */
  protected UpdateModelVersionComment newUpdateModelVersionComment(
      CommandContext context,
      String metalake,
      String catalog,
      String schema,
      String model,
      Integer version,
      String alias,
      String comment) {
    return new UpdateModelVersionComment(
        context, metalake, catalog, schema, model, version, alias, comment);
  }

  /**
   * Get a new instance of the command to add a model version.
   *
   * @param context The command context.
   * @param metalake The name of the metalake.
   * @param catalog The name of the catalog.
   * @param schema The name of the schema.
   * @param model The name of the model.
   * @param version The version of the model.
   * @param alias The alias of the model version.
   * @param uri The URI of the model version.
   * @return The instance of the command.
   */
  protected UpdateModelVersionUri newUpdateModelVersionUri(
      CommandContext context,
      String metalake,
      String catalog,
      String schema,
      String model,
      Integer version,
      String alias,
      String uri) {
    return new UpdateModelVersionUri(
        context, metalake, catalog, schema, model, version, alias, uri);
  }

  /**
   * Get a new instance of the command to add a model version alias.
   *
   * @param context The command context.
   * @param metalake The name of the metalake.
   * @param catalog The name of the catalog.
   * @param schema The name of the schema.
   * @param model The name of the model.
   * @param version The version of the model.
   * @param alias The alias of the model version.
   * @param aliasesToAdd The aliases to add.
   * @param aliasesToRemove The aliases to remove.
   * @return The instance of the command.
   */
  protected UpdateModelVersionAliases newUpdateModelVersionAliases(
      CommandContext context,
      String metalake,
      String catalog,
      String schema,
      String model,
      Integer version,
      String alias,
      String[] aliasesToAdd,
      String[] aliasesToRemove) {
    return new UpdateModelVersionAliases(
        context, metalake, catalog, schema, model, version, alias, aliasesToAdd, aliasesToRemove);
  }

  /**
   * Get a new instance of the command to remove a model property.
   *
   * @param context The command context.
   * @param metalake The name of the metalake.
   * @param catalog The name of the catalog.
   * @param schema The name of the schema.
   * @param model The name of the model.
   * @param property The name of the property to remove.
   * @param value The value of the property to remove.
   * @return The instance of the command.
   */
  protected SetModelProperty newSetModelProperty(
      CommandContext context,
      String metalake,
      String catalog,
      String schema,
      String model,
      String property,
      String value) {
    return new SetModelProperty(context, metalake, catalog, schema, model, property, value);
  }

  /**
   * Get a new instance of the command to remove a model version property.
   *
   * @param context The command context.
   * @param metalake The name of the metalake instance.
   * @param catalog The name of the catalog.
   * @param schema The name of the schema.
   * @param model The name of the model.
   * @param version The version of the model.
   * @param alias The alias of the model version.
   * @param property The name of the property to remove.
   * @return The instance of the command.
   */
  protected RemoveModelVersionProperty newRemoveModelVersionProperty(
      CommandContext context,
      String metalake,
      String catalog,
      String schema,
      String model,
      Integer version,
      String alias,
      String property) {
    return new RemoveModelVersionProperty(
        context, metalake, catalog, schema, model, version, alias, property);
  }

  /**
   * Get a new instance of the command to remove a model version property.
   *
   * @param context The command context.
   * @param metalake The name of the metalake instance.
   * @param catalog The name of the catalog.
   * @param schema The name of the schema.
   * @param model The name of the model.
   * @param version The version of the model.
   * @param alias The alias of the model version.
   * @param property The name of the property to remove.
   * @param value The value of the property to remove.
   * @return The instance of the command.
   */
  protected SetModelVersionProperty newSetModelVersionProperty(
      CommandContext context,
      String metalake,
      String catalog,
      String schema,
      String model,
      Integer version,
      String alias,
      String property,
      String value) {
    return new SetModelVersionProperty(
        context, metalake, catalog, schema, model, version, alias, property, value);
  }

  /**
   * Get a new instance of the command to remove a model property.
   *
   * @param context The command context.
   * @param metalake The name of the metalake instance.
   * @param catalog The name of the catalog.
   * @param schema The name of the schema.
   * @param model The name of the model.
   * @param property The name of the property to remove.
   * @return The instance of the command.
   */
  protected RemoveModelProperty newRemoveModelProperty(
      CommandContext context,
      String metalake,
      String catalog,
      String schema,
      String model,
      String property) {
    return new RemoveModelProperty(context, metalake, catalog, schema, model, property);
  }

  /**
   * Get a new instance of the command to delete a model.
   *
   * @param context The command context.
   * @param metalake The name of the metalake instance.
   * @param catalog The name of the catalog.
   * @param schema The name of the schema.
   * @param model The name of the model.
   * @return The instance of the command.
   */
  protected DeleteModel newDeleteModel(
      CommandContext context, String metalake, String catalog, String schema, String model) {
    return new DeleteModel(context, metalake, catalog, schema, model);
  }

  /**
   * Get a new instance of the command to link a model version.
   *
   * @param context The command context.
   * @param metalake The name of the metalake instance.
   * @param catalog The name of the catalog.
   * @param schema The name of the schema.
   * @param model The name of the model.
   * @param uris The map of external resource URIs to aliases.
   * @param alias The alias of the model version to link.
   * @param comment The comment for the linked model version.
   * @param properties The properties for the linked model version.
   * @return The instance of the command.
   */
  protected LinkModel newLinkModel(
      CommandContext context,
      String metalake,
      String catalog,
      String schema,
      String model,
      Map<String, String> uris,
      String[] alias,
      String comment,
      Map<String, String> properties) {
    return new LinkModel(
        context, metalake, catalog, schema, model, uris, alias, comment, properties);
  }

  /**
   * Get a new instance of the command to list the tables in a schema.
   *
   * @param context The command context.
   * @param metalake The name of the metalake instance.
   * @param catalog The name of the catalog.
   * @param schema The name of the schema.
   * @return The instance of the command.
   */
  protected ListTables newListTables(
      CommandContext context, String metalake, String catalog, String schema) {
    return new ListTables(context, metalake, catalog, schema);
  }
}
