/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.authorization;

import static com.datastrato.gravitino.authorization.Privilege.Name.ADD_GROUP;
import static com.datastrato.gravitino.authorization.Privilege.Name.ADD_USER;
import static com.datastrato.gravitino.authorization.Privilege.Name.ALTER_CATALOG;
import static com.datastrato.gravitino.authorization.Privilege.Name.ALTER_SCHEMA;
import static com.datastrato.gravitino.authorization.Privilege.Name.CREATE_CATALOG;
import static com.datastrato.gravitino.authorization.Privilege.Name.CREATE_FILESET;
import static com.datastrato.gravitino.authorization.Privilege.Name.CREATE_METALAKE;
import static com.datastrato.gravitino.authorization.Privilege.Name.CREATE_ROLE;
import static com.datastrato.gravitino.authorization.Privilege.Name.CREATE_SCHEMA;
import static com.datastrato.gravitino.authorization.Privilege.Name.CREATE_TABLE;
import static com.datastrato.gravitino.authorization.Privilege.Name.CREATE_TOPIC;
import static com.datastrato.gravitino.authorization.Privilege.Name.DELETE_ROLE;
import static com.datastrato.gravitino.authorization.Privilege.Name.DROP_CATALOG;
import static com.datastrato.gravitino.authorization.Privilege.Name.DROP_FILESET;
import static com.datastrato.gravitino.authorization.Privilege.Name.DROP_SCHEMA;
import static com.datastrato.gravitino.authorization.Privilege.Name.DROP_TABLE;
import static com.datastrato.gravitino.authorization.Privilege.Name.DROP_TOPIC;
import static com.datastrato.gravitino.authorization.Privilege.Name.GET_GROUP;
import static com.datastrato.gravitino.authorization.Privilege.Name.GET_ROLE;
import static com.datastrato.gravitino.authorization.Privilege.Name.GET_USER;
import static com.datastrato.gravitino.authorization.Privilege.Name.GRANT_ROLE;
import static com.datastrato.gravitino.authorization.Privilege.Name.MANAGE_METALAKE;
import static com.datastrato.gravitino.authorization.Privilege.Name.READ_FILESET;
import static com.datastrato.gravitino.authorization.Privilege.Name.READ_TABLE;
import static com.datastrato.gravitino.authorization.Privilege.Name.READ_TOPIC;
import static com.datastrato.gravitino.authorization.Privilege.Name.REMOVE_GROUP;
import static com.datastrato.gravitino.authorization.Privilege.Name.REMOVE_USER;
import static com.datastrato.gravitino.authorization.Privilege.Name.REVOKE_ROLE;
import static com.datastrato.gravitino.authorization.Privilege.Name.USE_CATALOG;
import static com.datastrato.gravitino.authorization.Privilege.Name.USE_METALAKE;
import static com.datastrato.gravitino.authorization.Privilege.Name.USE_SCHEMA;
import static com.datastrato.gravitino.authorization.Privilege.Name.WRITE_FILESET;
import static com.datastrato.gravitino.authorization.Privilege.Name.WRITE_TABLE;
import static com.datastrato.gravitino.authorization.Privilege.Name.WRITE_TOPIC;

/** The helper class for {@link Privilege}. */
public class Privileges {

  /**
   * Returns the Privilege with allow condition from the string representation.
   *
   * @param privilege The string representation of the privilege.
   * @return The Privilege.
   */
  public static Privilege allow(String privilege) {
    Privilege.Name name = Privilege.Name.valueOf(privilege);
    return allow(name);
  }

  /**
   * Returns the Privilege with allow condition from the `Privilege.Name`.
   *
   * @param name The `Privilege.Name` of the privilege.
   * @return The Privilege.
   */
  public static Privilege allow(Privilege.Name name) {
    switch (name) {
        // Catalog
      case CREATE_CATALOG:
        return CreateCatalog.allow();
      case DROP_CATALOG:
        return DropCatalog.allow();
      case ALTER_CATALOG:
        return AlterCatalog.allow();
      case USE_CATALOG:
        return UseCatalog.allow();

        // Schema
      case CREATE_SCHEMA:
        return CreateSchema.allow();
      case DROP_SCHEMA:
        return DropSchema.allow();
      case ALTER_SCHEMA:
        return AlterSchema.allow();
      case USE_SCHEMA:
        return UseSchema.allow();

        // Table
      case CREATE_TABLE:
        return CreateTable.allow();
      case DROP_TABLE:
        return DropTable.allow();
      case WRITE_TABLE:
        return WriteTable.allow();
      case READ_TABLE:
        return ReadTable.allow();

        // Fileset
      case CREATE_FILESET:
        return CreateFileset.allow();
      case DROP_FILESET:
        return DropFileset.allow();
      case WRITE_FILESET:
        return WriteFileset.allow();
      case READ_FILESET:
        return ReadFileset.allow();

        // Topic
      case CREATE_TOPIC:
        return CreateTopic.allow();
      case DROP_TOPIC:
        return DropTopic.allow();
      case WRITE_TOPIC:
        return WriteTopic.allow();
      case READ_TOPIC:
        return ReadTopic.allow();

        // Metalake
      case CREATE_METALAKE:
        return CreateMetalake.allow();
      case MANAGE_METALAKE:
        return ManageMetalake.allow();
      case USE_METALAKE:
        return UseMetalake.allow();

        // User
      case ADD_USER:
        return AddUser.allow();
      case REMOVE_USER:
        return RemoveUser.allow();
      case GET_USER:
        return GetUser.allow();

        // Group
      case ADD_GROUP:
        return AddGroup.allow();
      case REMOVE_GROUP:
        return RemoveGroup.allow();
      case GET_GROUP:
        return GetGroup.allow();

        // Role
      case CREATE_ROLE:
        return CreateRole.allow();
      case DELETE_ROLE:
        return DeleteRole.allow();
      case GRANT_ROLE:
        return GrantRole.allow();
      case REVOKE_ROLE:
        return RevokeRole.allow();
      case GET_ROLE:
        return GetRole.allow();

      default:
        throw new IllegalArgumentException("Doesn't support the privilege: " + name);
    }
  }

  /**
   * Returns the Privilege with deny condition from the string representation.
   *
   * @param privilege The string representation of the privilege.
   * @return The Privilege.
   */
  public static Privilege deny(String privilege) {
    Privilege.Name name = Privilege.Name.valueOf(privilege);
    return deny(name);
  }

  /**
   * Returns the Privilege with deny condition from the `Privilege.Name`.
   *
   * @param name The `Privilege.Name` of the privilege.
   * @return The Privilege.
   */
  public static Privilege deny(Privilege.Name name) {
    switch (name) {
        // Catalog
      case CREATE_CATALOG:
        return CreateCatalog.deny();
      case DROP_CATALOG:
        return DropCatalog.deny();
      case ALTER_CATALOG:
        return AlterCatalog.deny();
      case USE_CATALOG:
        return UseCatalog.deny();

        // Schema
      case CREATE_SCHEMA:
        return CreateSchema.deny();
      case DROP_SCHEMA:
        return DropSchema.deny();
      case ALTER_SCHEMA:
        return AlterSchema.deny();
      case USE_SCHEMA:
        return UseSchema.deny();

        // Table
      case CREATE_TABLE:
        return CreateTable.deny();
      case DROP_TABLE:
        return DropTable.deny();
      case WRITE_TABLE:
        return WriteTable.deny();
      case READ_TABLE:
        return ReadTable.deny();

        // Fileset
      case CREATE_FILESET:
        return CreateFileset.deny();
      case DROP_FILESET:
        return DropFileset.deny();
      case WRITE_FILESET:
        return WriteFileset.deny();
      case READ_FILESET:
        return ReadFileset.deny();

        // Topic
      case CREATE_TOPIC:
        return CreateTopic.deny();
      case DROP_TOPIC:
        return DropTopic.deny();
      case WRITE_TOPIC:
        return WriteTopic.deny();
      case READ_TOPIC:
        return ReadTopic.deny();

        // Metalake
      case CREATE_METALAKE:
        return CreateMetalake.deny();
      case MANAGE_METALAKE:
        return ManageMetalake.deny();
      case USE_METALAKE:
        return UseMetalake.deny();

        // User
      case ADD_USER:
        return AddUser.deny();
      case REMOVE_USER:
        return RemoveUser.deny();
      case GET_USER:
        return GetUser.deny();

        // Group
      case ADD_GROUP:
        return AddGroup.deny();
      case REMOVE_GROUP:
        return RemoveGroup.deny();
      case GET_GROUP:
        return GetGroup.deny();

        // Role
      case CREATE_ROLE:
        return CreateRole.deny();
      case DELETE_ROLE:
        return DeleteRole.deny();
      case GRANT_ROLE:
        return GrantRole.deny();
      case REVOKE_ROLE:
        return RevokeRole.deny();
      case GET_ROLE:
        return GetRole.deny();

      default:
        throw new IllegalArgumentException("Doesn't support the privilege: " + name);
    }
  }

  /** The privilege to create a catalog. */
  public static class CreateCatalog extends BasePrivilege {

    private static final CreateCatalog ALLOW_INSTANCE = new CreateCatalog(Condition.ALLOW);

    private static final CreateCatalog DENY_INSTANCE = new CreateCatalog(Condition.DENY);

    private CreateCatalog(Condition condition) {
      super(CREATE_CATALOG, condition);
    }

    /** @return The instance with allow condition of the privilege. */
    public static CreateCatalog allow() {
      return ALLOW_INSTANCE;
    }

    /** @return The instance with deny condition of the privilege. */
    public static CreateCatalog deny() {
      return DENY_INSTANCE;
    }

    /** @return A readable string representation for the privilege. */
    @Override
    public String simpleString() {
      return condition().name() + " create catalog";
    }
  }

  /** The privilege to alter a catalog. */
  public static class AlterCatalog extends BasePrivilege {

    private static final AlterCatalog ALLOW_INSTANCE = new AlterCatalog(Condition.ALLOW);

    private static final AlterCatalog DENY_INSTANCE = new AlterCatalog(Condition.DENY);

    private AlterCatalog(Condition condition) {
      super(ALTER_CATALOG, condition);
    }

    /** @return The instance with allow condition of the privilege. */
    public static AlterCatalog allow() {
      return ALLOW_INSTANCE;
    }

    /** @return The instance with deny condition of the privilege. */
    public static AlterCatalog deny() {
      return DENY_INSTANCE;
    }

    /** @return A readable string representation for the privilege. */
    @Override
    public String simpleString() {
      return condition().name() + " alter catalog";
    }
  }

  /** The privilege to drop a catalog. */
  public static class DropCatalog extends BasePrivilege {

    private static final DropCatalog ALLOW_INSTANCE = new DropCatalog(Condition.DENY);

    private static final DropCatalog DENY_INSTANCE = new DropCatalog(Condition.DENY);

    private DropCatalog(Condition condition) {
      super(DROP_CATALOG, condition);
    }

    /** @return The instance with allow condition of the privilege. */
    public static DropCatalog allow() {
      return ALLOW_INSTANCE;
    }

    /** @return The instance with deny condition of the privilege. */
    public static DropCatalog deny() {
      return DENY_INSTANCE;
    }

    /** @return A readable string representation for the privilege. */
    @Override
    public String simpleString() {
      return condition().name() + " drop catalog";
    }
  }

  /** The privilege to use a catalog. */
  public static class UseCatalog extends BasePrivilege {
    private static final UseCatalog ALLOW_INSTANCE = new UseCatalog(Condition.ALLOW);

    private static final UseCatalog DENY_INSTANCE = new UseCatalog(Condition.DENY);

    private UseCatalog(Condition condition) {
      super(USE_CATALOG, condition);
    }

    /** @return The instance with allow condition of the privilege. */
    public static UseCatalog allow() {
      return ALLOW_INSTANCE;
    }

    /** @return The instance with deny condition of the privilege. */
    public static UseCatalog deny() {
      return DENY_INSTANCE;
    }

    /** @return A readable string representation for the privilege. */
    @Override
    public String simpleString() {
      return condition().name() + " use catalog";
    }
  }

  /** The privilege to use a schema. */
  public static class UseSchema extends BasePrivilege {

    private static final UseSchema ALLOW_INSTANCE = new UseSchema(Condition.ALLOW);

    private static final UseSchema DENY_INSTANCE = new UseSchema(Condition.DENY);

    private UseSchema(Condition condition) {
      super(USE_SCHEMA, condition);
    }

    /** @return The instance with allow condition of the privilege. */
    public static UseSchema allow() {
      return ALLOW_INSTANCE;
    }

    /** @return The instance with deny condition of the privilege. */
    public static UseSchema deny() {
      return DENY_INSTANCE;
    }

    /** @return A readable string representation for the privilege. */
    @Override
    public String simpleString() {
      return condition().name() + " use schema";
    }
  }

  /** The privilege to create a schema. */
  public static class CreateSchema extends BasePrivilege {

    private static final CreateSchema ALLOW_INSTANCE = new CreateSchema(Condition.ALLOW);

    private static final CreateSchema DENY_INSTANCE = new CreateSchema(Condition.DENY);

    private CreateSchema(Condition condition) {
      super(CREATE_SCHEMA, condition);
    }

    /** @return The instance with allow condition of the privilege. */
    public static CreateSchema allow() {
      return ALLOW_INSTANCE;
    }

    /** @return The instance with deny condition of the privilege. */
    public static CreateSchema deny() {
      return DENY_INSTANCE;
    }

    /** @return A readable string representation for the privilege. */
    @Override
    public String simpleString() {
      return condition().name() + " create schema";
    }
  }

  /** The privilege to alter a schema. */
  public static class AlterSchema extends BasePrivilege {

    private static final AlterSchema ALLOW_INSTANCE = new AlterSchema(Condition.ALLOW);

    private static final AlterSchema DENY_INSTANCE = new AlterSchema(Condition.DENY);

    private AlterSchema(Condition condition) {
      super(ALTER_SCHEMA, condition);
    }

    /** @return The instance with allow condition of the privilege. */
    public static AlterSchema allow() {
      return ALLOW_INSTANCE;
    }

    /** @return The instance with deny condition of the privilege. */
    public static AlterSchema deny() {
      return DENY_INSTANCE;
    }

    /** @return A readable string representation for the privilege. */
    @Override
    public String simpleString() {
      return condition().name() + " alter schema";
    }
  }

  /** The privilege to drop a schema. */
  public static class DropSchema extends BasePrivilege {

    private static final DropSchema ALLOW_INSTANCE = new DropSchema(Condition.ALLOW);

    private static final DropSchema DENY_INSTANCE = new DropSchema(Condition.DENY);

    private DropSchema(Condition condition) {
      super(DROP_SCHEMA, condition);
    }

    /** @return The instance with allow condition of the privilege. */
    public static DropSchema allow() {
      return ALLOW_INSTANCE;
    }

    /** @return The instance with deny condition of the privilege. */
    public static DropSchema deny() {
      return DENY_INSTANCE;
    }

    /** @return A readable string representation for the privilege. */
    @Override
    public String simpleString() {
      return condition().name() + " drop schema";
    }
  }

  /** The privilege to create a table. */
  public static class CreateTable extends BasePrivilege {

    private static final CreateTable ALLOW_INSTANCE = new CreateTable(Condition.ALLOW);

    private static final CreateTable DENY_INSTANCE = new CreateTable(Condition.DENY);

    private CreateTable(Condition condition) {
      super(CREATE_TABLE, condition);
    }

    /** @return The instance with allow condition of the privilege. */
    public static CreateTable allow() {
      return ALLOW_INSTANCE;
    }

    /** @return The instance with deny condition of the privilege. */
    public static CreateTable deny() {
      return DENY_INSTANCE;
    }

    /** @return A readable string representation for the privilege. */
    @Override
    public String simpleString() {
      return condition().name() + " create table";
    }
  }

  /** The privilege to drop a table. */
  public static class DropTable extends BasePrivilege {

    private static final DropTable ALLOW_INSTANCE = new DropTable(Condition.ALLOW);

    private static final DropTable DENY_INSTANCE = new DropTable(Condition.DENY);

    private DropTable(Condition condition) {
      super(DROP_TABLE, condition);
    }

    /** @return The instance with allow condition of the privilege. */
    public static DropTable allow() {
      return ALLOW_INSTANCE;
    }

    /** @return The instance with deny condition of the privilege. */
    public static DropTable deny() {
      return DENY_INSTANCE;
    }

    /** @return A readable string representation for the privilege. */
    @Override
    public String simpleString() {
      return condition().name() + " drop table";
    }
  }

  /** The privilege to read a table. */
  public static class ReadTable extends BasePrivilege {

    private static final ReadTable ALLOW_INSTANCE = new ReadTable(Condition.ALLOW);

    private static final ReadTable DENY_INSTANCE = new ReadTable(Condition.DENY);

    private ReadTable(Condition condition) {
      super(READ_TABLE, condition);
    }

    /** @return The instance with allow condition of the privilege. */
    public static ReadTable allow() {
      return ALLOW_INSTANCE;
    }

    /** @return The instance with deny condition of the privilege. */
    public static ReadTable deny() {
      return DENY_INSTANCE;
    }

    /** @return A readable string representation for the privilege. */
    @Override
    public String simpleString() {
      return condition().name() + " read table";
    }
  }

  /** The privilege to write a table. */
  public static class WriteTable extends BasePrivilege {

    private static final WriteTable ALLOW_INSTANCE = new WriteTable(Condition.ALLOW);

    private static final WriteTable DENY_INSTANCE = new WriteTable(Condition.DENY);

    private WriteTable(Condition condition) {
      super(WRITE_TABLE, condition);
    }

    /** @return The instance with allow condition of the privilege. */
    public static WriteTable allow() {
      return ALLOW_INSTANCE;
    }

    /** @return The instance with deny condition of the privilege. */
    public static WriteTable deny() {
      return DENY_INSTANCE;
    }

    /** @return A readable string representation for the privilege. */
    @Override
    public String simpleString() {
      return condition().name() + " write table";
    }
  }

  /** The privilege to create a fileset. */
  public static class CreateFileset extends BasePrivilege {

    private static final CreateFileset ALLOW_INSTANCE = new CreateFileset(Condition.ALLOW);

    private static final CreateFileset DENY_INSTANCE = new CreateFileset(Condition.DENY);

    private CreateFileset(Condition condition) {
      super(CREATE_FILESET, condition);
    }

    /** @return The instance with allow condition of the privilege. */
    public static CreateFileset allow() {
      return ALLOW_INSTANCE;
    }

    /** @return The instance with deny condition of the privilege. */
    public static CreateFileset deny() {
      return DENY_INSTANCE;
    }

    /** @return A readable string representation for the privilege. */
    @Override
    public String simpleString() {
      return condition().name() + " create fileset";
    }
  }

  /** The privilege to drop a fileset. */
  public static class DropFileset extends BasePrivilege {

    private static final DropFileset ALLOW_INSTANCE = new DropFileset(Condition.ALLOW);

    private static final DropFileset DENY_INSTANCE = new DropFileset(Condition.DENY);

    private DropFileset(Condition condition) {
      super(DROP_FILESET, condition);
    }

    /** @return The instance with allow condition of the privilege. */
    public static DropFileset allow() {
      return ALLOW_INSTANCE;
    }

    /** @return The instance with deny condition of the privilege. */
    public static DropFileset deny() {
      return DENY_INSTANCE;
    }

    /** @return A readable string representation for the privilege. */
    @Override
    public String simpleString() {
      return condition().name() + " drop fileset";
    }
  }

  /** The privilege to read a fileset. */
  public static class ReadFileset extends BasePrivilege {

    private static final ReadFileset ALLOW_INSTANCE = new ReadFileset(Condition.ALLOW);

    private static final ReadFileset DENY_INSTANCE = new ReadFileset(Condition.DENY);

    private ReadFileset(Condition condition) {
      super(READ_FILESET, condition);
    }

    /** @return The instance with allow condition of the privilege. */
    public static ReadFileset allow() {
      return ALLOW_INSTANCE;
    }

    /** @return The instance with deny condition of the privilege. */
    public static ReadFileset deny() {
      return DENY_INSTANCE;
    }

    /** @return A readable string representation for the privilege. */
    @Override
    public String simpleString() {
      return condition().name() + " read fileset";
    }
  }

  /** The privilege to write a fileset. */
  public static class WriteFileset extends BasePrivilege {

    private static final WriteFileset ALLOW_INSTANCE = new WriteFileset(Condition.ALLOW);

    private static final WriteFileset DENY_INSTANCE = new WriteFileset(Condition.DENY);

    private WriteFileset(Condition condition) {
      super(WRITE_FILESET, condition);
    }

    /** @return The instance with allow condition of the privilege. */
    public static WriteFileset allow() {
      return ALLOW_INSTANCE;
    }

    /** @return The instance with deny condition of the privilege. */
    public static WriteFileset deny() {
      return DENY_INSTANCE;
    }

    /** @return A readable string representation for the privilege. */
    @Override
    public String simpleString() {
      return condition().name() + " write fileset";
    }
  }

  /** The privilege to create a topic. */
  public static class CreateTopic extends BasePrivilege {

    private static final CreateTopic ALLOW_INSTANCE = new CreateTopic(Condition.ALLOW);

    private static final CreateTopic DENY_INSTANCE = new CreateTopic(Condition.DENY);

    private CreateTopic(Condition condition) {
      super(CREATE_TOPIC, condition);
    }

    /** @return The instance with allow condition of the privilege. */
    public static CreateTopic allow() {
      return ALLOW_INSTANCE;
    }

    /** @return The instance with deny condition of the privilege. */
    public static CreateTopic deny() {
      return DENY_INSTANCE;
    }

    /** @return A readable string representation for the privilege. */
    @Override
    public String simpleString() {
      return condition().name() + " create topic";
    }
  }

  /** The privilege to drop a topic. */
  public static class DropTopic extends BasePrivilege {

    private static final DropTopic ALLOW_INSTANCE = new DropTopic(Condition.ALLOW);

    private static final DropTopic DENY_INSTANCE = new DropTopic(Condition.DENY);

    private DropTopic(Condition condition) {
      super(DROP_TOPIC, condition);
    }

    /** @return The instance with allow condition of the privilege. */
    public static DropTopic allow() {
      return ALLOW_INSTANCE;
    }

    /** @return The instance with deny condition of the privilege. */
    public static DropTopic deny() {
      return DENY_INSTANCE;
    }

    /** @return A readable string representation for the privilege. */
    @Override
    public String simpleString() {
      return condition().name() + " drop topic";
    }
  }

  /** The privilege to read a topic. */
  public static class ReadTopic extends BasePrivilege {

    private static final ReadTopic ALLOW_INSTANCE = new ReadTopic(Condition.ALLOW);

    private static final ReadTopic DENY_INSTANCE = new ReadTopic(Condition.DENY);

    private ReadTopic(Condition condition) {
      super(READ_TOPIC, condition);
    }

    /** @return The instance with allow condition of the privilege. */
    public static ReadTopic allow() {
      return ALLOW_INSTANCE;
    }

    /** @return The instance with deny condition of the privilege. */
    public static ReadTopic deny() {
      return DENY_INSTANCE;
    }

    /** @return A readable string representation for the privilege. */
    @Override
    public String simpleString() {
      return condition().name() + " read topic";
    }
  }

  /** The privilege to write a topic. */
  public static class WriteTopic extends BasePrivilege {

    private static final WriteTopic ALLOW_INSTANCE = new WriteTopic(Condition.ALLOW);

    private static final WriteTopic DENY_INSTANCE = new WriteTopic(Condition.DENY);

    private WriteTopic(Condition condition) {
      super(WRITE_TOPIC, condition);
    }

    /** @return The instance with allow condition of the privilege. */
    public static WriteTopic allow() {
      return ALLOW_INSTANCE;
    }

    /** @return The instance with deny condition of the privilege. */
    public static WriteTopic deny() {
      return DENY_INSTANCE;
    }

    /** @return A readable string representation for the privilege. */
    @Override
    public String simpleString() {
      return condition().name() + " write topic";
    }
  }

  /** The privilege to manage a metalake. */
  public static class ManageMetalake extends BasePrivilege {

    private static final ManageMetalake ALLOW_INSTANCE = new ManageMetalake(Condition.ALLOW);

    private static final ManageMetalake DENY_INSTANCE = new ManageMetalake(Condition.DENY);

    private ManageMetalake(Condition condition) {
      super(MANAGE_METALAKE, condition);
    }

    /** @return The instance with allow condition of the privilege. */
    public static ManageMetalake allow() {
      return ALLOW_INSTANCE;
    }

    /** @return The instance with deny condition of the privilege. */
    public static ManageMetalake deny() {
      return DENY_INSTANCE;
    }

    /** @return A readable string representation for the privilege. */
    @Override
    public String simpleString() {
      return condition().name() + " manage metalake";
    }
  }

  /** The privilege to manage a metalake. */
  public static class CreateMetalake extends BasePrivilege {

    private static final CreateMetalake ALLOW_INSTANCE = new CreateMetalake(Condition.ALLOW);

    private static final CreateMetalake DENY_INSTANCE = new CreateMetalake(Condition.DENY);

    private CreateMetalake(Condition condition) {
      super(CREATE_METALAKE, condition);
    }

    /** @return The instance with allow condition of the privilege. */
    public static CreateMetalake allow() {
      return ALLOW_INSTANCE;
    }

    /** @return The instance with deny condition of the privilege. */
    public static CreateMetalake deny() {
      return DENY_INSTANCE;
    }

    /** @return A readable string representation for the privilege. */
    @Override
    public String simpleString() {
      return condition().name() + " create metalake";
    }
  }

  /** The privilege to use a metalake. */
  public static class UseMetalake extends BasePrivilege {

    private static final UseMetalake ALLOW_INSTANCE = new UseMetalake(Condition.ALLOW);

    private static final UseMetalake DENY_INSTANCE = new UseMetalake(Condition.DENY);

    private UseMetalake(Condition condition) {
      super(USE_METALAKE, condition);
    }

    /** @return The instance with allow condition of the privilege. */
    public static UseMetalake allow() {
      return ALLOW_INSTANCE;
    }

    /** @return The instance with deny condition of the privilege. */
    public static UseMetalake deny() {
      return DENY_INSTANCE;
    }

    /** @return A readable string representation for the privilege. */
    @Override
    public String simpleString() {
      return condition().name() + " use metalake";
    }
  }

  /** The privilege to get a user. */
  public static class GetUser extends BasePrivilege {

    private static final GetUser ALLOW_INSTANCE = new GetUser(Condition.ALLOW);

    private static final GetUser DENY_INSTANCE = new GetUser(Condition.DENY);

    private GetUser(Condition condition) {
      super(GET_USER, condition);
    }

    /** @return The instance with allow condition of the privilege. */
    public static GetUser allow() {
      return ALLOW_INSTANCE;
    }

    /** @return The instance with deny condition of the privilege. */
    public static GetUser deny() {
      return DENY_INSTANCE;
    }

    /** @return A readable string representation for the privilege. */
    @Override
    public String simpleString() {
      return condition().name() + " get user";
    }
  }

  /** The privilege to add a user. */
  public static class AddUser extends BasePrivilege {

    private static final AddUser ALLOW_INSTANCE = new AddUser(Condition.ALLOW);

    private static final AddUser DENY_INSTANCE = new AddUser(Condition.DENY);

    private AddUser(Condition condition) {
      super(ADD_USER, condition);
    }

    /** @return The instance with allow condition of the privilege. */
    public static AddUser allow() {
      return ALLOW_INSTANCE;
    }

    /** @return The instance with deny condition of the privilege. */
    public static AddUser deny() {
      return DENY_INSTANCE;
    }

    /** @return A readable string representation for the privilege. */
    @Override
    public String simpleString() {
      return condition().name() + " add user";
    }
  }

  /** The privilege to remove a user. */
  public static class RemoveUser extends BasePrivilege {

    private static final RemoveUser ALLOW_INSTANCE = new RemoveUser(Condition.ALLOW);

    private static final RemoveUser DENY_INSTANCE = new RemoveUser(Condition.DENY);

    private RemoveUser(Condition condition) {
      super(REMOVE_USER, condition);
    }

    /** @return The instance with allow condition of the privilege. */
    public static RemoveUser allow() {
      return ALLOW_INSTANCE;
    }

    /** @return The instance with deny condition of the privilege. */
    public static RemoveUser deny() {
      return DENY_INSTANCE;
    }

    /** @return A readable string representation for the privilege. */
    @Override
    public String simpleString() {
      return condition().name() + " remove user";
    }
  }

  /** The privilege to add a group. */
  public static class AddGroup extends BasePrivilege {

    private static final AddGroup ALLOW_INSTANCE = new AddGroup(Condition.ALLOW);

    private static final AddGroup DENY_INSTANCE = new AddGroup(Condition.DENY);

    private AddGroup(Condition condition) {
      super(ADD_GROUP, condition);
    }

    /** @return The instance with allow condition of the privilege. */
    public static AddGroup allow() {
      return ALLOW_INSTANCE;
    }

    /** @return The instance with deny condition of the privilege. */
    public static AddGroup deny() {
      return DENY_INSTANCE;
    }

    /** @return A readable string representation for the privilege. */
    @Override
    public String simpleString() {
      return condition().name() + " add group";
    }
  }

  /** The privilege to remove a group. */
  public static class RemoveGroup extends BasePrivilege {

    private static final RemoveGroup ALLOW_INSTANCE = new RemoveGroup(Condition.ALLOW);

    private static final RemoveGroup DENY_INSTANCE = new RemoveGroup(Condition.DENY);

    private RemoveGroup(Condition condition) {
      super(REMOVE_GROUP, condition);
    }

    /** @return The instance with allow condition of the privilege. */
    public static RemoveGroup allow() {
      return ALLOW_INSTANCE;
    }

    /** @return The instance with deny condition of the privilege. */
    public static RemoveGroup deny() {
      return DENY_INSTANCE;
    }

    /** @return A readable string representation for the privilege. */
    @Override
    public String simpleString() {
      return condition().name() + " remove group";
    }
  }

  /** The privilege to get a group. */
  public static class GetGroup extends BasePrivilege {

    private static final GetGroup ALLOW_INSTANCE = new GetGroup(Condition.DENY);

    private static final GetGroup DENY_INSTANCE = new GetGroup(Condition.DENY);

    private GetGroup(Condition condition) {
      super(GET_GROUP, condition);
    }

    /** @return The instance with allow condition of the privilege. */
    public static GetGroup allow() {
      return ALLOW_INSTANCE;
    }

    /** @return The instance with deny condition of the privilege. */
    public static GetGroup deny() {
      return DENY_INSTANCE;
    }

    /** @return A readable string representation for the privilege. */
    @Override
    public String simpleString() {
      return condition().name() + " get group";
    }
  }

  /** The privilege to create a role. */
  public static class CreateRole extends BasePrivilege {

    private static final CreateRole ALLOW_INSTANCE = new CreateRole(Condition.ALLOW);

    private static final CreateRole DENY_INSTANCE = new CreateRole(Condition.DENY);

    private CreateRole(Condition condition) {
      super(CREATE_ROLE, condition);
    }

    /** @return The instance with allow condition of the privilege. */
    public static CreateRole allow() {
      return ALLOW_INSTANCE;
    }

    /** @return The instance with deny condition of the privilege. */
    public static CreateRole deny() {
      return DENY_INSTANCE;
    }

    /** @return A readable string representation for the privilege. */
    @Override
    public String simpleString() {
      return condition().name() + " create role";
    }
  }

  /** The privilege to get a role. */
  public static class GetRole extends BasePrivilege {

    private static final GetRole ALLOW_INSTANCE = new GetRole(Condition.ALLOW);

    private static final GetRole DENY_INSTANCE = new GetRole(Condition.DENY);

    private GetRole(Condition condition) {
      super(GET_ROLE, condition);
    }

    /** @return The instance with allow condition of the privilege. */
    public static GetRole allow() {
      return ALLOW_INSTANCE;
    }

    /** @return The instance with deny condition of the privilege. */
    public static GetRole deny() {
      return DENY_INSTANCE;
    }

    /** @return A readable string representation for the privilege. */
    @Override
    public String simpleString() {
      return condition().name() + " get role";
    }
  }

  /** The privilege to delete a role. */
  public static class DeleteRole extends BasePrivilege {

    private static final DeleteRole ALLOW_INSTANCE = new DeleteRole(Condition.ALLOW);

    private static final DeleteRole DENY_INSTANCE = new DeleteRole(Condition.DENY);

    private DeleteRole(Condition condition) {
      super(DELETE_ROLE, condition);
    }

    /** @return The instance with allow condition of the privilege. */
    public static DeleteRole allow() {
      return ALLOW_INSTANCE;
    }

    /** @return The instance with deny condition of the privilege. */
    public static DeleteRole deny() {
      return DENY_INSTANCE;
    }

    /** @return A readable string representation for the privilege. */
    @Override
    public String simpleString() {
      return condition().name() + " delete role";
    }
  }

  /** The privilege to grant a role to the user or the group. */
  public static class GrantRole extends BasePrivilege {

    private static final GrantRole ALLOW_INSTANCE = new GrantRole(Condition.ALLOW);

    private static final GrantRole DENY_INSTANCE = new GrantRole(Condition.DENY);

    private GrantRole(Condition condition) {
      super(GRANT_ROLE, condition);
    }

    /** @return The instance with allow condition of the privilege. */
    public static GrantRole allow() {
      return ALLOW_INSTANCE;
    }

    /** @return The instance with deny condition of the privilege. */
    public static GrantRole deny() {
      return DENY_INSTANCE;
    }

    /** @return A readable string representation for the privilege. */
    @Override
    public String simpleString() {
      return condition().name() + " grant role";
    }
  }

  /** The privilege to revoke a role from the user or the group. */
  public static class RevokeRole extends BasePrivilege {

    private static final RevokeRole ALLOW_INSTANCE = new RevokeRole(Condition.ALLOW);

    private static final RevokeRole DENY_INSTANCE = new RevokeRole(Condition.DENY);

    private RevokeRole(Condition condition) {
      super(REVOKE_ROLE, condition);
    }

    /** @return The instance with allow condition of the privilege. */
    public static RevokeRole allow() {
      return ALLOW_INSTANCE;
    }

    /** @return The instance with deny condition of the privilege. */
    public static RevokeRole deny() {
      return DENY_INSTANCE;
    }

    /** @return A readable string representation for the privilege. */
    @Override
    public String simpleString() {
      return condition().name() + " revoke role";
    }
  }

  private abstract static class BasePrivilege implements Privilege {
    private final Name name;
    private final Condition condition;

    protected BasePrivilege(Name name, Condition condition) {
      this.name = name;
      this.condition = condition;
    }

    @Override
    public Name name() {
      return name;
    }

    @Override
    public Condition condition() {
      return condition;
    }
  }
}
