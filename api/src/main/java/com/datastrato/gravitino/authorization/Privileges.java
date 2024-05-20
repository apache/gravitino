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
   * Returns the Privilege from the string representation.
   *
   * @param privilege The string representation of the privilege.
   * @return The Privilege.
   */
  public static Privilege fromString(String privilege) {
    Privilege.Name name = Privilege.Name.valueOf(privilege);
    return fromName(name);
  }

  /**
   * Returns the Privilege from the `Privilege.Name`.
   *
   * @param name The `Privilege.Name` of the privilege.
   * @return The Privilege.
   */
  public static Privilege fromName(Privilege.Name name) {
    switch (name) {
        // Catalog
      case CREATE_CATALOG:
        return CreateCatalog.get();
      case DROP_CATALOG:
        return DropCatalog.get();
      case ALTER_CATALOG:
        return AlterCatalog.get();
      case USE_CATALOG:
        return UseCatalog.get();

        // Schema
      case CREATE_SCHEMA:
        return CreateSchema.get();
      case DROP_SCHEMA:
        return DropSchema.get();
      case ALTER_SCHEMA:
        return AlterSchema.get();
      case USE_SCHEMA:
        return UseSchema.get();

        // Table
      case CREATE_TABLE:
        return CreateTable.get();
      case DROP_TABLE:
        return DropTable.get();
      case WRITE_TABLE:
        return WriteTable.get();
      case READ_TABLE:
        return ReadTable.get();

        // Fileset
      case CREATE_FILESET:
        return CreateFileset.get();
      case DROP_FILESET:
        return DropFileset.get();
      case WRITE_FILESET:
        return WriteFileset.get();
      case READ_FILESET:
        return ReadFileset.get();

        // Topic
      case CREATE_TOPIC:
        return CreateTopic.get();
      case DROP_TOPIC:
        return DropTopic.get();
      case WRITE_TOPIC:
        return WriteTopic.get();
      case READ_TOPIC:
        return ReadTopic.get();

        // Metalake
      case CREATE_METALAKE:
        return CreateMetalake.get();
      case MANAGE_METALAKE:
        return ManageMetalake.get();
      case USE_METALAKE:
        return UseMetalake.get();

        // User
      case ADD_USER:
        return AddUser.get();
      case REMOVE_USER:
        return RemoveUser.get();
      case GET_USER:
        return GetUser.get();

        // Group
      case ADD_GROUP:
        return AddGroup.get();
      case REMOVE_GROUP:
        return RemoveGroup.get();
      case GET_GROUP:
        return GetGroup.get();

        // Role
      case CREATE_ROLE:
        return CreateRole.get();
      case DELETE_ROLE:
        return DeleteRole.get();
      case GRANT_ROLE:
        return GrantRole.get();
      case REVOKE_ROLE:
        return RevokeRole.get();
      case GET_ROLE:
        return GetRole.get();

      default:
        throw new IllegalArgumentException("Don't support the privilege: " + name);
    }
  }

  /** The privilege to create a catalog. */
  public static class CreateCatalog implements Privilege {

    private static final CreateCatalog INSTANCE = new CreateCatalog();

    private CreateCatalog() {}

    /** @return The instance of the privilege. */
    public static CreateCatalog get() {
      return INSTANCE;
    }

    /** @return The generic name of the privilege. */
    @Override
    public Name name() {
      return CREATE_CATALOG;
    }

    /** @return A readable string representation for the privilege. */
    @Override
    public String simpleString() {
      return "create catalog";
    }
  }

  /** The privilege to alter a catalog. */
  public static class AlterCatalog implements Privilege {

    private static final AlterCatalog INSTANCE = new AlterCatalog();

    private AlterCatalog() {}

    /** @return The instance of the privilege. */
    public static AlterCatalog get() {
      return INSTANCE;
    }

    /** @return The generic name of the privilege. */
    @Override
    public Name name() {
      return ALTER_CATALOG;
    }

    /** @return A readable string representation for the privilege. */
    @Override
    public String simpleString() {
      return "alter catalog";
    }
  }

  /** The privilege to drop a catalog. */
  public static class DropCatalog implements Privilege {

    private static final DropCatalog INSTANCE = new DropCatalog();

    private DropCatalog() {}

    /** @return The instance of the privilege. */
    public static DropCatalog get() {
      return INSTANCE;
    }

    /** @return The generic name of the privilege. */
    @Override
    public Name name() {
      return DROP_CATALOG;
    }

    /** @return A readable string representation for the privilege. */
    @Override
    public String simpleString() {
      return "drop catalog";
    }
  }

  /** The privilege to use a catalog. */
  public static class UseCatalog implements Privilege {
    private static final UseCatalog INSTANCE = new UseCatalog();

    /** @return The instance of the privilege. */
    public static UseCatalog get() {
      return INSTANCE;
    }

    private UseCatalog() {}

    /** @return The generic name of the privilege. */
    @Override
    public Name name() {
      return USE_CATALOG;
    }

    /** @return A readable string representation for the privilege. */
    @Override
    public String simpleString() {
      return "use catalog";
    }
  }

  /** The privilege to use a schema. */
  public static class UseSchema implements Privilege {

    private static final UseSchema INSTANCE = new UseSchema();

    /** @return The instance of the privilege. */
    public static UseSchema get() {
      return INSTANCE;
    }

    /** @return The generic name of the privilege. */
    @Override
    public Name name() {
      return USE_SCHEMA;
    }

    /** @return A readable string representation for the privilege. */
    @Override
    public String simpleString() {
      return "use schema";
    }
  }

  /** The privilege to create a schema. */
  public static class CreateSchema implements Privilege {

    private static final CreateSchema INSTANCE = new CreateSchema();

    /** @return The instance of the privilege. */
    public static CreateSchema get() {
      return INSTANCE;
    }

    /** @return The generic name of the privilege. */
    @Override
    public Name name() {
      return CREATE_SCHEMA;
    }

    /** @return A readable string representation for the privilege. */
    @Override
    public String simpleString() {
      return "create schema";
    }
  }

  /** The privilege to alter a schema. */
  public static class AlterSchema implements Privilege {

    private static final AlterSchema INSTANCE = new AlterSchema();

    /** @return The instance of the privilege. */
    public static AlterSchema get() {
      return INSTANCE;
    }

    /** @return The generic name of the privilege. */
    @Override
    public Name name() {
      return ALTER_SCHEMA;
    }

    /** @return A readable string representation for the privilege. */
    @Override
    public String simpleString() {
      return "alter schema";
    }
  }

  /** The privilege to drop a schema. */
  public static class DropSchema implements Privilege {

    private static final DropSchema INSTANCE = new DropSchema();

    /** @return The instance of the privilege. */
    public static DropSchema get() {
      return INSTANCE;
    }

    /** @return The generic name of the privilege. */
    @Override
    public Name name() {
      return DROP_SCHEMA;
    }

    /** @return A readable string representation for the privilege. */
    @Override
    public String simpleString() {
      return "drop schema";
    }
  }

  /** The privilege to create a table. */
  public static class CreateTable implements Privilege {

    private static final CreateTable INSTANCE = new CreateTable();

    private CreateTable() {}

    /** @return The instance of the privilege. */
    public static CreateTable get() {
      return INSTANCE;
    }

    /** @return The generic name of the privilege. */
    @Override
    public Name name() {
      return CREATE_TABLE;
    }

    /** @return A readable string representation for the privilege. */
    @Override
    public String simpleString() {
      return "create table";
    }
  }

  /** The privilege to drop a table. */
  public static class DropTable implements Privilege {

    private static final DropTable INSTANCE = new DropTable();

    private DropTable() {}

    /** @return The instance of the privilege. */
    public static DropTable get() {
      return INSTANCE;
    }

    /** @return The generic name of the privilege. */
    @Override
    public Name name() {
      return DROP_TABLE;
    }

    /** @return A readable string representation for the privilege. */
    @Override
    public String simpleString() {
      return "drop table";
    }
  }

  /** The privilege to read a table. */
  public static class ReadTable implements Privilege {

    private static final ReadTable INSTANCE = new ReadTable();

    private ReadTable() {}

    /** @return The instance of the privilege. */
    public static ReadTable get() {
      return INSTANCE;
    }

    /** @return The generic name of the privilege. */
    @Override
    public Name name() {
      return READ_TABLE;
    }

    /** @return A readable string representation for the privilege. */
    @Override
    public String simpleString() {
      return "read table";
    }
  }

  /** The privilege to write a table. */
  public static class WriteTable implements Privilege {

    private static final WriteTable INSTANCE = new WriteTable();

    private WriteTable() {}

    /** @return The instance of the privilege. */
    public static WriteTable get() {
      return INSTANCE;
    }

    /** @return The generic name of the privilege. */
    @Override
    public Name name() {
      return WRITE_TABLE;
    }

    /** @return A readable string representation for the privilege. */
    @Override
    public String simpleString() {
      return "write table";
    }
  }

  /** The privilege to create a fileset. */
  public static class CreateFileset implements Privilege {

    private static final CreateFileset INSTANCE = new CreateFileset();

    private CreateFileset() {}

    /** @return The instance of the privilege. */
    public static CreateFileset get() {
      return INSTANCE;
    }

    /** @return The generic name of the privilege. */
    @Override
    public Name name() {
      return CREATE_FILESET;
    }

    /** @return A readable string representation for the privilege. */
    @Override
    public String simpleString() {
      return "create fileset";
    }
  }

  /** The privilege to drop a fileset. */
  public static class DropFileset implements Privilege {

    private static final DropFileset INSTANCE = new DropFileset();

    private DropFileset() {}

    /** @return The instance of the privilege. */
    public static DropFileset get() {
      return INSTANCE;
    }

    /** @return The generic name of the privilege. */
    @Override
    public Name name() {
      return DROP_FILESET;
    }

    /** @return A readable string representation for the privilege. */
    @Override
    public String simpleString() {
      return "drop fileset";
    }
  }

  /** The privilege to read a fileset. */
  public static class ReadFileset implements Privilege {

    private static final ReadFileset INSTANCE = new ReadFileset();

    private ReadFileset() {}

    /** @return The instance of the privilege. */
    public static ReadFileset get() {
      return INSTANCE;
    }

    /** @return The generic name of the privilege. */
    @Override
    public Name name() {
      return READ_FILESET;
    }

    /** @return A readable string representation for the privilege. */
    @Override
    public String simpleString() {
      return "read fileset";
    }
  }

  /** The privilege to write a fileset. */
  public static class WriteFileset implements Privilege {

    private static final WriteFileset INSTANCE = new WriteFileset();

    private WriteFileset() {}

    /** @return The instance of the privilege. */
    public static WriteFileset get() {
      return INSTANCE;
    }

    /** @return The generic name of the privilege. */
    @Override
    public Name name() {
      return WRITE_FILESET;
    }

    /** @return A readable string representation for the privilege. */
    @Override
    public String simpleString() {
      return "write fileset";
    }
  }

  /** The privilege to create a topic. */
  public static class CreateTopic implements Privilege {

    private static final CreateTopic INSTANCE = new CreateTopic();

    private CreateTopic() {}

    /** @return The instance of the privilege. */
    public static CreateTopic get() {
      return INSTANCE;
    }

    /** @return The generic name of the privilege. */
    @Override
    public Name name() {
      return CREATE_TOPIC;
    }

    /** @return A readable string representation for the privilege. */
    @Override
    public String simpleString() {
      return "create topic";
    }
  }

  /** The privilege to drop a topic. */
  public static class DropTopic implements Privilege {

    private static final DropTopic INSTANCE = new DropTopic();

    private DropTopic() {}

    /** @return The instance of the privilege. */
    public static DropTopic get() {
      return INSTANCE;
    }

    /** @return The generic name of the privilege. */
    @Override
    public Name name() {
      return DROP_TOPIC;
    }

    /** @return A readable string representation for the privilege. */
    @Override
    public String simpleString() {
      return "drop topic";
    }
  }

  /** The privilege to read a topic. */
  public static class ReadTopic implements Privilege {

    private static final ReadTopic INSTANCE = new ReadTopic();

    private ReadTopic() {}

    /** @return The instance of the privilege. */
    public static ReadTopic get() {
      return INSTANCE;
    }

    /** @return The generic name of the privilege. */
    @Override
    public Name name() {
      return READ_TOPIC;
    }

    /** @return A readable string representation for the privilege. */
    @Override
    public String simpleString() {
      return "read topic";
    }
  }

  /** The privilege to write a topic. */
  public static class WriteTopic implements Privilege {

    private static final WriteTopic INSTANCE = new WriteTopic();

    private WriteTopic() {}

    /** @return The instance of the privilege. */
    public static WriteTopic get() {
      return INSTANCE;
    }

    /** @return The generic name of the privilege. */
    @Override
    public Name name() {
      return WRITE_TOPIC;
    }

    /** @return A readable string representation for the privilege. */
    @Override
    public String simpleString() {
      return "write topic";
    }
  }

  /** The privilege to manage a metalake. */
  public static class ManageMetalake implements Privilege {

    private static final ManageMetalake INSTANCE = new ManageMetalake();

    private ManageMetalake() {}

    /** @return The instance of the privilege. */
    public static ManageMetalake get() {
      return INSTANCE;
    }

    /** @return The generic name of the privilege. */
    @Override
    public Name name() {
      return MANAGE_METALAKE;
    }

    /** @return A readable string representation for the privilege. */
    @Override
    public String simpleString() {
      return "manage metalake";
    }
  }

  /** The privilege to manage a metalake. */
  public static class CreateMetalake implements Privilege {

    private static final CreateMetalake INSTANCE = new CreateMetalake();

    private CreateMetalake() {}

    /** @return The instance of the privilege. */
    public static CreateMetalake get() {
      return INSTANCE;
    }

    /** @return The generic name of the privilege. */
    @Override
    public Name name() {
      return CREATE_METALAKE;
    }

    /** @return A readable string representation for the privilege. */
    @Override
    public String simpleString() {
      return "create metalake";
    }
  }

  /** The privilege to use a metalake. */
  public static class UseMetalake implements Privilege {

    private static final UseMetalake INSTANCE = new UseMetalake();

    private UseMetalake() {}

    /** @return The instance of the privilege. */
    public static UseMetalake get() {
      return INSTANCE;
    }

    /** @return The generic name of the privilege. */
    @Override
    public Name name() {
      return USE_METALAKE;
    }

    /** @return A readable string representation for the privilege. */
    @Override
    public String simpleString() {
      return "use metalake";
    }
  }

  /** The privilege to get a user. */
  public static class GetUser implements Privilege {

    private static final GetUser INSTANCE = new GetUser();

    private GetUser() {}

    /** @return The instance of the privilege. */
    public static GetUser get() {
      return INSTANCE;
    }

    /** @return The generic name of the privilege. */
    @Override
    public Name name() {
      return GET_USER;
    }

    /** @return A readable string representation for the privilege. */
    @Override
    public String simpleString() {
      return "get user";
    }
  }

  /** The privilege to add a user. */
  public static class AddUser implements Privilege {

    private static final AddUser INSTANCE = new AddUser();

    private AddUser() {}

    /** @return The instance of the privilege. */
    public static AddUser get() {
      return INSTANCE;
    }

    /** @return The generic name of the privilege. */
    @Override
    public Name name() {
      return ADD_USER;
    }

    /** @return A readable string representation for the privilege. */
    @Override
    public String simpleString() {
      return "add user";
    }
  }

  /** The privilege to remove a user. */
  public static class RemoveUser implements Privilege {

    private static final RemoveUser INSTANCE = new RemoveUser();

    private RemoveUser() {}

    /** @return The instance of the privilege. */
    public static RemoveUser get() {
      return INSTANCE;
    }

    /** @return The generic name of the privilege. */
    @Override
    public Name name() {
      return REMOVE_USER;
    }

    /** @return A readable string representation for the privilege. */
    @Override
    public String simpleString() {
      return "remove user";
    }
  }

  /** The privilege to add a group. */
  public static class AddGroup implements Privilege {

    private static final AddGroup INSTANCE = new AddGroup();

    private AddGroup() {}

    /** @return The instance of the privilege. */
    public static AddGroup get() {
      return INSTANCE;
    }

    /** @return The generic name of the privilege. */
    @Override
    public Name name() {
      return ADD_GROUP;
    }

    /** @return A readable string representation for the privilege. */
    @Override
    public String simpleString() {
      return "add group";
    }
  }

  /** The privilege to remove a group. */
  public static class RemoveGroup implements Privilege {

    private static final RemoveGroup INSTANCE = new RemoveGroup();

    private RemoveGroup() {}

    /** @return The instance of the privilege. */
    public static RemoveGroup get() {
      return INSTANCE;
    }

    /** @return The generic name of the privilege. */
    @Override
    public Name name() {
      return REMOVE_GROUP;
    }

    /** @return A readable string representation for the privilege. */
    @Override
    public String simpleString() {
      return "remove group";
    }
  }

  /** The privilege to get a group. */
  public static class GetGroup implements Privilege {

    private static final GetGroup INSTANCE = new GetGroup();

    private GetGroup() {}

    /** @return The instance of the privilege. */
    public static GetGroup get() {
      return INSTANCE;
    }

    /** @return The generic name of the privilege. */
    @Override
    public Name name() {
      return GET_GROUP;
    }

    /** @return A readable string representation for the privilege. */
    @Override
    public String simpleString() {
      return "get group";
    }
  }

  /** The privilege to create a role. */
  public static class CreateRole implements Privilege {

    private static final CreateRole INSTANCE = new CreateRole();

    private CreateRole() {}

    /** @return The instance of the privilege. */
    public static CreateRole get() {
      return INSTANCE;
    }

    /** @return The generic name of the privilege. */
    @Override
    public Name name() {
      return CREATE_ROLE;
    }

    /** @return A readable string representation for the privilege. */
    @Override
    public String simpleString() {
      return "create role";
    }
  }

  /** The privilege to get a role. */
  public static class GetRole implements Privilege {

    private static final GetRole INSTANCE = new GetRole();

    private GetRole() {}

    /** @return The instance of the privilege. */
    public static GetRole get() {
      return INSTANCE;
    }

    /** @return The generic name of the privilege. */
    @Override
    public Name name() {
      return GET_ROLE;
    }

    /** @return A readable string representation for the privilege. */
    @Override
    public String simpleString() {
      return "get role";
    }
  }

  /** The privilege to delete a role. */
  public static class DeleteRole implements Privilege {

    private static final DeleteRole INSTANCE = new DeleteRole();

    private DeleteRole() {}

    /** @return The instance of the privilege. */
    public static DeleteRole get() {
      return INSTANCE;
    }

    /** @return The generic name of the privilege. */
    @Override
    public Name name() {
      return DELETE_ROLE;
    }

    /** @return A readable string representation for the privilege. */
    @Override
    public String simpleString() {
      return "delete role";
    }
  }

  /** The privilege to grant a role to the user or the group. */
  public static class GrantRole implements Privilege {

    private static final GrantRole INSTANCE = new GrantRole();

    private GrantRole() {}

    /** @return The instance of the privilege. */
    public static GrantRole get() {
      return INSTANCE;
    }

    /** @return The generic name of the privilege. */
    @Override
    public Name name() {
      return GRANT_ROLE;
    }

    /** @return A readable string representation for the privilege. */
    @Override
    public String simpleString() {
      return "grant role";
    }
  }

  /** The privilege to revoke a role from the user or the group. */
  public static class RevokeRole implements Privilege {

    private static final RevokeRole INSTANCE = new RevokeRole();

    private RevokeRole() {}

    /** @return The instance of the privilege. */
    public static RevokeRole get() {
      return INSTANCE;
    }

    /** @return The generic name of the privilege. */
    @Override
    public Name name() {
      return REVOKE_ROLE;
    }

    /** @return A readable string representation for the privilege. */
    @Override
    public String simpleString() {
      return "revoke role";
    }
  }
}
