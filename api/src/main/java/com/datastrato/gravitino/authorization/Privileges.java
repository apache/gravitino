/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.authorization;

import static com.datastrato.gravitino.authorization.Privilege.Name.ALTER_CATALOG;
import static com.datastrato.gravitino.authorization.Privilege.Name.ALTER_SCHEMA;
import static com.datastrato.gravitino.authorization.Privilege.Name.CREATE_CATALOG;
import static com.datastrato.gravitino.authorization.Privilege.Name.CREATE_FILESET;
import static com.datastrato.gravitino.authorization.Privilege.Name.CREATE_METALAKE;
import static com.datastrato.gravitino.authorization.Privilege.Name.CREATE_SCHEMA;
import static com.datastrato.gravitino.authorization.Privilege.Name.CREATE_TABLE;
import static com.datastrato.gravitino.authorization.Privilege.Name.CREATE_TOPIC;
import static com.datastrato.gravitino.authorization.Privilege.Name.DROP_CATALOG;
import static com.datastrato.gravitino.authorization.Privilege.Name.DROP_FILESET;
import static com.datastrato.gravitino.authorization.Privilege.Name.DROP_SCHEMA;
import static com.datastrato.gravitino.authorization.Privilege.Name.DROP_TABLE;
import static com.datastrato.gravitino.authorization.Privilege.Name.DROP_TOPIC;
import static com.datastrato.gravitino.authorization.Privilege.Name.LIST_FILESET;
import static com.datastrato.gravitino.authorization.Privilege.Name.LIST_TOPIC;
import static com.datastrato.gravitino.authorization.Privilege.Name.MANAGE_GROUP;
import static com.datastrato.gravitino.authorization.Privilege.Name.MANAGE_METALAKE;
import static com.datastrato.gravitino.authorization.Privilege.Name.MANAGE_ROLE;
import static com.datastrato.gravitino.authorization.Privilege.Name.MANAGE_USER;
import static com.datastrato.gravitino.authorization.Privilege.Name.READ_FILESET;
import static com.datastrato.gravitino.authorization.Privilege.Name.READ_TABLE;
import static com.datastrato.gravitino.authorization.Privilege.Name.READ_TOPIC;
import static com.datastrato.gravitino.authorization.Privilege.Name.SHOW_CATALOG;
import static com.datastrato.gravitino.authorization.Privilege.Name.SHOW_SCHEMA;
import static com.datastrato.gravitino.authorization.Privilege.Name.SHOW_TABLE;
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
      case SHOW_CATALOG:
        return ShowCatalog.get();
      case USE_CATALOG:
        return UseCatalog.get();
      case CREATE_CATALOG:
        return CreateCatalog.get();
      case ALTER_CATALOG:
        return AlterCatalog.get();
      case DROP_CATALOG:
        return DropCatalog.get();

        // Schema
      case SHOW_SCHEMA:
        return ShowSchema.get();
      case USE_SCHEMA:
        return UseSchema.get();
      case CREATE_SCHEMA:
        return CreateSchema.get();
      case ALTER_SCHEMA:
        return AlterSchema.get();
      case DROP_SCHEMA:
        return DropSchema.get();

        // Table
      case SHOW_TABLE:
        return ShowTable.get();
      case CREATE_TABLE:
        return CreateTable.get();
      case DROP_TABLE:
        return DropTable.get();
      case READ_TABLE:
        return ReadTable.get();
      case WRITE_TABLE:
        return WriteTable.get();

        // Fileset
      case LIST_FILESET:
        return ListFileset.get();
      case CREATE_FILESET:
        return CreateFileset.get();
      case DROP_FILESET:
        return DropFileset.get();
      case READ_FILESET:
        return ReadFileset.get();
      case WRITE_FILESET:
        return WriteFileset.get();

        // Topic
      case LIST_TOPIC:
        return ListTopic.get();
      case CREATE_TOPIC:
        return CreateTopic.get();
      case DROP_TOPIC:
        return DropTopic.get();
      case READ_TOPIC:
        return ReadTopic.get();
      case WRITE_TOPIC:
        return WriteTopic.get();

        // Metalake
      case USE_METALAKE:
        return UseMetalake.get();
      case MANAGE_METALAKE:
        return ManageMetalake.get();
      case CREATE_METALAKE:
        return CreateMetalake.get();

        // Access control
      case MANAGE_USER:
        return ManageUser.get();
      case MANAGE_GROUP:
        return ManageGroup.get();
      case MANAGE_ROLE:
        return ManageRole.get();

      default:
        throw new IllegalArgumentException("Don't support the privilege: " + name);
    }
  }

  /** The privilege to show catalogs. */
  public static class ShowCatalog implements Privilege {

    private static final ShowCatalog INSTANCE = new ShowCatalog();

    /** @return The instance of the privilege. */
    public static ShowCatalog get() {
      return INSTANCE;
    }

    /** @return The generic name of the privilege. */
    @Override
    public Name name() {
      return SHOW_CATALOG;
    }

    /** @return A readable string representation for the privilege. */
    @Override
    public String simpleString() {
      return "show catalog";
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

  /** The privilege to show schemas. */
  public static class ShowSchema implements Privilege {

    private static final ShowSchema INSTANCE = new ShowSchema();

    private ShowSchema() {}

    /** @return The instance of the privilege. */
    public static ShowSchema get() {
      return INSTANCE;
    }

    /** @return The generic name of the privilege. */
    @Override
    public Name name() {
      return SHOW_SCHEMA;
    }

    /** @return A readable string representation for the privilege. */
    @Override
    public String simpleString() {
      return "show schema";
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

  /** The privilege to show tables. */
  public static class ShowTable implements Privilege {

    private static final ShowTable INSTANCE = new ShowTable();

    /** @return The instance of the privilege. */
    public static ShowTable get() {
      return INSTANCE;
    }

    /** @return The generic name of the privilege. */
    @Override
    public Name name() {
      return SHOW_TABLE;
    }

    /** @return A readable string representation for the privilege. */
    @Override
    public String simpleString() {
      return "show table";
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

  /** The privilege to list filesets. */
  public static class ListFileset implements Privilege {

    private static final ListFileset INSTANCE = new ListFileset();

    private ListFileset() {}

    /** @return The instance of the privilege. */
    public static ListFileset get() {
      return INSTANCE;
    }

    /** @return The generic name of the privilege. */
    @Override
    public Name name() {
      return LIST_FILESET;
    }

    /** @return A readable string representation for the privilege. */
    @Override
    public String simpleString() {
      return "list fileset";
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

  /** The privilege to list topics. */
  public static class ListTopic implements Privilege {

    private static final ListTopic INSTANCE = new ListTopic();

    private ListTopic() {}

    /** @return The instance of the privilege. */
    public static ListTopic get() {
      return INSTANCE;
    }

    /** @return The generic name of the privilege. */
    @Override
    public Name name() {
      return LIST_TOPIC;
    }

    /** @return A readable string representation for the privilege. */
    @Override
    public String simpleString() {
      return "list topic";
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

  /** The privilege to manage users. */
  public static class ManageUser implements Privilege {

    private static final ManageUser INSTANCE = new ManageUser();

    private ManageUser() {}

    /** @return The instance of the privilege. */
    public static ManageUser get() {
      return INSTANCE;
    }

    /** @return The generic name of the privilege. */
    @Override
    public Name name() {
      return MANAGE_USER;
    }

    /** @return A readable string representation for the privilege. */
    @Override
    public String simpleString() {
      return "manage user";
    }
  }

  /** The privilege to manage roles. */
  public static class ManageGroup implements Privilege {

    private static final ManageGroup INSTANCE = new ManageGroup();

    private ManageGroup() {}

    /** @return The instance of the privilege. */
    public static ManageGroup get() {
      return INSTANCE;
    }

    /** @return The generic name of the privilege. */
    @Override
    public Name name() {
      return MANAGE_GROUP;
    }

    /** @return A readable string representation for the privilege. */
    @Override
    public String simpleString() {
      return "manage group";
    }
  }

  /** The privilege to manage roles. */
  public static class ManageRole implements Privilege {

    private static final ManageRole INSTANCE = new ManageRole();

    private ManageRole() {}

    /** @return The instance of the privilege. */
    public static ManageRole get() {
      return INSTANCE;
    }

    /** @return The generic name of the privilege. */
    @Override
    public Name name() {
      return MANAGE_ROLE;
    }

    /** @return A readable string representation for the privilege. */
    @Override
    public String simpleString() {
      return "manage role";
    }
  }
}
