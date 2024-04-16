/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.authorization;

import static com.datastrato.gravitino.authorization.Privilege.Name.ALTER_CATALOG;
import static com.datastrato.gravitino.authorization.Privilege.Name.ALTER_FILESET;
import static com.datastrato.gravitino.authorization.Privilege.Name.ALTER_SCHEMA;
import static com.datastrato.gravitino.authorization.Privilege.Name.ALTER_TABLE;
import static com.datastrato.gravitino.authorization.Privilege.Name.ALTER_TOPIC;
import static com.datastrato.gravitino.authorization.Privilege.Name.CREATE_CATALOG;
import static com.datastrato.gravitino.authorization.Privilege.Name.CREATE_FILESET;
import static com.datastrato.gravitino.authorization.Privilege.Name.CREATE_SCHEMA;
import static com.datastrato.gravitino.authorization.Privilege.Name.CREATE_TABLE;
import static com.datastrato.gravitino.authorization.Privilege.Name.CREATE_TOPIC;
import static com.datastrato.gravitino.authorization.Privilege.Name.DROP_CATALOG;
import static com.datastrato.gravitino.authorization.Privilege.Name.DROP_FILESET;
import static com.datastrato.gravitino.authorization.Privilege.Name.DROP_SCHEMA;
import static com.datastrato.gravitino.authorization.Privilege.Name.DROP_TABLE;
import static com.datastrato.gravitino.authorization.Privilege.Name.DROP_TOPIC;
import static com.datastrato.gravitino.authorization.Privilege.Name.LIST_CATALOG;
import static com.datastrato.gravitino.authorization.Privilege.Name.LIST_FILESET;
import static com.datastrato.gravitino.authorization.Privilege.Name.LIST_SCHEMA;
import static com.datastrato.gravitino.authorization.Privilege.Name.LIST_TABLE;
import static com.datastrato.gravitino.authorization.Privilege.Name.LIST_TOPIC;
import static com.datastrato.gravitino.authorization.Privilege.Name.LOAD_FILESET;
import static com.datastrato.gravitino.authorization.Privilege.Name.LOAD_SCHEMA;
import static com.datastrato.gravitino.authorization.Privilege.Name.LOAD_TABLE;
import static com.datastrato.gravitino.authorization.Privilege.Name.LOAD_TOPIC;
import static com.datastrato.gravitino.authorization.Privilege.Name.READ_FILESET;
import static com.datastrato.gravitino.authorization.Privilege.Name.READ_TABLE;
import static com.datastrato.gravitino.authorization.Privilege.Name.READ_TOPIC;
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
      case LIST_CATALOG:
        return ListCatalog.get();
      case LOAD_CATALOG:
        return LoadCatalog.get();
      case CREATE_CATALOG:
        return CreateCatalog.get();
      case ALTER_CATALOG:
        return AlterCatalog.get();
      case DROP_CATALOG:
        return DropCatalog.get();

        // Schema
      case LIST_SCHEMA:
        return ListSchema.get();
      case LOAD_SCHEMA:
        return LoadSchema.get();
      case CREATE_SCHEMA:
        return CreateSchema.get();
      case ALTER_SCHEMA:
        return AlterSchema.get();
      case DROP_SCHEMA:
        return DropSchema.get();

        // Table
      case LIST_TABLE:
        return ListTable.get();
      case LOAD_TABLE:
        return LoadTable.get();
      case CREATE_TABLE:
        return CreateTable.get();
      case ALTER_TABLE:
        return AlterTable.get();
      case DROP_TABLE:
        return DropTable.get();
      case READ_TABLE:
        return ReadTable.get();
      case WRITE_TABLE:
        return WriteTable.get();

        // Fileset
      case LIST_FILESET:
        return ListFileset.get();
      case LOAD_FILESET:
        return LoadFileset.get();
      case CREATE_FILESET:
        return CreateFileset.get();
      case ALTER_FILESET:
        return AlterFileset.get();
      case DROP_FILESET:
        return DropFileset.get();
      case READ_FILESET:
        return ReadFileset.get();
      case WRITE_FILESET:
        return WriteFileset.get();

        // Topic
      case LIST_TOPIC:
        return ListTopic.get();
      case LOAD_TOPIC:
        return LoadTopic.get();
      case CREATE_TOPIC:
        return CreateTopic.get();
      case DROP_TOPIC:
        return DropTopic.get();
      case ALTER_TOPIC:
        return AlterTopic.get();
      case READ_TOPIC:
        return ReadTopic.get();
      case WRITE_TOPIC:
        return WriteTopic.get();

      default:
        throw new IllegalArgumentException("Don't support the privilege: " + name);
    }
  }

  /** The privilege to list catalogs. */
  public static class ListCatalog implements Privilege {

    private static final ListCatalog INSTANCE = new ListCatalog();

    /** @return The instance of the privilege. */
    public static ListCatalog get() {
      return INSTANCE;
    }

    /** @return The generic name of the privilege. */
    @Override
    public Name name() {
      return LIST_CATALOG;
    }

    /** @return A readable string representation for the privilege. */
    @Override
    public String simpleString() {
      return "list catalog";
    }
  }

  /** The privilege to load a catalog. */
  public static class LoadCatalog implements Privilege {
    private static final LoadCatalog INSTANCE = new LoadCatalog();

    /** @return The instance of the privilege. */
    public static LoadCatalog get() {
      return INSTANCE;
    }

    private LoadCatalog() {}

    /** @return The generic name of the privilege. */
    @Override
    public Name name() {
      return Name.LOAD_CATALOG;
    }

    /** @return A readable string representation for the privilege. */
    @Override
    public String simpleString() {
      return "load catalog";
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

  /** The privilege to list schemas. */
  public static class ListSchema implements Privilege {

    private static final ListSchema INSTANCE = new ListSchema();

    private ListSchema() {}

    /** @return The instance of the privilege. */
    public static ListSchema get() {
      return INSTANCE;
    }

    /** @return The generic name of the privilege. */
    @Override
    public Name name() {
      return LIST_SCHEMA;
    }

    /** @return A readable string representation for the privilege. */
    @Override
    public String simpleString() {
      return "list schema";
    }
  }

  /** The privilege to load a schema. */
  public static class LoadSchema implements Privilege {

    private static final LoadSchema INSTANCE = new LoadSchema();

    /** @return The instance of the privilege. */
    public static LoadSchema get() {
      return INSTANCE;
    }

    /** @return The generic name of the privilege. */
    @Override
    public Name name() {
      return LOAD_SCHEMA;
    }

    /** @return A readable string representation for the privilege. */
    @Override
    public String simpleString() {
      return "load schema";
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

  /** The privilege to list tables. */
  public static class ListTable implements Privilege {

    private static final ListTable INSTANCE = new ListTable();

    /** @return The instance of the privilege. */
    public static ListTable get() {
      return INSTANCE;
    }

    /** @return The generic name of the privilege. */
    @Override
    public Name name() {
      return LIST_TABLE;
    }

    /** @return A readable string representation for the privilege. */
    @Override
    public String simpleString() {
      return "list table";
    }
  }

  /** The privilege to load a table. */
  public static class LoadTable implements Privilege {

    private static final LoadTable INSTANCE = new LoadTable();

    /** @return The instance of the privilege. */
    public static LoadTable get() {
      return INSTANCE;
    }

    /** @return The generic name of the privilege. */
    @Override
    public Name name() {
      return LOAD_TABLE;
    }

    /** @return A readable string representation for the privilege. */
    @Override
    public String simpleString() {
      return "load table";
    }
  }

  /** The privilege to alter a table. */
  public static class AlterTable implements Privilege {

    private static final AlterTable INSTANCE = new AlterTable();

    /** @return The instance of the privilege. */
    public static AlterTable get() {
      return INSTANCE;
    }

    /** @return The generic name of the privilege. */
    @Override
    public Name name() {
      return ALTER_TABLE;
    }

    /** @return A readable string representation for the privilege. */
    @Override
    public String simpleString() {
      return "alter table";
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

  /** The privilege to load a fileset. */
  public static class LoadFileset implements Privilege {
    private static final LoadFileset INSTANCE = new LoadFileset();

    private LoadFileset() {}

    /** @return The instance of the privilege. */
    public static LoadFileset get() {
      return INSTANCE;
    }

    /** @return The generic name of the privilege. */
    @Override
    public Name name() {
      return LOAD_FILESET;
    }

    /** @return A readable string representation for the privilege. */
    @Override
    public String simpleString() {
      return "load fileset";
    }
  }

  /** The privilege to alter a fileset. */
  public static class AlterFileset implements Privilege {

    private static final AlterFileset INSTANCE = new AlterFileset();

    private AlterFileset() {}

    /** @return The instance of the privilege. */
    public static AlterFileset get() {
      return INSTANCE;
    }

    /** @return The generic name of the privilege. */
    @Override
    public Name name() {
      return ALTER_FILESET;
    }

    /** @return A readable string representation for the privilege. */
    @Override
    public String simpleString() {
      return "alter fileset";
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

  /** The privilege to load a topic. */
  public static class LoadTopic implements Privilege {
    private static final LoadTopic INSTANCE = new LoadTopic();

    private LoadTopic() {}

    /** @return The instance of the privilege. */
    public static LoadTopic get() {
      return INSTANCE;
    }

    /** @return The generic name of the privilege. */
    @Override
    public Name name() {
      return LOAD_TOPIC;
    }

    /** @return A readable string representation for the privilege. */
    @Override
    public String simpleString() {
      return "load topic";
    }
  }

  /** The privilege to alter a topic. */
  public static class AlterTopic implements Privilege {

    private static final AlterTopic INSTANCE = new AlterTopic();

    private AlterTopic() {}

    /** @return The instance of the privilege. */
    public static AlterTopic get() {
      return INSTANCE;
    }

    /** @return The generic name of the privilege. */
    @Override
    public Name name() {
      return ALTER_TOPIC;
    }

    /** @return A readable string representation for the privilege. */
    @Override
    public String simpleString() {
      return "alter topic";
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
}
