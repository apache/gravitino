/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.authorization;

import java.util.Objects;

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

  public abstract static class GenericPrivilege<T extends GenericPrivilege<T>>
      implements Privilege {
    @FunctionalInterface
    public interface GenericPrivilegeFactory<T extends GenericPrivilege<T>> {
      T create(Condition condition, Name name);
    }

    private final Condition condition;
    private final Name name;

    protected GenericPrivilege(Condition condition, Name name) {
      this.condition = condition;
      this.name = name;
    }

    public static <T extends GenericPrivilege<T>> T allow(
        Name name, GenericPrivilegeFactory<T> factory) {
      return factory.create(Condition.ALLOW, name);
    }

    public static <T extends GenericPrivilege<T>> T deny(
        Name name, GenericPrivilegeFactory<T> factory) {
      return factory.create(Condition.DENY, name);
    }

    @Override
    public Name name() {
      return name;
    }

    @Override
    public Condition condition() {
      return condition;
    }

    @Override
    public String simpleString() {
      return condition.name() + " " + name.name().toLowerCase().replace('_', ' ');
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (!(o instanceof GenericPrivilege)) return false;
      GenericPrivilege<?> that = (GenericPrivilege<?>) o;
      return condition == that.condition && name == that.name;
    }

    @Override
    public int hashCode() {
      return Objects.hash(condition, name);
    }
  }

  /** The privilege to create a catalog. */
  public static class CreateCatalog extends GenericPrivilege<CreateCatalog> {
    private CreateCatalog(Condition condition, Name name) {
      super(condition, name);
    }

    /** @return The instance with allow condition of the privilege. */
    public static CreateCatalog allow() {
      return GenericPrivilege.allow(Name.CREATE_CATALOG, CreateCatalog::new);
    }

    /** @return The instance with deny condition of the privilege. */
    public static CreateCatalog deny() {
      return GenericPrivilege.deny(Name.CREATE_CATALOG, CreateCatalog::new);
    }
  }

  /** The privilege to alter a catalog. */
  public static class AlterCatalog extends GenericPrivilege<AlterCatalog> {
    private AlterCatalog(Condition condition, Name name) {
      super(condition, name);
    }

    /** @return The instance with allow condition of the privilege. */
    public static AlterCatalog allow() {
      return GenericPrivilege.allow(Name.ALTER_CATALOG, AlterCatalog::new);
    }

    /** @return The instance with deny condition of the privilege. */
    public static AlterCatalog deny() {
      return GenericPrivilege.deny(Name.ALTER_CATALOG, AlterCatalog::new);
    }
  }

  /** The privilege to drop a catalog. */
  public static class DropCatalog extends GenericPrivilege<DropCatalog> {
    private DropCatalog(Condition condition, Name name) {
      super(condition, name);
    }

    /** @return The instance with allow condition of the privilege. */
    public static DropCatalog allow() {
      return GenericPrivilege.allow(Name.DROP_CATALOG, DropCatalog::new);
    }

    /** @return The instance with deny condition of the privilege. */
    public static DropCatalog deny() {
      return GenericPrivilege.deny(Name.DROP_CATALOG, DropCatalog::new);
    }
  }

  /** The privilege to use a catalog. */
  public static class UseCatalog extends GenericPrivilege<UseCatalog> {
    private UseCatalog(Condition condition, Name name) {
      super(condition, name);
    }

    /** @return The instance with allow condition of the privilege. */
    public static UseCatalog allow() {
      return GenericPrivilege.allow(Name.USE_CATALOG, UseCatalog::new);
    }

    /** @return The instance with deny condition of the privilege. */
    public static UseCatalog deny() {
      return GenericPrivilege.deny(Name.USE_CATALOG, UseCatalog::new);
    }
  }

  /** The privilege to use a schema. */
  public static class UseSchema extends GenericPrivilege<UseSchema> {
    private UseSchema(Condition condition, Name name) {
      super(condition, name);
    }

    /** @return The instance with allow condition of the privilege. */
    public static UseSchema allow() {
      return GenericPrivilege.allow(Name.USE_SCHEMA, UseSchema::new);
    }

    /** @return The instance with deny condition of the privilege. */
    public static UseSchema deny() {
      return GenericPrivilege.deny(Name.USE_SCHEMA, UseSchema::new);
    }
  }

  /** The privilege to create a schema. */
  public static class CreateSchema extends GenericPrivilege<CreateSchema> {
    private CreateSchema(Condition condition, Name name) {
      super(condition, name);
    }

    /** @return The instance with allow condition of the privilege. */
    public static CreateSchema allow() {
      return GenericPrivilege.allow(Name.CREATE_SCHEMA, CreateSchema::new);
    }

    /** @return The instance with deny condition of the privilege. */
    public static CreateSchema deny() {
      return GenericPrivilege.deny(Name.CREATE_SCHEMA, CreateSchema::new);
    }
  }

  /** The privilege to alter a schema. */
  public static class AlterSchema extends GenericPrivilege<AlterSchema> {
    private AlterSchema(Condition condition, Name name) {
      super(condition, name);
    }

    /** @return The instance with allow condition of the privilege. */
    public static AlterSchema allow() {
      return GenericPrivilege.allow(Name.ALTER_SCHEMA, AlterSchema::new);
    }

    /** @return The instance with deny condition of the privilege. */
    public static AlterSchema deny() {
      return GenericPrivilege.deny(Name.ALTER_SCHEMA, AlterSchema::new);
    }
  }

  /** The privilege to drop a schema. */
  public static class DropSchema extends GenericPrivilege<DropSchema> {
    private DropSchema(Condition condition, Name name) {
      super(condition, name);
    }

    /** @return The instance with allow condition of the privilege. */
    public static DropSchema allow() {
      return GenericPrivilege.allow(Name.DROP_SCHEMA, DropSchema::new);
    }

    /** @return The instance with deny condition of the privilege. */
    public static DropSchema deny() {
      return GenericPrivilege.deny(Name.DROP_SCHEMA, DropSchema::new);
    }
  }

  /** The privilege to create a table. */
  public static class CreateTable extends GenericPrivilege<CreateTable> {
    private CreateTable(Condition condition, Name name) {
      super(condition, name);
    }

    /** @return The instance with allow condition of the privilege. */
    public static CreateTable allow() {
      return GenericPrivilege.allow(Name.CREATE_TABLE, CreateTable::new);
    }

    /** @return The instance with deny condition of the privilege. */
    public static CreateTable deny() {
      return GenericPrivilege.deny(Name.CREATE_TABLE, CreateTable::new);
    }
  }

  /** The privilege to drop a table. */
  public static class DropTable extends GenericPrivilege<DropTable> {
    private DropTable(Condition condition, Name name) {
      super(condition, name);
    }

    /** @return The instance with allow condition of the privilege. */
    public static DropTable allow() {
      return GenericPrivilege.allow(Name.DROP_TABLE, DropTable::new);
    }

    /** @return The instance with deny condition of the privilege. */
    public static DropTable deny() {
      return GenericPrivilege.deny(Name.DROP_TABLE, DropTable::new);
    }
  }

  /** The privilege to read a table. */
  public static class ReadTable extends GenericPrivilege<ReadTable> {
    private ReadTable(Condition condition, Name name) {
      super(condition, name);
    }

    /** @return The instance with allow condition of the privilege. */
    public static ReadTable allow() {
      return GenericPrivilege.allow(Name.READ_TABLE, ReadTable::new);
    }

    /** @return The instance with deny condition of the privilege. */
    public static ReadTable deny() {
      return GenericPrivilege.deny(Name.READ_TABLE, ReadTable::new);
    }
  }

  /** The privilege to write a table. */
  public static class WriteTable extends GenericPrivilege<WriteTable> {
    private WriteTable(Condition condition, Name name) {
      super(condition, name);
    }

    /** @return The instance with allow condition of the privilege. */
    public static WriteTable allow() {
      return GenericPrivilege.allow(Name.WRITE_TABLE, WriteTable::new);
    }

    /** @return The instance with deny condition of the privilege. */
    public static WriteTable deny() {
      return GenericPrivilege.deny(Name.WRITE_TABLE, WriteTable::new);
    }
  }

  /** The privilege to create a fileset. */
  public static class CreateFileset extends GenericPrivilege<CreateFileset> {
    private CreateFileset(Condition condition, Name name) {
      super(condition, name);
    }

    /** @return The instance with allow condition of the privilege. */
    public static CreateFileset allow() {
      return GenericPrivilege.allow(Name.CREATE_FILESET, CreateFileset::new);
    }

    /** @return The instance with deny condition of the privilege. */
    public static CreateFileset deny() {
      return GenericPrivilege.deny(Name.CREATE_FILESET, CreateFileset::new);
    }
  }

  /** The privilege to drop a fileset. */
  public static class DropFileset extends GenericPrivilege<DropFileset> {
    private DropFileset(Condition condition, Name name) {
      super(condition, name);
    }

    /** @return The instance with allow condition of the privilege. */
    public static DropFileset allow() {
      return GenericPrivilege.allow(Name.DROP_FILESET, DropFileset::new);
    }

    /** @return The instance with deny condition of the privilege. */
    public static DropFileset deny() {
      return GenericPrivilege.deny(Name.DROP_FILESET, DropFileset::new);
    }
  }

  /** The privilege to read a fileset. */
  public static class ReadFileset extends GenericPrivilege<ReadFileset> {
    private ReadFileset(Condition condition, Name name) {
      super(condition, name);
    }

    /** @return The instance with allow condition of the privilege. */
    public static ReadFileset allow() {
      return GenericPrivilege.allow(Name.READ_FILESET, ReadFileset::new);
    }

    /** @return The instance with deny condition of the privilege. */
    public static ReadFileset deny() {
      return GenericPrivilege.deny(Name.READ_FILESET, ReadFileset::new);
    }
  }

  /** The privilege to write a fileset. */
  public static class WriteFileset extends GenericPrivilege<WriteFileset> {
    private WriteFileset(Condition condition, Name name) {
      super(condition, name);
    }

    /** @return The instance with allow condition of the privilege. */
    public static WriteFileset allow() {
      return GenericPrivilege.allow(Name.WRITE_FILESET, WriteFileset::new);
    }

    /** @return The instance with deny condition of the privilege. */
    public static WriteFileset deny() {
      return GenericPrivilege.deny(Name.WRITE_FILESET, WriteFileset::new);
    }
  }

  /** The privilege to create a topic. */
  public static class CreateTopic extends GenericPrivilege<CreateTopic> {
    private CreateTopic(Condition condition, Name name) {
      super(condition, name);
    }

    /** @return The instance with allow condition of the privilege. */
    public static CreateTopic allow() {
      return GenericPrivilege.allow(Name.CREATE_TOPIC, CreateTopic::new);
    }

    /** @return The instance with deny condition of the privilege. */
    public static CreateTopic deny() {
      return GenericPrivilege.deny(Name.CREATE_TOPIC, CreateTopic::new);
    }
  }

  /** The privilege to drop a topic. */
  public static class DropTopic extends GenericPrivilege<DropTopic> {
    private DropTopic(Condition condition, Name name) {
      super(condition, name);
    }

    /** @return The instance with allow condition of the privilege. */
    public static DropTopic allow() {
      return GenericPrivilege.allow(Name.DROP_TOPIC, DropTopic::new);
    }

    /** @return The instance with deny condition of the privilege. */
    public static DropTopic deny() {
      return GenericPrivilege.deny(Name.DROP_TOPIC, DropTopic::new);
    }
  }

  /** The privilege to read a topic. */
  public static class ReadTopic extends GenericPrivilege<ReadTopic> {
    private ReadTopic(Condition condition, Name name) {
      super(condition, name);
    }

    /** @return The instance with allow condition of the privilege. */
    public static ReadTopic allow() {
      return GenericPrivilege.allow(Name.READ_TOPIC, ReadTopic::new);
    }

    /** @return The instance with deny condition of the privilege. */
    public static ReadTopic deny() {
      return GenericPrivilege.deny(Name.READ_TOPIC, ReadTopic::new);
    }
  }

  /** The privilege to write a topic. */
  public static class WriteTopic extends GenericPrivilege<WriteTopic> {
    private WriteTopic(Condition condition, Name name) {
      super(condition, name);
    }

    /** @return The instance with allow condition of the privilege. */
    public static WriteTopic allow() {
      return GenericPrivilege.allow(Name.WRITE_TOPIC, WriteTopic::new);
    }

    /** @return The instance with deny condition of the privilege. */
    public static WriteTopic deny() {
      return GenericPrivilege.deny(Name.WRITE_TOPIC, WriteTopic::new);
    }
  }

  /** The privilege to manage a metalake. */
  public static class ManageMetalake extends GenericPrivilege<ManageMetalake> {
    private ManageMetalake(Condition condition, Name name) {
      super(condition, name);
    }

    /** @return The instance with allow condition of the privilege. */
    public static ManageMetalake allow() {
      return GenericPrivilege.allow(Name.MANAGE_METALAKE, ManageMetalake::new);
    }

    /** @return The instance with deny condition of the privilege. */
    public static ManageMetalake deny() {
      return GenericPrivilege.deny(Name.MANAGE_METALAKE, ManageMetalake::new);
    }
  }

  /** The privilege to create a metalake. */
  public static class CreateMetalake extends GenericPrivilege<CreateMetalake> {
    private CreateMetalake(Condition condition, Name name) {
      super(condition, name);
    }

    /** @return The instance with allow condition of the privilege. */
    public static CreateMetalake allow() {
      return GenericPrivilege.allow(Name.CREATE_METALAKE, CreateMetalake::new);
    }

    /** @return The instance with deny condition of the privilege. */
    public static CreateMetalake deny() {
      return GenericPrivilege.deny(Name.CREATE_METALAKE, CreateMetalake::new);
    }
  }

  /** The privilege to use a metalake. */
  public static class UseMetalake extends GenericPrivilege<UseMetalake> {
    private UseMetalake(Condition condition, Name name) {
      super(condition, name);
    }

    /** @return The instance with allow condition of the privilege. */
    public static UseMetalake allow() {
      return GenericPrivilege.allow(Name.USE_METALAKE, UseMetalake::new);
    }

    /** @return The instance with deny condition of the privilege. */
    public static UseMetalake deny() {
      return GenericPrivilege.deny(Name.USE_METALAKE, UseMetalake::new);
    }
  }

  /** The privilege to get a user. */
  public static class GetUser extends GenericPrivilege<GetUser> {
    private GetUser(Condition condition, Name name) {
      super(condition, name);
    }

    /** @return The instance with allow condition of the privilege. */
    public static GetUser allow() {
      return GenericPrivilege.allow(Name.GET_USER, GetUser::new);
    }

    /** @return The instance with deny condition of the privilege. */
    public static GetUser deny() {
      return GenericPrivilege.deny(Name.GET_USER, GetUser::new);
    }
  }

  /** The privilege to add a user. */
  public static class AddUser extends GenericPrivilege<AddUser> {
    private AddUser(Condition condition, Name name) {
      super(condition, name);
    }

    /** @return The instance with allow condition of the privilege. */
    public static AddUser allow() {
      return GenericPrivilege.allow(Name.ADD_USER, AddUser::new);
    }

    /** @return The instance with deny condition of the privilege. */
    public static AddUser deny() {
      return GenericPrivilege.deny(Name.ADD_USER, AddUser::new);
    }
  }

  /** The privilege to remove a user. */
  public static class RemoveUser extends GenericPrivilege<RemoveUser> {
    private RemoveUser(Condition condition, Name name) {
      super(condition, name);
    }

    /** @return The instance with allow condition of the privilege. */
    public static RemoveUser allow() {
      return GenericPrivilege.allow(Name.REMOVE_USER, RemoveUser::new);
    }

    /** @return The instance with deny condition of the privilege. */
    public static RemoveUser deny() {
      return GenericPrivilege.deny(Name.REMOVE_USER, RemoveUser::new);
    }
  }

  /** The privilege to add a group. */
  public static class AddGroup extends GenericPrivilege<AddGroup> {
    private AddGroup(Condition condition, Name name) {
      super(condition, name);
    }

    /** @return The instance with allow condition of the privilege. */
    public static AddGroup allow() {
      return GenericPrivilege.allow(Name.ADD_GROUP, AddGroup::new);
    }

    /** @return The instance with deny condition of the privilege. */
    public static AddGroup deny() {
      return GenericPrivilege.deny(Name.ADD_GROUP, AddGroup::new);
    }
  }

  /** The privilege to remove a group. */
  public static class RemoveGroup extends GenericPrivilege<RemoveGroup> {
    private RemoveGroup(Condition condition, Name name) {
      super(condition, name);
    }

    /** @return The instance with allow condition of the privilege. */
    public static RemoveGroup allow() {
      return GenericPrivilege.allow(Name.REMOVE_GROUP, RemoveGroup::new);
    }

    /** @return The instance with deny condition of the privilege. */
    public static RemoveGroup deny() {
      return GenericPrivilege.deny(Name.REMOVE_GROUP, RemoveGroup::new);
    }
  }

  /** The privilege to get a group. */
  public static class GetGroup extends GenericPrivilege<GetGroup> {
    private GetGroup(Condition condition, Name name) {
      super(condition, name);
    }

    /** @return The instance with allow condition of the privilege. */
    public static GetGroup allow() {
      return GenericPrivilege.allow(Name.GET_GROUP, GetGroup::new);
    }

    /** @return The instance with deny condition of the privilege. */
    public static GetGroup deny() {
      return GenericPrivilege.deny(Name.GET_GROUP, GetGroup::new);
    }
  }

  /** The privilege to create a role. */
  public static class CreateRole extends GenericPrivilege<CreateRole> {
    private CreateRole(Condition condition, Name name) {
      super(condition, name);
    }

    /** @return The instance with allow condition of the privilege. */
    public static CreateRole allow() {
      return GenericPrivilege.allow(Name.CREATE_ROLE, CreateRole::new);
    }

    /** @return The instance with deny condition of the privilege. */
    public static CreateRole deny() {
      return GenericPrivilege.deny(Name.CREATE_ROLE, CreateRole::new);
    }
  }

  /** The privilege to get a role. */
  public static class GetRole extends GenericPrivilege<GetRole> {
    private GetRole(Condition condition, Name name) {
      super(condition, name);
    }

    /** @return The instance with allow condition of the privilege. */
    public static GetRole allow() {
      return GenericPrivilege.allow(Name.GET_ROLE, GetRole::new);
    }

    /** @return The instance with deny condition of the privilege. */
    public static GetRole deny() {
      return GenericPrivilege.deny(Name.GET_ROLE, GetRole::new);
    }
  }

  /** The privilege to delete a role. */
  public static class DeleteRole extends GenericPrivilege<DeleteRole> {
    private DeleteRole(Condition condition, Name name) {
      super(condition, name);
    }

    /** @return The instance with allow condition of the privilege. */
    public static DeleteRole allow() {
      return GenericPrivilege.allow(Name.DELETE_ROLE, DeleteRole::new);
    }

    /** @return The instance with deny condition of the privilege. */
    public static DeleteRole deny() {
      return GenericPrivilege.deny(Name.DELETE_ROLE, DeleteRole::new);
    }
  }

  /** The privilege to grant a role to the user or the group. */
  public static class GrantRole extends GenericPrivilege<GrantRole> {
    private GrantRole(Condition condition, Name name) {
      super(condition, name);
    }

    /** @return The instance with allow condition of the privilege. */
    public static GrantRole allow() {
      return GenericPrivilege.allow(Name.GRANT_ROLE, GrantRole::new);
    }

    /** @return The instance with deny condition of the privilege. */
    public static GrantRole deny() {
      return GenericPrivilege.deny(Name.GRANT_ROLE, GrantRole::new);
    }
  }

  /** The privilege to revoke a role from the user or the group. */
  public static class RevokeRole extends GenericPrivilege<RevokeRole> {
    private RevokeRole(Condition condition, Name name) {
      super(condition, name);
    }

    /** @return The instance with allow condition of the privilege. */
    public static RevokeRole allow() {
      return GenericPrivilege.allow(Name.REVOKE_ROLE, RevokeRole::new);
    }

    /** @return The instance with deny condition of the privilege. */
    public static RevokeRole deny() {
      return GenericPrivilege.deny(Name.REVOKE_ROLE, RevokeRole::new);
    }
  }
}
