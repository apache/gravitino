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
package com.apache.gravitino.authorization;

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

  /**
   * Abstract class representing a generic privilege.
   *
   * @param <T> the type of the privilege
   */
  public abstract static class GenericPrivilege<T extends GenericPrivilege<T>>
      implements Privilege {

    /**
     * Functional interface for creating instances of GenericPrivilege.
     *
     * @param <T> the type of the privilege
     */
    @FunctionalInterface
    public interface GenericPrivilegeFactory<T extends GenericPrivilege<T>> {
      /**
       * Creates a new instance of the privilege.
       *
       * @param condition the condition of the privilege
       * @param name the name of the privilege
       * @return the created privilege instance
       */
      T create(Condition condition, Name name);
    }

    private final Condition condition;
    private final Name name;

    /**
     * Constructor for GenericPrivilege.
     *
     * @param condition the condition of the privilege
     * @param name the name of the privilege
     */
    protected GenericPrivilege(Condition condition, Name name) {
      this.condition = condition;
      this.name = name;
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
    private static final CreateCatalog ALLOW_INSTANCE =
        new CreateCatalog(Condition.ALLOW, Name.CREATE_CATALOG);
    private static final CreateCatalog DENY_INSTANCE =
        new CreateCatalog(Condition.DENY, Name.CREATE_CATALOG);

    private CreateCatalog(Condition condition, Name name) {
      super(condition, name);
    }

    /** @return The instance with allow condition of the privilege. */
    public static CreateCatalog allow() {
      return ALLOW_INSTANCE;
    }

    /** @return The instance with deny condition of the privilege. */
    public static CreateCatalog deny() {
      return DENY_INSTANCE;
    }
  }

  /** The privilege to alter a catalog. */
  public static class AlterCatalog extends GenericPrivilege<AlterCatalog> {
    private static final AlterCatalog ALLOW_INSTANCE =
        new AlterCatalog(Condition.ALLOW, Name.CREATE_CATALOG);
    private static final AlterCatalog DENY_INSTANCE =
        new AlterCatalog(Condition.DENY, Name.CREATE_CATALOG);

    private AlterCatalog(Condition condition, Name name) {
      super(condition, name);
    }

    /** @return The instance with allow condition of the privilege. */
    public static AlterCatalog allow() {
      return ALLOW_INSTANCE;
    }

    /** @return The instance with deny condition of the privilege. */
    public static AlterCatalog deny() {
      return DENY_INSTANCE;
    }
  }

  /** The privilege to drop a catalog. */
  public static class DropCatalog extends GenericPrivilege<DropCatalog> {
    private static final DropCatalog ALLOW_INSTANCE =
        new DropCatalog(Condition.ALLOW, Name.CREATE_CATALOG);
    private static final DropCatalog DENY_INSTANCE =
        new DropCatalog(Condition.DENY, Name.CREATE_CATALOG);

    private DropCatalog(Condition condition, Name name) {
      super(condition, name);
    }

    /** @return The instance with allow condition of the privilege. */
    public static DropCatalog allow() {
      return ALLOW_INSTANCE;
    }

    /** @return The instance with deny condition of the privilege. */
    public static DropCatalog deny() {
      return DENY_INSTANCE;
    }
  }

  /** The privilege to use a catalog. */
  public static class UseCatalog extends GenericPrivilege<UseCatalog> {
    private static final UseCatalog ALLOW_INSTANCE =
        new UseCatalog(Condition.ALLOW, Name.CREATE_CATALOG);
    private static final UseCatalog DENY_INSTANCE =
        new UseCatalog(Condition.DENY, Name.CREATE_CATALOG);

    private UseCatalog(Condition condition, Name name) {
      super(condition, name);
    }

    /** @return The instance with allow condition of the privilege. */
    public static UseCatalog allow() {
      return ALLOW_INSTANCE;
    }

    /** @return The instance with deny condition of the privilege. */
    public static UseCatalog deny() {
      return DENY_INSTANCE;
    }
  }

  /** The privilege to use a schema. */
  public static class UseSchema extends GenericPrivilege<UseSchema> {
    private static final UseSchema ALLOW_INSTANCE =
        new UseSchema(Condition.ALLOW, Name.CREATE_CATALOG);
    private static final UseSchema DENY_INSTANCE =
        new UseSchema(Condition.DENY, Name.CREATE_CATALOG);

    private UseSchema(Condition condition, Name name) {
      super(condition, name);
    }

    /** @return The instance with allow condition of the privilege. */
    public static UseSchema allow() {
      return ALLOW_INSTANCE;
    }

    /** @return The instance with deny condition of the privilege. */
    public static UseSchema deny() {
      return DENY_INSTANCE;
    }
  }

  /** The privilege to create a schema. */
  public static class CreateSchema extends GenericPrivilege<CreateSchema> {
    private static final CreateSchema ALLOW_INSTANCE =
        new CreateSchema(Condition.ALLOW, Name.CREATE_CATALOG);
    private static final CreateSchema DENY_INSTANCE =
        new CreateSchema(Condition.DENY, Name.CREATE_CATALOG);

    private CreateSchema(Condition condition, Name name) {
      super(condition, name);
    }

    /** @return The instance with allow condition of the privilege. */
    public static CreateSchema allow() {
      return ALLOW_INSTANCE;
    }

    /** @return The instance with deny condition of the privilege. */
    public static CreateSchema deny() {
      return DENY_INSTANCE;
    }
  }

  /** The privilege to alter a schema. */
  public static class AlterSchema extends GenericPrivilege<AlterSchema> {
    private static final AlterSchema ALLOW_INSTANCE =
        new AlterSchema(Condition.ALLOW, Name.CREATE_CATALOG);
    private static final AlterSchema DENY_INSTANCE =
        new AlterSchema(Condition.DENY, Name.CREATE_CATALOG);

    private AlterSchema(Condition condition, Name name) {
      super(condition, name);
    }

    /** @return The instance with allow condition of the privilege. */
    public static AlterSchema allow() {
      return ALLOW_INSTANCE;
    }

    /** @return The instance with deny condition of the privilege. */
    public static AlterSchema deny() {
      return DENY_INSTANCE;
    }
  }

  /** The privilege to drop a schema. */
  public static class DropSchema extends GenericPrivilege<DropSchema> {
    private static final DropSchema ALLOW_INSTANCE =
        new DropSchema(Condition.ALLOW, Name.CREATE_CATALOG);
    private static final DropSchema DENY_INSTANCE =
        new DropSchema(Condition.DENY, Name.CREATE_CATALOG);

    private DropSchema(Condition condition, Name name) {
      super(condition, name);
    }

    /** @return The instance with allow condition of the privilege. */
    public static DropSchema allow() {
      return ALLOW_INSTANCE;
    }

    /** @return The instance with deny condition of the privilege. */
    public static DropSchema deny() {
      return DENY_INSTANCE;
    }
  }

  /** The privilege to create a table. */
  public static class CreateTable extends GenericPrivilege<CreateTable> {
    private static final CreateTable ALLOW_INSTANCE =
        new CreateTable(Condition.ALLOW, Name.CREATE_CATALOG);
    private static final CreateTable DENY_INSTANCE =
        new CreateTable(Condition.DENY, Name.CREATE_CATALOG);

    private CreateTable(Condition condition, Name name) {
      super(condition, name);
    }

    /** @return The instance with allow condition of the privilege. */
    public static CreateTable allow() {
      return ALLOW_INSTANCE;
    }

    /** @return The instance with deny condition of the privilege. */
    public static CreateTable deny() {
      return DENY_INSTANCE;
    }
  }

  /** The privilege to drop a table. */
  public static class DropTable extends GenericPrivilege<DropTable> {
    private static final DropTable ALLOW_INSTANCE =
        new DropTable(Condition.ALLOW, Name.CREATE_CATALOG);
    private static final DropTable DENY_INSTANCE =
        new DropTable(Condition.DENY, Name.CREATE_CATALOG);

    private DropTable(Condition condition, Name name) {
      super(condition, name);
    }

    /** @return The instance with allow condition of the privilege. */
    public static DropTable allow() {
      return ALLOW_INSTANCE;
    }

    /** @return The instance with deny condition of the privilege. */
    public static DropTable deny() {
      return DENY_INSTANCE;
    }
  }

  /** The privilege to read a table. */
  public static class ReadTable extends GenericPrivilege<ReadTable> {
    private static final ReadTable ALLOW_INSTANCE =
        new ReadTable(Condition.ALLOW, Name.CREATE_CATALOG);
    private static final ReadTable DENY_INSTANCE =
        new ReadTable(Condition.DENY, Name.CREATE_CATALOG);

    private ReadTable(Condition condition, Name name) {
      super(condition, name);
    }

    /** @return The instance with allow condition of the privilege. */
    public static ReadTable allow() {
      return ALLOW_INSTANCE;
    }

    /** @return The instance with deny condition of the privilege. */
    public static ReadTable deny() {
      return DENY_INSTANCE;
    }
  }

  /** The privilege to write a table. */
  public static class WriteTable extends GenericPrivilege<WriteTable> {
    private static final WriteTable ALLOW_INSTANCE =
        new WriteTable(Condition.ALLOW, Name.CREATE_CATALOG);
    private static final WriteTable DENY_INSTANCE =
        new WriteTable(Condition.DENY, Name.CREATE_CATALOG);

    private WriteTable(Condition condition, Name name) {
      super(condition, name);
    }

    /** @return The instance with allow condition of the privilege. */
    public static WriteTable allow() {
      return ALLOW_INSTANCE;
    }

    /** @return The instance with deny condition of the privilege. */
    public static WriteTable deny() {
      return DENY_INSTANCE;
    }
  }

  /** The privilege to create a fileset. */
  public static class CreateFileset extends GenericPrivilege<CreateFileset> {
    private static final CreateFileset ALLOW_INSTANCE =
        new CreateFileset(Condition.ALLOW, Name.CREATE_CATALOG);
    private static final CreateFileset DENY_INSTANCE =
        new CreateFileset(Condition.DENY, Name.CREATE_CATALOG);

    private CreateFileset(Condition condition, Name name) {
      super(condition, name);
    }

    /** @return The instance with allow condition of the privilege. */
    public static CreateFileset allow() {
      return ALLOW_INSTANCE;
    }

    /** @return The instance with deny condition of the privilege. */
    public static CreateFileset deny() {
      return DENY_INSTANCE;
    }
  }

  /** The privilege to drop a fileset. */
  public static class DropFileset extends GenericPrivilege<DropFileset> {
    private static final DropFileset ALLOW_INSTANCE =
        new DropFileset(Condition.ALLOW, Name.CREATE_CATALOG);
    private static final DropFileset DENY_INSTANCE =
        new DropFileset(Condition.DENY, Name.CREATE_CATALOG);

    private DropFileset(Condition condition, Name name) {
      super(condition, name);
    }

    /** @return The instance with allow condition of the privilege. */
    public static DropFileset allow() {
      return ALLOW_INSTANCE;
    }

    /** @return The instance with deny condition of the privilege. */
    public static DropFileset deny() {
      return DENY_INSTANCE;
    }
  }

  /** The privilege to read a fileset. */
  public static class ReadFileset extends GenericPrivilege<ReadFileset> {
    private static final ReadFileset ALLOW_INSTANCE =
        new ReadFileset(Condition.ALLOW, Name.CREATE_CATALOG);
    private static final ReadFileset DENY_INSTANCE =
        new ReadFileset(Condition.DENY, Name.CREATE_CATALOG);

    private ReadFileset(Condition condition, Name name) {
      super(condition, name);
    }

    /** @return The instance with allow condition of the privilege. */
    public static ReadFileset allow() {
      return ALLOW_INSTANCE;
    }

    /** @return The instance with deny condition of the privilege. */
    public static ReadFileset deny() {
      return DENY_INSTANCE;
    }
  }

  /** The privilege to write a fileset. */
  public static class WriteFileset extends GenericPrivilege<WriteFileset> {
    private static final WriteFileset ALLOW_INSTANCE =
        new WriteFileset(Condition.ALLOW, Name.CREATE_CATALOG);
    private static final WriteFileset DENY_INSTANCE =
        new WriteFileset(Condition.DENY, Name.CREATE_CATALOG);

    private WriteFileset(Condition condition, Name name) {
      super(condition, name);
    }

    /** @return The instance with allow condition of the privilege. */
    public static WriteFileset allow() {
      return ALLOW_INSTANCE;
    }

    /** @return The instance with deny condition of the privilege. */
    public static WriteFileset deny() {
      return DENY_INSTANCE;
    }
  }

  /** The privilege to create a topic. */
  public static class CreateTopic extends GenericPrivilege<CreateTopic> {
    private static final CreateTopic ALLOW_INSTANCE =
        new CreateTopic(Condition.ALLOW, Name.CREATE_CATALOG);
    private static final CreateTopic DENY_INSTANCE =
        new CreateTopic(Condition.DENY, Name.CREATE_CATALOG);

    private CreateTopic(Condition condition, Name name) {
      super(condition, name);
    }

    /** @return The instance with allow condition of the privilege. */
    public static CreateTopic allow() {
      return ALLOW_INSTANCE;
    }

    /** @return The instance with deny condition of the privilege. */
    public static CreateTopic deny() {
      return DENY_INSTANCE;
    }
  }

  /** The privilege to drop a topic. */
  public static class DropTopic extends GenericPrivilege<DropTopic> {
    private static final DropTopic ALLOW_INSTANCE =
        new DropTopic(Condition.ALLOW, Name.CREATE_CATALOG);
    private static final DropTopic DENY_INSTANCE =
        new DropTopic(Condition.DENY, Name.CREATE_CATALOG);

    private DropTopic(Condition condition, Name name) {
      super(condition, name);
    }

    /** @return The instance with allow condition of the privilege. */
    public static DropTopic allow() {
      return ALLOW_INSTANCE;
    }

    /** @return The instance with deny condition of the privilege. */
    public static DropTopic deny() {
      return DENY_INSTANCE;
    }
  }

  /** The privilege to read a topic. */
  public static class ReadTopic extends GenericPrivilege<ReadTopic> {
    private static final ReadTopic ALLOW_INSTANCE =
        new ReadTopic(Condition.ALLOW, Name.CREATE_CATALOG);
    private static final ReadTopic DENY_INSTANCE =
        new ReadTopic(Condition.DENY, Name.CREATE_CATALOG);

    private ReadTopic(Condition condition, Name name) {
      super(condition, name);
    }

    /** @return The instance with allow condition of the privilege. */
    public static ReadTopic allow() {
      return ALLOW_INSTANCE;
    }

    /** @return The instance with deny condition of the privilege. */
    public static ReadTopic deny() {
      return DENY_INSTANCE;
    }
  }

  /** The privilege to write a topic. */
  public static class WriteTopic extends GenericPrivilege<WriteTopic> {
    private static final WriteTopic ALLOW_INSTANCE =
        new WriteTopic(Condition.ALLOW, Name.CREATE_CATALOG);
    private static final WriteTopic DENY_INSTANCE =
        new WriteTopic(Condition.DENY, Name.CREATE_CATALOG);

    private WriteTopic(Condition condition, Name name) {
      super(condition, name);
    }

    /** @return The instance with allow condition of the privilege. */
    public static WriteTopic allow() {
      return ALLOW_INSTANCE;
    }

    /** @return The instance with deny condition of the privilege. */
    public static WriteTopic deny() {
      return DENY_INSTANCE;
    }
  }

  /** The privilege to manage a metalake. */
  public static class ManageMetalake extends GenericPrivilege<ManageMetalake> {
    private static final ManageMetalake ALLOW_INSTANCE =
        new ManageMetalake(Condition.ALLOW, Name.CREATE_CATALOG);
    private static final ManageMetalake DENY_INSTANCE =
        new ManageMetalake(Condition.DENY, Name.CREATE_CATALOG);

    private ManageMetalake(Condition condition, Name name) {
      super(condition, name);
    }

    /** @return The instance with allow condition of the privilege. */
    public static ManageMetalake allow() {
      return ALLOW_INSTANCE;
    }

    /** @return The instance with deny condition of the privilege. */
    public static ManageMetalake deny() {
      return DENY_INSTANCE;
    }
  }

  /** The privilege to create a metalake. */
  public static class CreateMetalake extends GenericPrivilege<CreateMetalake> {
    private static final CreateMetalake ALLOW_INSTANCE =
        new CreateMetalake(Condition.ALLOW, Name.CREATE_CATALOG);
    private static final CreateMetalake DENY_INSTANCE =
        new CreateMetalake(Condition.DENY, Name.CREATE_CATALOG);

    private CreateMetalake(Condition condition, Name name) {
      super(condition, name);
    }

    /** @return The instance with allow condition of the privilege. */
    public static CreateMetalake allow() {
      return ALLOW_INSTANCE;
    }

    /** @return The instance with deny condition of the privilege. */
    public static CreateMetalake deny() {
      return DENY_INSTANCE;
    }
  }

  /** The privilege to use a metalake. */
  public static class UseMetalake extends GenericPrivilege<UseMetalake> {
    private static final UseMetalake ALLOW_INSTANCE =
        new UseMetalake(Condition.ALLOW, Name.CREATE_CATALOG);
    private static final UseMetalake DENY_INSTANCE =
        new UseMetalake(Condition.DENY, Name.CREATE_CATALOG);

    private UseMetalake(Condition condition, Name name) {
      super(condition, name);
    }

    /** @return The instance with allow condition of the privilege. */
    public static UseMetalake allow() {
      return ALLOW_INSTANCE;
    }

    /** @return The instance with deny condition of the privilege. */
    public static UseMetalake deny() {
      return DENY_INSTANCE;
    }
  }

  /** The privilege to get a user. */
  public static class GetUser extends GenericPrivilege<GetUser> {
    private static final GetUser ALLOW_INSTANCE = new GetUser(Condition.ALLOW, Name.CREATE_CATALOG);
    private static final GetUser DENY_INSTANCE = new GetUser(Condition.DENY, Name.CREATE_CATALOG);

    private GetUser(Condition condition, Name name) {
      super(condition, name);
    }

    /** @return The instance with allow condition of the privilege. */
    public static GetUser allow() {
      return ALLOW_INSTANCE;
    }

    /** @return The instance with deny condition of the privilege. */
    public static GetUser deny() {
      return DENY_INSTANCE;
    }
  }

  /** The privilege to add a user. */
  public static class AddUser extends GenericPrivilege<AddUser> {
    private static final AddUser ALLOW_INSTANCE = new AddUser(Condition.ALLOW, Name.CREATE_CATALOG);
    private static final AddUser DENY_INSTANCE = new AddUser(Condition.DENY, Name.CREATE_CATALOG);

    private AddUser(Condition condition, Name name) {
      super(condition, name);
    }

    /** @return The instance with allow condition of the privilege. */
    public static AddUser allow() {
      return ALLOW_INSTANCE;
    }

    /** @return The instance with deny condition of the privilege. */
    public static AddUser deny() {
      return DENY_INSTANCE;
    }
  }

  /** The privilege to remove a user. */
  public static class RemoveUser extends GenericPrivilege<RemoveUser> {
    private static final RemoveUser ALLOW_INSTANCE =
        new RemoveUser(Condition.ALLOW, Name.CREATE_CATALOG);
    private static final RemoveUser DENY_INSTANCE =
        new RemoveUser(Condition.DENY, Name.CREATE_CATALOG);

    private RemoveUser(Condition condition, Name name) {
      super(condition, name);
    }

    /** @return The instance with allow condition of the privilege. */
    public static RemoveUser allow() {
      return ALLOW_INSTANCE;
    }

    /** @return The instance with deny condition of the privilege. */
    public static RemoveUser deny() {
      return DENY_INSTANCE;
    }
  }

  /** The privilege to add a group. */
  public static class AddGroup extends GenericPrivilege<AddGroup> {
    private static final AddGroup ALLOW_INSTANCE =
        new AddGroup(Condition.ALLOW, Name.CREATE_CATALOG);
    private static final AddGroup DENY_INSTANCE = new AddGroup(Condition.DENY, Name.CREATE_CATALOG);

    private AddGroup(Condition condition, Name name) {
      super(condition, name);
    }

    /** @return The instance with allow condition of the privilege. */
    public static AddGroup allow() {
      return ALLOW_INSTANCE;
    }

    /** @return The instance with deny condition of the privilege. */
    public static AddGroup deny() {
      return DENY_INSTANCE;
    }
  }

  /** The privilege to remove a group. */
  public static class RemoveGroup extends GenericPrivilege<RemoveGroup> {
    private static final RemoveGroup ALLOW_INSTANCE =
        new RemoveGroup(Condition.ALLOW, Name.CREATE_CATALOG);
    private static final RemoveGroup DENY_INSTANCE =
        new RemoveGroup(Condition.DENY, Name.CREATE_CATALOG);

    private RemoveGroup(Condition condition, Name name) {
      super(condition, name);
    }

    /** @return The instance with allow condition of the privilege. */
    public static RemoveGroup allow() {
      return ALLOW_INSTANCE;
    }

    /** @return The instance with deny condition of the privilege. */
    public static RemoveGroup deny() {
      return DENY_INSTANCE;
    }
  }

  /** The privilege to get a group. */
  public static class GetGroup extends GenericPrivilege<GetGroup> {
    private static final GetGroup ALLOW_INSTANCE =
        new GetGroup(Condition.ALLOW, Name.CREATE_CATALOG);
    private static final GetGroup DENY_INSTANCE = new GetGroup(Condition.DENY, Name.CREATE_CATALOG);

    private GetGroup(Condition condition, Name name) {
      super(condition, name);
    }

    /** @return The instance with allow condition of the privilege. */
    public static GetGroup allow() {
      return ALLOW_INSTANCE;
    }

    /** @return The instance with deny condition of the privilege. */
    public static GetGroup deny() {
      return DENY_INSTANCE;
    }
  }

  /** The privilege to create a role. */
  public static class CreateRole extends GenericPrivilege<CreateRole> {
    private static final CreateRole ALLOW_INSTANCE =
        new CreateRole(Condition.ALLOW, Name.CREATE_CATALOG);
    private static final CreateRole DENY_INSTANCE =
        new CreateRole(Condition.DENY, Name.CREATE_CATALOG);

    private CreateRole(Condition condition, Name name) {
      super(condition, name);
    }

    /** @return The instance with allow condition of the privilege. */
    public static CreateRole allow() {
      return ALLOW_INSTANCE;
    }

    /** @return The instance with deny condition of the privilege. */
    public static CreateRole deny() {
      return DENY_INSTANCE;
    }
  }

  /** The privilege to get a role. */
  public static class GetRole extends GenericPrivilege<GetRole> {
    private static final GetRole ALLOW_INSTANCE = new GetRole(Condition.ALLOW, Name.CREATE_CATALOG);
    private static final GetRole DENY_INSTANCE = new GetRole(Condition.DENY, Name.CREATE_CATALOG);

    private GetRole(Condition condition, Name name) {
      super(condition, name);
    }

    /** @return The instance with allow condition of the privilege. */
    public static GetRole allow() {
      return ALLOW_INSTANCE;
    }

    /** @return The instance with deny condition of the privilege. */
    public static GetRole deny() {
      return DENY_INSTANCE;
    }
  }

  /** The privilege to delete a role. */
  public static class DeleteRole extends GenericPrivilege<DeleteRole> {
    private static final DeleteRole ALLOW_INSTANCE =
        new DeleteRole(Condition.ALLOW, Name.CREATE_CATALOG);
    private static final DeleteRole DENY_INSTANCE =
        new DeleteRole(Condition.DENY, Name.CREATE_CATALOG);

    private DeleteRole(Condition condition, Name name) {
      super(condition, name);
    }

    /** @return The instance with allow condition of the privilege. */
    public static DeleteRole allow() {
      return ALLOW_INSTANCE;
    }

    /** @return The instance with deny condition of the privilege. */
    public static DeleteRole deny() {
      return DENY_INSTANCE;
    }
  }

  /** The privilege to grant a role to the user or the group. */
  public static class GrantRole extends GenericPrivilege<GrantRole> {
    private static final GrantRole ALLOW_INSTANCE =
        new GrantRole(Condition.ALLOW, Name.CREATE_CATALOG);
    private static final GrantRole DENY_INSTANCE =
        new GrantRole(Condition.DENY, Name.CREATE_CATALOG);

    private GrantRole(Condition condition, Name name) {
      super(condition, name);
    }

    /** @return The instance with allow condition of the privilege. */
    public static GrantRole allow() {
      return ALLOW_INSTANCE;
    }

    /** @return The instance with deny condition of the privilege. */
    public static GrantRole deny() {
      return DENY_INSTANCE;
    }
  }

  /** The privilege to revoke a role from the user or the group. */
  public static class RevokeRole extends GenericPrivilege<RevokeRole> {
    private static final RevokeRole ALLOW_INSTANCE =
        new RevokeRole(Condition.ALLOW, Name.CREATE_CATALOG);
    private static final RevokeRole DENY_INSTANCE =
        new RevokeRole(Condition.DENY, Name.CREATE_CATALOG);

    private RevokeRole(Condition condition, Name name) {
      super(condition, name);
    }

    /** @return The instance with allow condition of the privilege. */
    public static RevokeRole allow() {
      return ALLOW_INSTANCE;
    }

    /** @return The instance with deny condition of the privilege. */
    public static RevokeRole deny() {
      return DENY_INSTANCE;
    }
  }
}
