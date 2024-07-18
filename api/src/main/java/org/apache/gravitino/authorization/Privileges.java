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
package org.apache.gravitino.authorization;

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
      case USE_CATALOG:
        return UseCatalog.allow();

        // Schema
      case CREATE_SCHEMA:
        return CreateSchema.allow();
      case USE_SCHEMA:
        return UseSchema.allow();

        // Table
      case CREATE_TABLE:
        return CreateTable.allow();
      case MODIFY_TABLE:
        return ModifyTable.allow();
      case SELECT_TABLE:
        return SelectTable.allow();

        // Fileset
      case CREATE_FILESET:
        return CreateFileset.allow();
      case WRITE_FILESET:
        return WriteFileset.allow();
      case READ_FILESET:
        return ReadFileset.allow();

        // Topic
      case CREATE_TOPIC:
        return CreateTopic.allow();
      case PRODUCE_TOPIC:
        return ProduceTopic.allow();
      case CONSUME_TOPIC:
        return ConsumeTopic.allow();

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
      case USE_CATALOG:
        return UseCatalog.deny();

        // Schema
      case CREATE_SCHEMA:
        return CreateSchema.deny();
      case USE_SCHEMA:
        return UseSchema.deny();

        // Table
      case CREATE_TABLE:
        return CreateTable.deny();
      case MODIFY_TABLE:
        return ModifyTable.deny();
      case SELECT_TABLE:
        return SelectTable.deny();

        // Fileset
      case CREATE_FILESET:
        return CreateFileset.deny();
      case WRITE_FILESET:
        return WriteFileset.deny();
      case READ_FILESET:
        return ReadFileset.deny();

        // Topic
      case CREATE_TOPIC:
        return CreateTopic.deny();
      case PRODUCE_TOPIC:
        return ProduceTopic.deny();
      case CONSUME_TOPIC:
        return ConsumeTopic.deny();

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

  /** The privilege to use a catalog. */
  public static class UseCatalog extends GenericPrivilege<UseCatalog> {
    private static final UseCatalog ALLOW_INSTANCE =
        new UseCatalog(Condition.ALLOW, Name.USE_CATALOG);
    private static final UseCatalog DENY_INSTANCE =
        new UseCatalog(Condition.DENY, Name.USE_CATALOG);

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
    private static final UseSchema ALLOW_INSTANCE = new UseSchema(Condition.ALLOW, Name.USE_SCHEMA);
    private static final UseSchema DENY_INSTANCE = new UseSchema(Condition.DENY, Name.USE_SCHEMA);

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
        new CreateSchema(Condition.ALLOW, Name.CREATE_SCHEMA);
    private static final CreateSchema DENY_INSTANCE =
        new CreateSchema(Condition.DENY, Name.CREATE_SCHEMA);

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

  /** The privilege to create a table. */
  public static class CreateTable extends GenericPrivilege<CreateTable> {
    private static final CreateTable ALLOW_INSTANCE =
        new CreateTable(Condition.ALLOW, Name.CREATE_TABLE);
    private static final CreateTable DENY_INSTANCE =
        new CreateTable(Condition.DENY, Name.CREATE_TABLE);

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

  /** The privilege to select data from a table. */
  public static class SelectTable extends GenericPrivilege<SelectTable> {
    private static final SelectTable ALLOW_INSTANCE =
        new SelectTable(Condition.ALLOW, Name.SELECT_TABLE);
    private static final SelectTable DENY_INSTANCE =
        new SelectTable(Condition.DENY, Name.SELECT_TABLE);

    private SelectTable(Condition condition, Name name) {
      super(condition, name);
    }

    /** @return The instance with allow condition of the privilege. */
    public static SelectTable allow() {
      return ALLOW_INSTANCE;
    }

    /** @return The instance with deny condition of the privilege. */
    public static SelectTable deny() {
      return DENY_INSTANCE;
    }
  }

  /** The privilege to alter, insert, update, or delete a table. */
  public static class ModifyTable extends GenericPrivilege<ModifyTable> {
    private static final ModifyTable ALLOW_INSTANCE =
        new ModifyTable(Condition.ALLOW, Name.MODIFY_TABLE);
    private static final ModifyTable DENY_INSTANCE =
        new ModifyTable(Condition.DENY, Name.MODIFY_TABLE);

    private ModifyTable(Condition condition, Name name) {
      super(condition, name);
    }

    /** @return The instance with allow condition of the privilege. */
    public static ModifyTable allow() {
      return ALLOW_INSTANCE;
    }

    /** @return The instance with deny condition of the privilege. */
    public static ModifyTable deny() {
      return DENY_INSTANCE;
    }
  }

  /** The privilege to create a fileset. */
  public static class CreateFileset extends GenericPrivilege<CreateFileset> {
    private static final CreateFileset ALLOW_INSTANCE =
        new CreateFileset(Condition.ALLOW, Name.CREATE_FILESET);
    private static final CreateFileset DENY_INSTANCE =
        new CreateFileset(Condition.DENY, Name.CREATE_FILESET);

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

  /** The privilege to read a fileset. */
  public static class ReadFileset extends GenericPrivilege<ReadFileset> {
    private static final ReadFileset ALLOW_INSTANCE =
        new ReadFileset(Condition.ALLOW, Name.READ_FILESET);
    private static final ReadFileset DENY_INSTANCE =
        new ReadFileset(Condition.DENY, Name.READ_FILESET);

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
        new WriteFileset(Condition.ALLOW, Name.WRITE_FILESET);
    private static final WriteFileset DENY_INSTANCE =
        new WriteFileset(Condition.DENY, Name.WRITE_FILESET);

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
        new CreateTopic(Condition.ALLOW, Name.CREATE_TOPIC);
    private static final CreateTopic DENY_INSTANCE =
        new CreateTopic(Condition.DENY, Name.CREATE_TOPIC);

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

  /** The privilege to consume a topic. */
  public static class ConsumeTopic extends GenericPrivilege<ConsumeTopic> {
    private static final ConsumeTopic ALLOW_INSTANCE =
        new ConsumeTopic(Condition.ALLOW, Name.CONSUME_TOPIC);
    private static final ConsumeTopic DENY_INSTANCE =
        new ConsumeTopic(Condition.DENY, Name.CONSUME_TOPIC);

    private ConsumeTopic(Condition condition, Name name) {
      super(condition, name);
    }

    /** @return The instance with allow condition of the privilege. */
    public static ConsumeTopic allow() {
      return ALLOW_INSTANCE;
    }

    /** @return The instance with deny condition of the privilege. */
    public static ConsumeTopic deny() {
      return DENY_INSTANCE;
    }
  }

  /** The privilege to produce a topic. */
  public static class ProduceTopic extends GenericPrivilege<ProduceTopic> {
    private static final ProduceTopic ALLOW_INSTANCE =
        new ProduceTopic(Condition.ALLOW, Name.PRODUCE_TOPIC);
    private static final ProduceTopic DENY_INSTANCE =
        new ProduceTopic(Condition.DENY, Name.PRODUCE_TOPIC);

    private ProduceTopic(Condition condition, Name name) {
      super(condition, name);
    }

    /** @return The instance with allow condition of the privilege. */
    public static ProduceTopic allow() {
      return ALLOW_INSTANCE;
    }

    /** @return The instance with deny condition of the privilege. */
    public static ProduceTopic deny() {
      return DENY_INSTANCE;
    }
  }

  /** The privilege to get a user. */
  public static class GetUser extends GenericPrivilege<GetUser> {
    private static final GetUser ALLOW_INSTANCE = new GetUser(Condition.ALLOW, Name.GET_USER);
    private static final GetUser DENY_INSTANCE = new GetUser(Condition.DENY, Name.GET_USER);

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
    private static final AddUser ALLOW_INSTANCE = new AddUser(Condition.ALLOW, Name.ADD_USER);
    private static final AddUser DENY_INSTANCE = new AddUser(Condition.DENY, Name.ADD_USER);

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
        new RemoveUser(Condition.ALLOW, Name.REMOVE_USER);
    private static final RemoveUser DENY_INSTANCE =
        new RemoveUser(Condition.DENY, Name.REMOVE_USER);

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
    private static final AddGroup ALLOW_INSTANCE = new AddGroup(Condition.ALLOW, Name.ADD_GROUP);
    private static final AddGroup DENY_INSTANCE = new AddGroup(Condition.DENY, Name.ADD_GROUP);

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
        new RemoveGroup(Condition.ALLOW, Name.REMOVE_GROUP);
    private static final RemoveGroup DENY_INSTANCE =
        new RemoveGroup(Condition.DENY, Name.REMOVE_GROUP);

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
        new CreateRole(Condition.ALLOW, Name.CREATE_ROLE);
    private static final CreateRole DENY_INSTANCE =
        new CreateRole(Condition.DENY, Name.CREATE_ROLE);

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
    private static final GetRole ALLOW_INSTANCE = new GetRole(Condition.ALLOW, Name.GET_ROLE);
    private static final GetRole DENY_INSTANCE = new GetRole(Condition.DENY, Name.GET_ROLE);

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
        new DeleteRole(Condition.ALLOW, Name.DELETE_ROLE);
    private static final DeleteRole DENY_INSTANCE =
        new DeleteRole(Condition.DENY, Name.DELETE_ROLE);

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
    private static final GrantRole ALLOW_INSTANCE = new GrantRole(Condition.ALLOW, Name.GRANT_ROLE);
    private static final GrantRole DENY_INSTANCE = new GrantRole(Condition.DENY, Name.GRANT_ROLE);

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
        new RevokeRole(Condition.ALLOW, Name.REVOKE_ROLE);
    private static final RevokeRole DENY_INSTANCE =
        new RevokeRole(Condition.DENY, Name.REVOKE_ROLE);

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
