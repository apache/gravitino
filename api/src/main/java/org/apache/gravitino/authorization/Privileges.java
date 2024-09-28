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
      case MANAGE_USERS:
        return ManageUsers.allow();

        // Group
      case MANAGE_GROUPS:
        return ManageGroups.allow();

        // Role
      case CREATE_ROLE:
        return CreateRole.allow();
      case MANAGE_GRANTS:
        return ManageGrants.allow();

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
      case MANAGE_USERS:
        return ManageUsers.deny();

        // Group
      case MANAGE_GROUPS:
        return ManageGroups.deny();

        // Role
      case CREATE_ROLE:
        return CreateRole.deny();
      case MANAGE_GRANTS:
        return ManageGrants.deny();

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

  /** The privilege to execute SQL `ALTER`, `INSERT`, `UPDATE`, or `DELETE` for a table. */
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

  /** The privilege to consume from a topic. */
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

  /** The privilege to produce to a topic. */
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

  /** The privilege to manage users. */
  public static class ManageUsers extends GenericPrivilege<ManageUsers> {
    private static final ManageUsers ALLOW_INSTANCE =
        new ManageUsers(Condition.ALLOW, Name.MANAGE_USERS);
    private static final ManageUsers DENY_INSTANCE =
        new ManageUsers(Condition.DENY, Name.MANAGE_USERS);

    private ManageUsers(Condition condition, Name name) {
      super(condition, name);
    }

    /** @return The instance with allow condition of the privilege. */
    public static ManageUsers allow() {
      return ALLOW_INSTANCE;
    }

    /** @return The instance with deny condition of the privilege. */
    public static ManageUsers deny() {
      return DENY_INSTANCE;
    }
  }

  /** The privilege to manage groups. */
  public static class ManageGroups extends GenericPrivilege<ManageGroups> {
    private static final ManageGroups ALLOW_INSTANCE =
        new ManageGroups(Condition.ALLOW, Name.MANAGE_GROUPS);
    private static final ManageGroups DENY_INSTANCE =
        new ManageGroups(Condition.DENY, Name.MANAGE_GROUPS);

    private ManageGroups(Condition condition, Name name) {
      super(condition, name);
    }

    /** @return The instance with allow condition of the privilege. */
    public static ManageGroups allow() {
      return ALLOW_INSTANCE;
    }

    /** @return The instance with deny condition of the privilege. */
    public static ManageGroups deny() {
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

  /** The privilege to grant or revoke a role for the user or the group. */
  public static class ManageGrants extends GenericPrivilege<ManageGrants> {
    private static final ManageGrants ALLOW_INSTANCE =
        new ManageGrants(Condition.ALLOW, Name.MANAGE_GRANTS);
    private static final ManageGrants DENY_INSTANCE =
        new ManageGrants(Condition.DENY, Name.MANAGE_GRANTS);

    private ManageGrants(Condition condition, Name name) {
      super(condition, name);
    }

    /** @return The instance with allow condition of the privilege. */
    public static ManageGrants allow() {
      return ALLOW_INSTANCE;
    }

    /** @return The instance with deny condition of the privilege. */
    public static ManageGrants deny() {
      return DENY_INSTANCE;
    }
  }
}
