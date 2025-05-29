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
package org.apache.gravitino.cli.outputs;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.gravitino.Audit;
import org.apache.gravitino.Catalog;
import org.apache.gravitino.Metalake;
import org.apache.gravitino.Schema;
import org.apache.gravitino.authorization.Group;
import org.apache.gravitino.authorization.Privilege;
import org.apache.gravitino.authorization.Role;
import org.apache.gravitino.authorization.SecurableObject;
import org.apache.gravitino.authorization.User;
import org.apache.gravitino.cli.CommandContext;
import org.apache.gravitino.file.Fileset;
import org.apache.gravitino.messaging.Topic;
import org.apache.gravitino.model.Model;
import org.apache.gravitino.rel.Column;
import org.apache.gravitino.rel.Table;
import org.apache.gravitino.tag.Tag;

/** Plain format to print a pretty string to standard out. */
public abstract class PlainFormat<T> extends BaseOutputFormat<T> {

  /**
   * Routes the object to its appropriate formatter and outputs the formatted result. Creates a new
   * formatter instance for the given object type and delegates the formatting.
   *
   * @param entity The object to format
   * @param context The command context
   * @throws IllegalArgumentException if the object type is not supported
   */
  public static void output(Object entity, CommandContext context) {

    if (entity instanceof Metalake) {
      new MetalakePlainFormat(context).output((Metalake) entity);
    } else if (entity instanceof Metalake[]) {
      new MetalakeListPlainFormat(context).output((Metalake[]) entity);
    } else if (entity instanceof Catalog) {
      new CatalogPlainFormat(context).output((Catalog) entity);
    } else if (entity instanceof Catalog[]) {
      new CatalogListPlainFormat(context).output((Catalog[]) entity);
    } else if (entity instanceof Schema) {
      new SchemaPlainFormat(context).output((Schema) entity);
    } else if (entity instanceof Schema[]) {
      new SchemaListPlainFormat(context).output((Schema[]) entity);
    } else if (entity instanceof Table) {
      new TablePlainFormat(context).output((Table) entity);
    } else if (entity instanceof Table[]) {
      new TableListPlainFormat(context).output((Table[]) entity);
    } else if (entity instanceof Model) {
      new ModelDetailPlainFormat(context).output((Model) entity);
    } else if (entity instanceof Model[]) {
      new ModelListPlainFormat(context).output((Model[]) entity);
    } else if (entity instanceof User) {
      new UserDetailsPlainFormat(context).output((User) entity);
    } else if (entity instanceof User[]) {
      new UserListPlainFormat(context).output((User[]) entity);
    } else if (entity instanceof Group) {
      new GroupDetailsPlainFormat(context).output((Group) entity);
    } else if (entity instanceof Group[]) {
      new GroupListPlainFormat(context).output((Group[]) entity);
    } else if (entity instanceof Audit) {
      new AuditPlainFormat(context).output((Audit) entity);
    } else if (entity instanceof Column[]) {
      new ColumnListPlainFormat(context).output((Column[]) entity);
    } else if (entity instanceof Role) {
      new RoleDetailsPlainFormat(context).output((Role) entity);
    } else if (entity instanceof Role[]) {
      new RoleListPlainFormat(context).output((Role[]) entity);
    } else if (entity instanceof Fileset) {
      new FilesetDetailsPlainFormat(context).output((Fileset) entity);
    } else if (entity instanceof Fileset[]) {
      new FilesetListPlainFormat(context).output((Fileset[]) entity);
    } else if (entity instanceof Topic) {
      new TopicDetailsPlainFormat(context).output((Topic) entity);
    } else if (entity instanceof Topic[]) {
      new TopicListPlainFormat(context).output((Topic[]) entity);
    } else if (entity instanceof Tag) {
      new TagDetailsPlainFormat(context).output((Tag) entity);
    } else if (entity instanceof Tag[]) {
      new TagListPlainFormat(context).output((Tag[]) entity);
    } else if (entity instanceof Map) {
      new PropertiesListPlainFormat(context).output((Map<?, ?>) entity);
    } else {
      throw new IllegalArgumentException(
          "Unsupported object type: " + (entity == null ? "null" : entity.getClass().getName()));
    }
  }

  /**
   * Creates a new {@link PlainFormat} with the specified command context.
   *
   * @param context The command context.
   */
  protected PlainFormat(CommandContext context) {
    super(context);
  }

  /**
   * Formats a single {@link Metalake} instance as a comma-separated string. Output format: name,
   * comment
   */
  static final class MetalakePlainFormat extends PlainFormat<Metalake> {

    public MetalakePlainFormat(CommandContext context) {
      super(context);
    }

    /** {@inheritDoc} */
    @Override
    public String getOutput(Metalake metalake) {
      return COMMA_JOINER.join(metalake.name(), metalake.comment());
    }
  }

  /**
   * Formats an array of Metalakes, outputting one name per line. Returns null if the array is empty
   * or null.
   */
  static final class MetalakeListPlainFormat extends PlainFormat<Metalake[]> {

    public MetalakeListPlainFormat(CommandContext context) {
      super(context);
    }

    /** {@inheritDoc} */
    @Override
    public String getOutput(Metalake[] metalakes) {
      List<String> metalakeNames =
          Arrays.stream(metalakes).map(Metalake::name).collect(Collectors.toList());
      return NEWLINE_JOINER.join(metalakeNames);
    }
  }

  /**
   * Formats a single {@link Catalog} instance as a comma-separated string. Output format: name,
   * type, provider, comment
   */
  static final class CatalogPlainFormat extends PlainFormat<Catalog> {
    public CatalogPlainFormat(CommandContext context) {
      super(context);
    }

    /** {@inheritDoc} */
    @Override
    public String getOutput(Catalog catalog) {
      return COMMA_JOINER.join(
          catalog.name(), catalog.type(), catalog.provider(), catalog.comment());
    }
  }

  /**
   * Formats an array of Catalogs, outputting one name per line. Returns null if the array is empty
   * or null.
   */
  static final class CatalogListPlainFormat extends PlainFormat<Catalog[]> {
    public CatalogListPlainFormat(CommandContext context) {
      super(context);
    }

    /** {@inheritDoc} */
    @Override
    public String getOutput(Catalog[] catalogs) {

      List<String> catalogNames =
          Arrays.stream(catalogs).map(Catalog::name).collect(Collectors.toList());
      return NEWLINE_JOINER.join(catalogNames);
    }
  }

  /**
   * Formats a single {@link Schema} instance as a comma-separated string. Output format: name,
   * comment
   */
  static final class SchemaPlainFormat extends PlainFormat<Schema> {
    public SchemaPlainFormat(CommandContext context) {
      super(context);
    }

    /** {@inheritDoc} */
    @Override
    public String getOutput(Schema schema) {
      return COMMA_JOINER.join(schema.name(), schema.comment());
    }
  }

  /**
   * Formats an array of Schemas, outputting one name per line. Returns null if the array is empty
   * or null.
   */
  static final class SchemaListPlainFormat extends PlainFormat<Schema[]> {
    public SchemaListPlainFormat(CommandContext context) {
      super(context);
    }

    /** {@inheritDoc} */
    @Override
    public String getOutput(Schema[] schemas) {
      List<String> schemaNames =
          Arrays.stream(schemas).map(Schema::name).collect(Collectors.toList());
      return NEWLINE_JOINER.join(schemaNames);
    }
  }

  /**
   * Formats a single Table instance with detailed column information. Output format: table_name,
   * table_comment
   */
  static final class TablePlainFormat extends PlainFormat<Table> {
    public TablePlainFormat(CommandContext context) {
      super(context);
    }

    /** {@inheritDoc} */
    @Override
    public String getOutput(Table table) {
      String comment = table.comment() == null ? "N/A" : table.comment();
      return COMMA_JOINER.join(new String[] {table.name(), comment});
    }
  }

  /**
   * Formats an array of Tables, outputting one name per line. Returns null if the array is empty or
   * null.
   */
  static final class TableListPlainFormat extends PlainFormat<Table[]> {
    public TableListPlainFormat(CommandContext context) {
      super(context);
    }

    /** {@inheritDoc} */
    @Override
    public String getOutput(Table[] tables) {
      List<String> tableNames = Arrays.stream(tables).map(Table::name).collect(Collectors.toList());
      return NEWLINE_JOINER.join(tableNames);
    }
  }

  /**
   * Formats an instance of {@link Audit} , outputting the audit information. Output format:
   * creator, create_time, modified, modified_time
   */
  static final class AuditPlainFormat extends PlainFormat<Audit> {
    public AuditPlainFormat(CommandContext context) {
      super(context);
    }

    /** {@inheritDoc} */
    @Override
    public String getOutput(Audit audit) {
      return COMMA_JOINER.join(
          audit.creator(),
          audit.createTime() == null ? "N/A" : audit.createTime(),
          audit.lastModifier() == null ? "N/A" : audit.lastModifier(),
          audit.lastModifiedTime() == null ? "N/A" : audit.lastModifiedTime());
    }
  }

  /**
   * Formats an array of {@link org.apache.gravitino.rel.Column} into a six-column table display.
   * Lists all column names, types, default values, auto-increment, nullable, and comments in a
   * plain format.
   */
  static final class ColumnListPlainFormat extends PlainFormat<Column[]> {

    /**
     * Creates a new {@link ColumnListPlainFormat} with the specified command context.
     *
     * @param context The command context.
     */
    public ColumnListPlainFormat(CommandContext context) {
      super(context);
    }

    /** {@inheritDoc} */
    @Override
    public String getOutput(Column[] columns) {
      String header =
          COMMA_JOINER.join(
              "name", "datatype", "default_value", "comment", "nullable", "auto_increment");
      StringBuilder data = new StringBuilder();
      for (int i = 0; i < columns.length; i++) {
        String name = columns[i].name();
        String dataType = columns[i].dataType().simpleString();
        String defaultValue = LineUtil.getDefaultValue(columns[i]);
        String comment = LineUtil.getComment(columns[i]);
        String nullable = columns[i].nullable() ? "true" : "false";
        String autoIncrement = LineUtil.getAutoIncrement(columns[i]);

        data.append(
            COMMA_JOINER.join(name, dataType, defaultValue, comment, nullable, autoIncrement));
        data.append(System.lineSeparator());
      }
      return NEWLINE_JOINER.join(header, data.toString());
    }
  }

  /**
   * Format a {@link Model} instance with detailed information. Output format: model_name,
   * model_comment and latest_version
   */
  static final class ModelDetailPlainFormat extends PlainFormat<Model> {

    /**
     * Creates a new {@link PlainFormat} with the specified command context.
     *
     * @param context the {@link CommandContext} instance.
     */
    public ModelDetailPlainFormat(CommandContext context) {
      super(context);
    }

    /** {@inheritDoc} */
    @Override
    public String getOutput(Model model) {
      return String.format(
          "Model name %s, latest version: %s%n", model.name(), model.latestVersion());
    }
  }

  /** Format an array of {@link Model} instances with their names. Output format: model_name */
  static final class ModelListPlainFormat extends PlainFormat<Model[]> {

    /**
     * Creates a new {@link PlainFormat} with the specified command context.
     *
     * @param context the {@link CommandContext} instance.
     */
    public ModelListPlainFormat(CommandContext context) {
      super(context);
    }

    /** {@inheritDoc} */
    @Override
    public String getOutput(Model[] models) {
      return COMMA_JOINER.join(Arrays.stream(models).map(Model::name).collect(Collectors.toList()));
    }
  }

  /** Format a {@link User} instance with their details. Output format: username, role */
  static final class UserDetailsPlainFormat extends PlainFormat<User> {

    /**
     * Creates a new {@link PlainFormat} with the specified command context.
     *
     * @param context the {@link CommandContext} instance.
     */
    public UserDetailsPlainFormat(CommandContext context) {
      super(context);
    }

    /** {@inheritDoc} */
    @Override
    public String getOutput(User user) {
      return COMMA_JOINER.join(user.roles());
    }
  }

  /** Format an array of {@link User} instances with their names. Output format: username */
  static final class UserListPlainFormat extends PlainFormat<User[]> {

    /**
     * Creates a new {@link PlainFormat} with the specified command context.
     *
     * @param context the {@link CommandContext} instance.
     */
    public UserListPlainFormat(CommandContext context) {
      super(context);
    }

    /** {@inheritDoc} */
    @Override
    public String getOutput(User[] users) {
      return COMMA_JOINER.join(Arrays.stream(users).map(User::name).collect(Collectors.toList()));
    }
  }

  /** Format a {@link Group} instance with their details. Output format: group name, role */
  static final class GroupDetailsPlainFormat extends PlainFormat<Group> {
    /**
     * Constructs a new {@link GroupDetailsPlainFormat} instance.
     *
     * @param context the {@link CommandContext} instance.
     */
    public GroupDetailsPlainFormat(CommandContext context) {
      super(context);
    }

    /** {@inheritDoc} */
    @Override
    public String getOutput(Group group) {
      return COMMA_JOINER.join(group.roles());
    }
  }

  /** Format an array of {@link Group} instances with their names. Output format: group name */
  static final class GroupListPlainFormat extends PlainFormat<Group[]> {
    /**
     * Constructs a new {@link GroupListPlainFormat} instance.
     *
     * @param context the {@link CommandContext} instance.
     */
    public GroupListPlainFormat(CommandContext context) {
      super(context);
    }

    /** {@inheritDoc} */
    @Override
    public String getOutput(Group[] groups) {
      return COMMA_JOINER.join(Arrays.stream(groups).map(Group::name).collect(Collectors.toList()));
    }
  }

  /**
   * Formats detail information of {@link org.apache.gravitino.tag.Tag}. Output format: name,
   * comment
   */
  static final class TagDetailsPlainFormat extends PlainFormat<Tag> {

    /**
     * Creates a new {@link TagDetailsPlainFormat}.
     *
     * @param context The command context.
     */
    public TagDetailsPlainFormat(CommandContext context) {
      super(context);
    }

    /** {@inheritDoc} */
    @Override
    public String getOutput(Tag tag) {
      String comment = tag.comment() == null ? "N/A" : tag.comment();
      return COMMA_JOINER.join(tag.name(), comment);
    }
  }

  /** Formats array of {@link org.apache.gravitino.tag.Tag} information. Output format: name */
  static final class TagListPlainFormat extends PlainFormat<Tag[]> {

    /**
     * Creates a new {@link TagListPlainFormat}.
     *
     * @param context The command context.
     */
    public TagListPlainFormat(CommandContext context) {
      super(context);
    }

    /** {@inheritDoc} */
    @Override
    public String getOutput(Tag[] tags) {
      List<String> tagNames = Arrays.stream(tags).map(Tag::name).collect(Collectors.toList());
      return NEWLINE_JOINER.join(tagNames);
    }
  }

  /**
   * Formats information about properties of {@link org.apache.gravitino.tag.Tag}. Output format:
   * key, value
   */
  static final class PropertiesListPlainFormat extends PlainFormat<Map<?, ?>> {

    /**
     * Creates a new {@link PropertiesListPlainFormat}.
     *
     * @param context The command context.
     */
    public PropertiesListPlainFormat(CommandContext context) {
      super(context);
    }

    /** {@inheritDoc} */
    @Override
    public String getOutput(Map<?, ?> properties) {
      StringBuilder data = new StringBuilder();
      properties.forEach(
          (key, value) -> {
            data.append(COMMA_JOINER.join(key.toString(), value.toString()));
            data.append(System.lineSeparator());
          });

      return data.toString();
    }
  }

  /**
   * Format a {@link Role} instance with their details. Output format: securable object details
   * which belongs to the role,
   */
  static final class RoleDetailsPlainFormat extends PlainFormat<Role> {

    /**
     * Creates a new {@link PlainFormat} with the specified CommandContext.
     *
     * @param context The command context.
     */
    public RoleDetailsPlainFormat(CommandContext context) {
      super(context);
    }

    /** {@inheritDoc} */
    @Override
    public String getOutput(Role entity) {
      List<SecurableObject> objects = entity.securableObjects();
      StringBuilder sb = new StringBuilder();
      for (SecurableObject object : objects) {
        sb.append(object.name()).append(",").append(object.type()).append(",").append("\n");
        sb.append(
            String.join(
                "\n",
                object.privileges().stream().map(Privilege::simpleString).toArray(String[]::new)));
        sb.append("\n");
      }

      return sb.toString();
    }
  }

  /** Format an array of {@link Role} instances with their names. Output format: role name */
  static final class RoleListPlainFormat extends PlainFormat<Role[]> {
    /**
     * Creates a new {@link PlainFormat} with the specified CommandContext.
     *
     * @param context The command context.
     */
    public RoleListPlainFormat(CommandContext context) {
      super(context);
    }

    /** {@inheritDoc} */
    @Override
    public String getOutput(Role[] entity) {
      List<String> roleList = Arrays.stream(entity).map(Role::name).collect(Collectors.toList());
      return String.join(",", roleList);
    }
  }

  /**
   * Format a {@link Fileset} instance with their details. Output format: fileset name, type,
   * comment and storage location
   */
  static final class FilesetDetailsPlainFormat extends PlainFormat<Fileset> {

    /**
     * Creates a new {@link PlainFormat} with the specified CommandContext.
     *
     * @param context The command context.
     */
    public FilesetDetailsPlainFormat(CommandContext context) {
      super(context);
    }

    @Override
    public String getOutput(Fileset entity) {
      String filesetType = (entity.type() == Fileset.Type.MANAGED) ? "managed" : "external";
      return entity.name()
          + ","
          + filesetType
          + ","
          + entity.comment()
          + ","
          + entity.storageLocation();
    }
  }

  /** Format an array of {@link Fileset} instances with their names. Output format: fileset name */
  static final class FilesetListPlainFormat extends PlainFormat<Fileset[]> {

    /**
     * Creates a new {@link PlainFormat} with the specified CommandContext.
     *
     * @param context The command context.
     */
    public FilesetListPlainFormat(CommandContext context) {
      super(context);
    }

    /** {@inheritDoc} */
    @Override
    public String getOutput(Fileset[] entity) {
      List<String> filesetList =
          Arrays.stream(entity).map(Fileset::name).collect(Collectors.toList());
      return String.join(",", filesetList);
    }
  }

  /** Format a {@link Topic} instance with their details. Output format: topic name, comment */
  static final class TopicDetailsPlainFormat extends PlainFormat<Topic> {

    /**
     * Creates a new {@link PlainFormat} with the specified CommandContext.
     *
     * @param context The command context.
     */
    public TopicDetailsPlainFormat(CommandContext context) {
      super(context);
    }

    /** {@inheritDoc} */
    @Override
    public String getOutput(Topic entity) {
      return entity.name() + "," + entity.comment();
    }
  }

  /** Format an array of {@link Topic} instances with their names. Output format: topic name */
  static final class TopicListPlainFormat extends PlainFormat<Topic[]> {

    /**
     * Creates a new {@link PlainFormat} with the specified CommandContext.
     *
     * @param context The command context.
     */
    public TopicListPlainFormat(CommandContext context) {
      super(context);
    }

    /** {@inheritDoc} */
    @Override
    public String getOutput(Topic[] entity) {
      List<String> topicList = Arrays.stream(entity).map(Topic::name).collect(Collectors.toList());
      return String.join(",", topicList);
    }
  }
}
