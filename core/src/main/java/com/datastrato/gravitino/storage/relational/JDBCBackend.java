/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

package com.datastrato.gravitino.storage.relational;

import static com.datastrato.gravitino.Configs.GARBAGE_COLLECTOR_SINGLE_DELETION_LIMIT;

import com.datastrato.gravitino.Config;
import com.datastrato.gravitino.Configs;
import com.datastrato.gravitino.Entity;
import com.datastrato.gravitino.EntityAlreadyExistsException;
import com.datastrato.gravitino.HasIdentifier;
import com.datastrato.gravitino.NameIdentifier;
import com.datastrato.gravitino.Namespace;
import com.datastrato.gravitino.UnsupportedEntityTypeException;
import com.datastrato.gravitino.exceptions.AlreadyExistsException;
import com.datastrato.gravitino.exceptions.NoSuchEntityException;
import com.datastrato.gravitino.meta.BaseMetalake;
import com.datastrato.gravitino.meta.CatalogEntity;
import com.datastrato.gravitino.meta.FilesetEntity;
import com.datastrato.gravitino.meta.GroupEntity;
import com.datastrato.gravitino.meta.RoleEntity;
import com.datastrato.gravitino.meta.SchemaEntity;
import com.datastrato.gravitino.meta.TableEntity;
import com.datastrato.gravitino.meta.TopicEntity;
import com.datastrato.gravitino.meta.UserEntity;
import com.datastrato.gravitino.storage.relational.converters.SQLExceptionConverterFactory;
import com.datastrato.gravitino.storage.relational.service.CatalogMetaService;
import com.datastrato.gravitino.storage.relational.service.FilesetMetaService;
import com.datastrato.gravitino.storage.relational.service.GroupMetaService;
import com.datastrato.gravitino.storage.relational.service.MetalakeMetaService;
import com.datastrato.gravitino.storage.relational.service.RoleMetaService;
import com.datastrato.gravitino.storage.relational.service.SchemaMetaService;
import com.datastrato.gravitino.storage.relational.service.TableMetaService;
import com.datastrato.gravitino.storage.relational.service.TopicMetaService;
import com.datastrato.gravitino.storage.relational.service.UserMetaService;
import com.datastrato.gravitino.storage.relational.session.SqlSessionFactoryHelper;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.List;
import java.util.function.Function;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link JDBCBackend} is a jdbc implementation of {@link RelationalBackend} interface. You can use
 * a database that supports the JDBC protocol as storage. If the specified database has special SQL
 * syntax, please implement the SQL statements and methods in MyBatis Mapper separately and switch
 * according to the {@link Configs#ENTITY_RELATIONAL_JDBC_BACKEND_URL_KEY} parameter.
 */
public class JDBCBackend implements RelationalBackend {

  private static final Logger LOG = LoggerFactory.getLogger(JDBCBackend.class);

  /** Initialize the jdbc backend instance. */
  @Override
  public void initialize(Config config) {
    initJDBCBackend(config);

    SqlSessionFactoryHelper.getInstance().init(config);
    SQLExceptionConverterFactory.initConverter(config);
  }

  @Override
  public <E extends Entity & HasIdentifier> List<E> list(
      Namespace namespace, Entity.EntityType entityType) {
    switch (entityType) {
      case METALAKE:
        return (List<E>) MetalakeMetaService.getInstance().listMetalakes();
      case CATALOG:
        return (List<E>) CatalogMetaService.getInstance().listCatalogsByNamespace(namespace);
      case SCHEMA:
        return (List<E>) SchemaMetaService.getInstance().listSchemasByNamespace(namespace);
      case TABLE:
        return (List<E>) TableMetaService.getInstance().listTablesByNamespace(namespace);
      case FILESET:
        return (List<E>) FilesetMetaService.getInstance().listFilesetsByNamespace(namespace);
      case TOPIC:
        return (List<E>) TopicMetaService.getInstance().listTopicsByNamespace(namespace);
      default:
        throw new UnsupportedEntityTypeException(
            "Unsupported entity type: %s for list operation", entityType);
    }
  }

  @Override
  public boolean exists(NameIdentifier ident, Entity.EntityType entityType) {
    try {
      Entity entity = get(ident, entityType);
      return entity != null;
    } catch (NoSuchEntityException ne) {
      return false;
    }
  }

  @Override
  public <E extends Entity & HasIdentifier> void insert(E e, boolean overwritten)
      throws EntityAlreadyExistsException {
    if (e instanceof BaseMetalake) {
      MetalakeMetaService.getInstance().insertMetalake((BaseMetalake) e, overwritten);
    } else if (e instanceof CatalogEntity) {
      CatalogMetaService.getInstance().insertCatalog((CatalogEntity) e, overwritten);
    } else if (e instanceof SchemaEntity) {
      SchemaMetaService.getInstance().insertSchema((SchemaEntity) e, overwritten);
    } else if (e instanceof TableEntity) {
      TableMetaService.getInstance().insertTable((TableEntity) e, overwritten);
    } else if (e instanceof FilesetEntity) {
      FilesetMetaService.getInstance().insertFileset((FilesetEntity) e, overwritten);
    } else if (e instanceof TopicEntity) {
      TopicMetaService.getInstance().insertTopic((TopicEntity) e, overwritten);
    } else if (e instanceof UserEntity) {
      UserMetaService.getInstance().insertUser((UserEntity) e, overwritten);
    } else if (e instanceof RoleEntity) {
      RoleMetaService.getInstance().insertRole((RoleEntity) e, overwritten);
    } else if (e instanceof GroupEntity) {
      GroupMetaService.getInstance().insertGroup((GroupEntity) e, overwritten);
    } else {
      throw new UnsupportedEntityTypeException(
          "Unsupported entity type: %s for insert operation", e.getClass());
    }
  }

  @Override
  public <E extends Entity & HasIdentifier> E update(
      NameIdentifier ident, Entity.EntityType entityType, Function<E, E> updater)
      throws IOException, NoSuchEntityException, AlreadyExistsException {
    switch (entityType) {
      case METALAKE:
        return (E) MetalakeMetaService.getInstance().updateMetalake(ident, updater);
      case CATALOG:
        return (E) CatalogMetaService.getInstance().updateCatalog(ident, updater);
      case SCHEMA:
        return (E) SchemaMetaService.getInstance().updateSchema(ident, updater);
      case TABLE:
        return (E) TableMetaService.getInstance().updateTable(ident, updater);
      case FILESET:
        return (E) FilesetMetaService.getInstance().updateFileset(ident, updater);
      case TOPIC:
        return (E) TopicMetaService.getInstance().updateTopic(ident, updater);
      case USER:
        return (E) UserMetaService.getInstance().updateUser(ident, updater);
      case GROUP:
        return (E) GroupMetaService.getInstance().updateGroup(ident, updater);
      default:
        throw new UnsupportedEntityTypeException(
            "Unsupported entity type: %s for update operation", entityType);
    }
  }

  @Override
  public <E extends Entity & HasIdentifier> E get(
      NameIdentifier ident, Entity.EntityType entityType) throws NoSuchEntityException {
    switch (entityType) {
      case METALAKE:
        return (E) MetalakeMetaService.getInstance().getMetalakeByIdentifier(ident);
      case CATALOG:
        return (E) CatalogMetaService.getInstance().getCatalogByIdentifier(ident);
      case SCHEMA:
        return (E) SchemaMetaService.getInstance().getSchemaByIdentifier(ident);
      case TABLE:
        return (E) TableMetaService.getInstance().getTableByIdentifier(ident);
      case FILESET:
        return (E) FilesetMetaService.getInstance().getFilesetByIdentifier(ident);
      case TOPIC:
        return (E) TopicMetaService.getInstance().getTopicByIdentifier(ident);
      case USER:
        return (E) UserMetaService.getInstance().getUserByIdentifier(ident);
      case GROUP:
        return (E) GroupMetaService.getInstance().getGroupByIdentifier(ident);
      case ROLE:
        return (E) RoleMetaService.getInstance().getRoleByIdentifier(ident);
      default:
        throw new UnsupportedEntityTypeException(
            "Unsupported entity type: %s for get operation", entityType);
    }
  }

  @Override
  public boolean delete(NameIdentifier ident, Entity.EntityType entityType, boolean cascade) {
    switch (entityType) {
      case METALAKE:
        return MetalakeMetaService.getInstance().deleteMetalake(ident, cascade);
      case CATALOG:
        return CatalogMetaService.getInstance().deleteCatalog(ident, cascade);
      case SCHEMA:
        return SchemaMetaService.getInstance().deleteSchema(ident, cascade);
      case TABLE:
        return TableMetaService.getInstance().deleteTable(ident);
      case FILESET:
        return FilesetMetaService.getInstance().deleteFileset(ident);
      case TOPIC:
        return TopicMetaService.getInstance().deleteTopic(ident);
      case USER:
        return UserMetaService.getInstance().deleteUser(ident);
      case GROUP:
        return GroupMetaService.getInstance().deleteGroup(ident);
      case ROLE:
        return RoleMetaService.getInstance().deleteRole(ident);
      default:
        throw new UnsupportedEntityTypeException(
            "Unsupported entity type: %s for delete operation", entityType);
    }
  }

  @Override
  public int hardDeleteLegacyData(Entity.EntityType entityType, long legacyTimeline) {
    switch (entityType) {
      case METALAKE:
        return MetalakeMetaService.getInstance()
            .deleteMetalakeMetasByLegacyTimeline(
                legacyTimeline, GARBAGE_COLLECTOR_SINGLE_DELETION_LIMIT);
      case CATALOG:
        return CatalogMetaService.getInstance()
            .deleteCatalogMetasByLegacyTimeline(
                legacyTimeline, GARBAGE_COLLECTOR_SINGLE_DELETION_LIMIT);
      case SCHEMA:
        return SchemaMetaService.getInstance()
            .deleteSchemaMetasByLegacyTimeline(
                legacyTimeline, GARBAGE_COLLECTOR_SINGLE_DELETION_LIMIT);
      case TABLE:
        return TableMetaService.getInstance()
            .deleteTableMetasByLegacyTimeline(
                legacyTimeline, GARBAGE_COLLECTOR_SINGLE_DELETION_LIMIT);
      case FILESET:
        return FilesetMetaService.getInstance()
            .deleteFilesetAndVersionMetasByLegacyTimeline(
                legacyTimeline, GARBAGE_COLLECTOR_SINGLE_DELETION_LIMIT);
      case TOPIC:
        return TopicMetaService.getInstance()
            .deleteTopicMetasByLegacyTimeline(
                legacyTimeline, GARBAGE_COLLECTOR_SINGLE_DELETION_LIMIT);
      case USER:
        return UserMetaService.getInstance()
            .deleteUserMetasByLegacyTimeline(
                legacyTimeline, GARBAGE_COLLECTOR_SINGLE_DELETION_LIMIT);
      case GROUP:
        return GroupMetaService.getInstance()
            .deleteGroupMetasByLegacyTimeline(
                legacyTimeline, GARBAGE_COLLECTOR_SINGLE_DELETION_LIMIT);
      case ROLE:
        return RoleMetaService.getInstance()
            .deleteRoleMetasByLegacyTimeline(
                legacyTimeline, GARBAGE_COLLECTOR_SINGLE_DELETION_LIMIT);
      case COLUMN:
      case AUDIT:
        return 0;
        // TODO: Implement hard delete logic for these entity types.

      default:
        throw new IllegalArgumentException(
            "Unsupported entity type when collectAndRemoveLegacyData: " + entityType);
    }
  }

  @Override
  public int deleteOldVersionData(Entity.EntityType entityType, long versionRetentionCount) {
    switch (entityType) {
      case METALAKE:
      case CATALOG:
      case SCHEMA:
      case TABLE:
      case COLUMN:
      case TOPIC:
      case USER:
      case GROUP:
      case AUDIT:
      case ROLE:
        // These entity types have not implemented multi-versions, so we can skip.
        return 0;

      case FILESET:
        return FilesetMetaService.getInstance()
            .deleteFilesetVersionsByRetentionCount(
                versionRetentionCount, GARBAGE_COLLECTOR_SINGLE_DELETION_LIMIT);

      default:
        throw new IllegalArgumentException(
            "Unsupported entity type when collectAndRemoveOldVersionData: " + entityType);
    }
  }

  @Override
  public void close() throws IOException {
    SqlSessionFactoryHelper.getInstance().close();
  }

  enum JDBCBackendType {
    H2("h2"),
    DERBY("derby"),
    SQLITE("sqlite"),
    MYSQL("mysql");

    private final String name;

    JDBCBackendType(String name) {
      this.name = name;
    }

    public static JDBCBackendType fromName(String name) {
      for (JDBCBackendType backend : JDBCBackendType.values()) {
        if (backend.name.equalsIgnoreCase(name)) {
          return backend;
        }
      }

      throw new IllegalArgumentException("Unknown EmbeddedJDBCBackend: " + name);
    }
  }

  private static void initJDBCBackend(Config config) {

    // Default username and password for embedded database like H2
    String jdbcUserName = "gravitino";
    String jdbcPassword = "gravitino";

    String storagePath = getStoragePath(config);

    LOG.info("Embedded path: {}", storagePath);
    String jdbcUrl = config.get(Configs.ENTITY_RELATIONAL_JDBC_BACKEND_URL);
    String jdbcType = getJDBCType(jdbcUrl);

    JDBCBackendType jdbcBackend = JDBCBackendType.fromName(jdbcType);

    switch (jdbcBackend) {
      case H2:
        initH2Backend(config, jdbcUserName, jdbcPassword, storagePath);
        break;
      case DERBY:
      case SQLITE:
        throw new UnsupportedOperationException(jdbcPassword + " is not supported yet.");
      case MYSQL:
        break;
      default:
        throw new IllegalArgumentException("Unknown EmbeddedJDBCBackend: " + jdbcBackend);
    }
  }

  private static String getJDBCType(String jdbcURI) {
    if (jdbcURI.startsWith("jdbc:h2")) {
      return "h2";
    } else if (jdbcURI.startsWith("jdbc:derby")) {
      return "derby";
    } else if (jdbcURI.startsWith("jdbc:sqlite")) {
      return "sqlite";
    } else if (jdbcURI.startsWith("jdbc:mysql")) {
      return "mysql";
    } else {
      throw new IllegalArgumentException("Unknown JDBC URI: " + jdbcURI);
    }
  }

  private static void initH2Backend(
      Config config, String h2UserName, String h2UserPassword, String storagePath) {
    String gravitinoHome = System.getenv("GRAVITINO_HOME");
    String originalJDBCUrl = config.get(Configs.ENTITY_RELATIONAL_JDBC_BACKEND_URL);

    String connectionUrl = constructH2URI(originalJDBCUrl, storagePath);
    try (Connection connection =
            DriverManager.getConnection(connectionUrl, h2UserName, h2UserPassword);
        Statement statement = connection.createStatement()) {
      String sqlContent =
          FileUtils.readFileToString(
              new File(gravitinoHome + "/scripts/h2/schema-h2.sql"), StandardCharsets.UTF_8);

      statement.execute(sqlContent);
    } catch (Exception e) {
      LOG.error("Failed to create table for H2 database.", e);
      throw new RuntimeException("Failed to create table for H2 database.", e);
    }

    config.set(Configs.ENTITY_RELATIONAL_JDBC_BACKEND_URL, connectionUrl);
    config.set(Configs.ENTITY_RELATIONAL_JDBC_BACKEND_USER, h2UserName);
    config.set(Configs.ENTITY_RELATIONAL_JDBC_BACKEND_PASSWORD, h2UserPassword);
    config.set(Configs.ENTITY_RELATIONAL_JDBC_BACKEND_DRIVER, "org.h2.Driver");
  }

  private static String constructH2URI(String originURI, String storagePath) {
    if (!originURI.contains(":file:")) {
      originURI = "jdbc:h2:file:" + storagePath;
    }

    if (!originURI.contains("DB_CLOSE_DELAY")) {
      originURI = originURI + ";DB_CLOSE_DELAY=-1";
    }

    if (!originURI.contains("MODE")) {
      originURI = originURI + ";MODE=MYSQL";
    }

    originURI = originURI + ";AUTO_SERVER=TRUE";

    return originURI;
  }

  static String getStoragePath(Config config) {
    String dbPath = config.get(Configs.ENTRY_RELATIONAL_JDBC_BACKEND_PATH);
    if (StringUtils.isBlank(dbPath)) {
      return Configs.DEFAULT_RELATIONAL_JDBC_BACKEND_PATH;
    }

    Path path = Paths.get(dbPath);
    // Relative Path
    if (!path.isAbsolute()) {
      path = Paths.get(System.getenv("GRAVITINO_HOME"), dbPath);
      return path.toString();
    }

    return dbPath;
  }
}
