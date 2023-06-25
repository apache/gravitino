package com.datastrato.graviton.connector.hive.json;

import com.datastrato.graviton.Entity;
import com.datastrato.graviton.Field;
import com.datastrato.graviton.Namespace;
import com.datastrato.graviton.meta.catalog.NoSuchNamespaceException;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.PrincipalType;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static com.datastrato.graviton.connector.hive.HiveConnector.isValidateNamespace;

public class HiveDatabase implements Entity {
  public static final Logger LOG = LoggerFactory.getLogger(HiveDatabase.class);
  public static final Field METADATA =
          Field.optional("metadata", Map.class, "The metadata of the database");
  public static final Field NAMESPACE =
          Field.optional("namespace", Namespace.class, "The namespace of the database");
  public static final Field CONFIGURATION =
          Field.optional("conf", Configuration.class, "The configuration of the hive server");


  public static final String HMS_DB_OWNER = "hive.metastore.database.owner";
  public static final String HMS_DB_OWNER_TYPE = "hive.metastore.database.owner-type";

  private Namespace namespace;

  private Map<String, String> metadata;
  private Configuration conf;

  private Database innerDatabase;

  public Database getInnerDatabase() {
    return innerDatabase;
  }

  @Override
  public Map<Field, Object> fields() {
    Map<Field, Object> fields = new HashMap<>();
    fields.put(METADATA, metadata);
    fields.put(NAMESPACE, namespace);
    fields.put(CONFIGURATION, conf);

    return Collections.unmodifiableMap(fields);
  }

  public static class Builder {
    private final HiveDatabase hiveDatabase;

    public Builder() {
      this.hiveDatabase = new HiveDatabase();
    }

    public HiveDatabase.Builder withConf(Configuration conf) {
      this.hiveDatabase.conf = conf;
      return this;
    }

    public HiveDatabase.Builder withHiveDatabase(Database database) {
      this.hiveDatabase.innerDatabase = database;
      return this;
    }

    public HiveDatabase.Builder withNamespace(Namespace namespace) {
      this.hiveDatabase.namespace = namespace;
      return this;
    }

    public HiveDatabase.Builder withMetadata(Map<String, String> metadata) {
      this.hiveDatabase.metadata = metadata;
      return this;
    }

    public HiveDatabase build() {
      if (null == this.hiveDatabase.innerDatabase) {
        this.hiveDatabase.innerDatabase = hiveDatabase.convertToHiveDatabase();
      }
      return hiveDatabase;
    }
  }

  private Database convertToHiveDatabase() {
    if (!isValidateNamespace(namespace)) {
      throw new NoSuchNamespaceException("Namespace does not exist: " + namespace);
    }

    Database database = new Database();
    Map<String, String> parameter = Maps.newHashMap();

    database.setName(namespace.level(0));
    database.setLocationUri(databaseLocation(namespace.level(0)));

    metadata.forEach(
            (key, value) -> {
              if (key.equals("comment")) {
                database.setDescription(value);
              } else if (key.equals("location")) {
                database.setLocationUri(value);
              } else if (key.equals(HMS_DB_OWNER)) {
                database.setOwnerName(value);
              } else if (key.equals(HMS_DB_OWNER_TYPE) && value != null) {
                database.setOwnerType(PrincipalType.valueOf(value));
              } else {
                if (value != null) {
                  parameter.put(key, value);
                }
              }
            });

    if (database.getOwnerName() == null) {
      database.setOwnerName(currentUser());
      database.setOwnerType(PrincipalType.USER);
    }

    database.setParameters(parameter);

    return database;
  }

  public Map<String, String> convertToMetadata() {
    Map<String, String> meta = Maps.newHashMap();

    meta.putAll(innerDatabase.getParameters());
    meta.put("location", innerDatabase.getLocationUri());
    if (innerDatabase.getDescription() != null) {
      meta.put("comment", innerDatabase.getDescription());
    }
    if (innerDatabase.getOwnerName() != null) {
      meta.put(HMS_DB_OWNER, innerDatabase.getOwnerName());
      if (innerDatabase.getOwnerType() != null) {
        meta.put(HMS_DB_OWNER_TYPE, innerDatabase.getOwnerType().name());
      }
    }

    return meta;
  }

  private String databaseLocation(String databaseName) {
    String warehouseLocation = conf.get(HiveConf.ConfVars.METASTOREWAREHOUSE.varname);
    Preconditions.checkNotNull(
            warehouseLocation, "Warehouse location is not set: hive.metastore.warehouse.dir=null");
    warehouseLocation = stripTrailingSlash(warehouseLocation);
    return String.format("%s/%s.db", warehouseLocation, databaseName);
  }

  public static String stripTrailingSlash(String path) {
    Preconditions.checkArgument(path != null && path.length() > 0,
            "path must not be null or empty");

    String result = path;
    while (result.endsWith("/")) {
      result = result.substring(0, result.length() - 1);
    }
    return result;
  }

  public static String currentUser() {
    String username = null;
    try {
      username = UserGroupInformation.getCurrentUser().getShortUserName();
    } catch (IOException e) {
      LOG.warn("Failed to get Hadoop user", e);
    }

    if (username != null) {
      return username;
    } else {
      LOG.warn("Hadoop user is null, defaulting to user.name");
      return System.getProperty("user.name");
    }
  }
}
