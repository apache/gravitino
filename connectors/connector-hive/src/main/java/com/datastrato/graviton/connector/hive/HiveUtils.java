package com.datastrato.graviton.connector.hive;

import com.google.common.base.Joiner;
import com.google.common.net.HostAndPort;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.api.*;
import org.apache.hadoop.hive.ql.io.HiveInputFormat;
import org.apache.hadoop.hive.ql.io.HiveOutputFormat;
import org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.*;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.hadoop.hive.metastore.TableType.MANAGED_TABLE;

/**
 * Helper utilities. The Util class is just a placeholder for static methods,
 * it should be never instantiated.
 */
public final class HiveUtils {
  private static final String DEFAULT_TYPE = "string";
  private static final String TYPE_SEPARATOR = ":";
  private static final String THRIFT_SCHEMA = "thrift";
  private static final String ENV_SERVER = "HMS_HOST";
  private static final String ENV_PORT = "HMS_PORT";
  private static final String PROP_HOST = "hms.host";
  private static final String PROP_PORT = "hms.port";

  private static final Pattern[] EMPTY_PATTERN = new Pattern[]{};
  private static final Pattern[] MATCH_ALL_PATTERN = new Pattern[]{Pattern.compile(".*")};

  private static final Logger LOG = LoggerFactory.getLogger(HiveUtils.class);

  // Disable public constructor
  private HiveUtils() {
  }

  /**
   * A builder for Database.  The name of the new database is required.  Everything else
   * selects reasonable defaults.
   * This is a modified version of Hive 3.0 DatabaseBuilder.
   */
  public static class DatabaseBuilder {
    private String name;
    private String description;
    private String location;
    private String ownerName;
    private PrincipalType ownerType;
    private Map<String, String> params = null;

    // Disable default constructor
    private DatabaseBuilder() {
    }

    public DatabaseBuilder(String name) {
      this.name = name;
      ownerType = PrincipalType.USER;
    }

    public DatabaseBuilder withDescription(String description) {
      this.description = description;
      return this;
    }

    public DatabaseBuilder withLocation(String location) {
      this.location = location;
      return this;
    }

    public DatabaseBuilder withParams(Map<String, String> params) {
      this.params = params;
      return this;
    }

    public DatabaseBuilder withParam(String key, String val) {
      if (this.params == null) {
        this.params = new HashMap<>();
      }
      this.params.put(key, val);
      return this;
    }

    public DatabaseBuilder withOwnerName(String ownerName) {
      this.ownerName = ownerName;
      return this;
    }

    public DatabaseBuilder withOwnerType(PrincipalType ownerType) {
      this.ownerType = ownerType;
      return this;
    }

    public Database build() {
      Database db = new Database(name, description, location, params);
      if (ownerName != null) {
        db.setOwnerName(ownerName);
      }
      if (ownerType != null) {
        db.setOwnerType(ownerType);
      }
      return db;
    }
  }

  public static class TableBuilder {
    private final String dbName;
    private final String tableName;
    private TableType tableType = MANAGED_TABLE;
    private String location;
    private String serde = LazySimpleSerDe.class.getName();
    private String owner;
    private List<FieldSchema> columns;
    private List<FieldSchema> partitionKeys;
    private String inputFormat = HiveInputFormat.class.getName();
    private String outputFormat = HiveOutputFormat.class.getName();
    private final Map<String, String> parameters = new HashMap<>();

    private TableBuilder() {
      dbName = null;
      tableName = null;
    }

    public TableBuilder(String dbName, String tableName) {
      this.dbName = dbName;
      this.tableName = tableName;
    }

    public static Table buildDefaultTable(String dbName, String tableName) {
      return new TableBuilder(dbName, tableName).build();
    }

    public TableBuilder withType(TableType tabeType) {
      this.tableType = tabeType;
      return this;
    }

    public TableBuilder withOwner(String owner) {
      this.owner = owner;
      return this;
    }

    public TableBuilder withColumns(List<FieldSchema> columns) {
      this.columns = columns;
      return this;
    }

    public TableBuilder withPartitionKeys(List<FieldSchema> partitionKeys) {
      this.partitionKeys = partitionKeys;
      return this;
    }

    public TableBuilder withSerde(String serde) {
      this.serde = serde;
      return this;
    }

    public TableBuilder withInputFormat(String inputFormat) {
      this.inputFormat = inputFormat;
      return this;
    }

    public TableBuilder withOutputFormat(String outputFormat) {
      this.outputFormat = outputFormat;
      return this;
    }

    public TableBuilder withParameter(String name, String value) {
      parameters.put(name, value);
      return this;
    }

    public TableBuilder withLocation(String location) {
      this.location = location;
      return this;
    }

    public Table build() {
      StorageDescriptor sd = new StorageDescriptor();
      if (columns == null) {
        sd.setCols(Collections.emptyList());
      } else {
        sd.setCols(columns);
      }
      SerDeInfo serdeInfo = new SerDeInfo();
      serdeInfo.setSerializationLib(serde);
      serdeInfo.setName(tableName);
      sd.setSerdeInfo(serdeInfo);
      sd.setInputFormat(inputFormat);
      sd.setOutputFormat(outputFormat);
      if (location != null) {
        sd.setLocation(location);
      }

      Table table = new Table();
      table.setDbName(dbName);
      table.setTableName(tableName);
      table.setSd(sd);
      table.setParameters(parameters);
      table.setOwner(owner);
      if (partitionKeys != null) {
        table.setPartitionKeys(partitionKeys);
      }
      table.setTableType(tableType.toString());
      return table;
    }
  }

//  public static class PartitionBuilder {
//    private final Table table;
//    private List<String> values;
//    private String location;
//    private Map<String, String> parameters = new HashMap<>();
//
//    private PartitionBuilder() {
//      table = null;
//    }
//
//    PartitionBuilder(Table table) {
//      this.table = table;
//    }
//
//    PartitionBuilder withValues(List<String> values) {
//      this.values = new ArrayList<>(values);
//      return this;
//    }
//
//    PartitionBuilder withLocation(String location) {
//      this.location = location;
//      return this;
//    }
//
//    PartitionBuilder withParameter(String name, String value) {
//      parameters.put(name, value);
//      return this;
//    }
//
//    PartitionBuilder withParameters(Map<String, String> params) {
//      parameters = params;
//      return this;
//    }
//
//    Partition build() {
//      Partition partition = new Partition();
//      List<String> partitionNames = table.getPartitionKeys()
//              .stream()
//              .map(FieldSchema::getName)
//              .collect(Collectors.toList());
//      if (partitionNames.size() != values.size()) {
//        throw new RuntimeException("Partition values do not match table schema");
//      }
//      List<String> spec = IntStream.range(0, values.size())
//              .mapToObj(i -> partitionNames.get(i) + "=" + values.get(i))
//              .collect(Collectors.toList());
//
//      partition.setDbName(table.getDbName());
//      partition.setTableName(table.getTableName());
//      partition.setParameters(parameters);
//      partition.setValues(values);
//      partition.setSd(table.getSd().deepCopy());
//      if (this.location == null) {
//        partition.getSd().setLocation(table.getSd().getLocation() + "/" + Joiner.on("/").join(spec));
//      } else {
//        partition.getSd().setLocation(location);
//      }
//      return partition;
//    }
//  }
//
//  public static List<FieldSchema> createSchema(List<String> params) {
//    if (params == null || params.isEmpty()) {
//      return Collections.emptyList();
//    }
//
//    return params.stream()
//            .map(Util::param2Schema)
//            .collect(Collectors.toList());
//  }
//
//  public static URI getServerUri(String host, String portString) throws
//          URISyntaxException {
//    if (host == null) {
//      host = System.getenv(ENV_SERVER);
//    }
//    if (host == null) {
//      host = System.getProperty(PROP_HOST);
//    }
//    if (host == null) {
//      host = Constants.DEFAULT_HOST;
//    }
//    host = host.trim();
//
//    if ((portString == null || portString.isEmpty() || portString.equals("0")) &&
//            !host.contains(":")) {
//      portString = System.getenv(ENV_PORT);
//      if (portString == null) {
//        portString = System.getProperty(PROP_PORT);
//      }
//    }
//    Integer port = Constants.HMS_DEFAULT_PORT;
//    if (portString != null) {
//      port = Integer.parseInt(portString);
//    }
//
//    HostAndPort hp = HostAndPort.fromString(host)
//            .withDefaultPort(port);
//
//    LOG.info("Connecting to {}:{}", hp.getHostText(), hp.getPort());
//
//    return new URI(THRIFT_SCHEMA, null, hp.getHostText(), hp.getPort(),
//            null, null, null);
//  }
//
//  private static FieldSchema param2Schema(String param) {
//    String colType = DEFAULT_TYPE;
//    String name = param;
//    if (param.contains(TYPE_SEPARATOR)) {
//      String[] parts = param.split(TYPE_SEPARATOR);
//      name = parts[0];
//      colType = parts[1].toLowerCase();
//    }
//    return new FieldSchema(name, colType, "");
//  }

//  static List<Partition> createManyPartitions(Table table,
//                                              Map<String, String> parameters,
//                                              List<String> arguments,
//                                              int nPartitions) {
//    return IntStream.range(0, nPartitions)
//            .mapToObj(i ->
//                    new PartitionBuilder(table)
//                            .withParameters(parameters)
//                            .withValues(
//                                    arguments.stream()
//                                            .map(a -> a + i)
//                                            .collect(Collectors.toList())).build())
//            .collect(Collectors.toList());
//  }

//  static Object addManyPartitions(HMSClient client,
//                                  String dbName,
//                                  String tableName,
//                                  Map<String, String> parameters,
//                                  List<String> arguments,
//                                  int nPartitions) throws TException {
//    Table table = client.getTable(dbName, tableName);
//    client.addPartitions(createManyPartitions(table, parameters, arguments, nPartitions));
//    return null;
//  }
//
//  static List<String> generatePartitionNames(String prefix, int npartitions) {
//    return IntStream.range(0, npartitions).mapToObj(i -> prefix + i).collect(Collectors.toList());
//  }
//
//  static void addManyPartitionsNoException(HMSClient client,
//                                           String dbName,
//                                           String tableName,
//                                           Map<String, String> parameters,
//                                           List<String> arguments,
//                                           int npartitions) {
//    throwingSupplierWrapper(() ->
//            addManyPartitions(client, dbName, tableName, parameters, arguments, npartitions));
//  }
//
//  public static List<String> filterMatches(List<String> candidates,
//                                           Pattern[] positivePatterns,
//                                           Pattern[] negativePatterns) {
//    if (candidates == null || candidates.isEmpty()) {
//      return Collections.emptyList();
//    }
//    final Pattern[] positive = (positivePatterns == null || positivePatterns.length == 0) ?
//            MATCH_ALL_PATTERN : positivePatterns;
//    final Pattern[] negative = negativePatterns == null ? EMPTY_PATTERN : negativePatterns;
//
//    return candidates.stream()
//            .filter(c -> Arrays.stream(positive).anyMatch(p -> p.matcher(c).matches()))
//            .filter(c -> Arrays.stream(negative).noneMatch(p -> p.matcher(c).matches()))
//            .collect(Collectors.toList());
//  }
}
