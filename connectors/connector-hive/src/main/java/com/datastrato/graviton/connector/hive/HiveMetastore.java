package com.datastrato.graviton.connector.hive;

import com.google.common.collect.ImmutableMap;
import io.trino.hive.thrift.metastore.DataOperationType;
import io.trino.plugin.hive.HiveColumnStatisticType;
import io.trino.plugin.hive.HivePartition;
import io.trino.plugin.hive.HiveType;
import io.trino.plugin.hive.PartitionStatistics;
import io.trino.plugin.hive.acid.AcidOperation;
import io.trino.plugin.hive.acid.AcidTransaction;
import io.trino.plugin.hive.metastore.HivePrivilegeInfo.HivePrivilege;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.security.RoleGrant;
import io.trino.spi.type.Type;

import java.util.*;
import java.util.function.Function;


public interface HiveMetastore
{
    Optional<Database> getDatabase(String databaseName);

    List<String> getAllDatabases();

    Optional<Table> getTable(String databaseName, String tableName);

    Set<HiveColumnStatisticType> getSupportedColumnStatistics(Type type);

    PartitionStatistics getTableStatistics(Table table);

    Map<String, PartitionStatistics> getPartitionStatistics(Table table, List<Partition> partitions);

    void updateTableStatistics(String databaseName, String tableName, AcidTransaction transaction, Function<PartitionStatistics, PartitionStatistics> update);

    default void updatePartitionStatistics(Table table, String partitionName, Function<PartitionStatistics, PartitionStatistics> update)
    {
        updatePartitionStatistics(table, ImmutableMap.of(partitionName, update));
    }

    void updatePartitionStatistics(Table table, Map<String, Function<PartitionStatistics, PartitionStatistics>> updates);

    List<String> getAllTables(String databaseName);

    /**
     * @return List of tables, views and materialized views names from all schemas or Optional.empty if operation is not supported
     */
    Optional<List<SchemaTableName>> getAllTables();

    List<String> getTablesWithParameter(String databaseName, String parameterKey, String parameterValue);

    List<String> getAllViews(String databaseName);

    /**
     * @return List of views including materialized views names from all schemas or Optional.empty if operation is not supported
     */
    Optional<List<SchemaTableName>> getAllViews();

    void createDatabase(Database database);

    void dropDatabase(String databaseName, boolean deleteData);

    void renameDatabase(String databaseName, String newDatabaseName);

    void setDatabaseOwner(String databaseName, HivePrincipal principal);

    void createTable(Table table, PrincipalPrivileges principalPrivileges);

    void dropTable(String databaseName, String tableName, boolean deleteData);

    /**
     * This should only be used if the semantic here is drop and add. Trying to
     * alter one field of a table object previously acquired from getTable is
     * probably not what you want.
     */
    void replaceTable(String databaseName, String tableName, Table newTable, PrincipalPrivileges principalPrivileges);

    void renameTable(String databaseName, String tableName, String newDatabaseName, String newTableName);

    void commentTable(String databaseName, String tableName, Optional<String> comment);

    void setTableOwner(String databaseName, String tableName, HivePrincipal principal);

    void commentColumn(String databaseName, String tableName, String columnName, Optional<String> comment);

    void addColumn(String databaseName, String tableName, String columnName, HiveType columnType, String columnComment);

    void renameColumn(String databaseName, String tableName, String oldColumnName, String newColumnName);

    void dropColumn(String databaseName, String tableName, String columnName);

    Optional<Partition> getPartition(Table table, List<String> partitionValues);

    /**
     * Return a list of partition names, with optional filtering (hint to improve performance if possible).
     *
     * @param databaseName the name of the database
     * @param tableName the name of the table
     * @param columnNames the list of partition column names
     * @param partitionKeysFilter optional filter for the partition column values
     * @return a list of partition names as created by {@link MetastoreUtil#toPartitionName}
     * @see TupleDomain
     */
    Optional<List<String>> getPartitionNamesByFilter(String databaseName, String tableName, List<String> columnNames, TupleDomain<String> partitionKeysFilter);

    Map<String, Optional<Partition>> getPartitionsByNames(Table table, List<String> partitionNames);

    void addPartitions(String databaseName, String tableName, List<PartitionWithStatistics> partitions);

    void dropPartition(String databaseName, String tableName, List<String> parts, boolean deleteData);

    void alterPartition(String databaseName, String tableName, PartitionWithStatistics partition);

    void createRole(String role, String grantor);

    void dropRole(String role);

    Set<String> listRoles();

    void grantRoles(Set<String> roles, Set<HivePrincipal> grantees, boolean adminOption, HivePrincipal grantor);

    void revokeRoles(Set<String> roles, Set<HivePrincipal> grantees, boolean adminOption, HivePrincipal grantor);

    Set<RoleGrant> listGrantedPrincipals(String role);

    Set<RoleGrant> listRoleGrants(HivePrincipal principal);

    void grantTablePrivileges(String databaseName, String tableName, String tableOwner, HivePrincipal grantee, HivePrincipal grantor, Set<HivePrivilege> privileges, boolean grantOption);

    void revokeTablePrivileges(String databaseName, String tableName, String tableOwner, HivePrincipal grantee, HivePrincipal grantor, Set<HivePrivilege> privileges, boolean grantOption);

    /**
     * @param principal when empty, all table privileges are returned
     */
    Set<HivePrivilegeInfo> listTablePrivileges(String databaseName, String tableName, Optional<String> tableOwner, Optional<HivePrincipal> principal);

    default void checkSupportsTransactions()
    {
        throw new TrinoException(NOT_SUPPORTED, getClass().getSimpleName() + " does not support ACID tables");
    }

    default long openTransaction(AcidTransactionOwner transactionOwner)
    {
        throw new UnsupportedOperationException();
    }

    default void commitTransaction(long transactionId)
    {
        throw new UnsupportedOperationException();
    }

    default void abortTransaction(long transactionId)
    {
        throw new UnsupportedOperationException();
    }

    default void sendTransactionHeartbeat(long transactionId)
    {
        throw new UnsupportedOperationException();
    }

    default void acquireSharedReadLock(
            AcidTransactionOwner transactionOwner,
            String queryId,
            long transactionId,
            List<SchemaTableName> fullTables,
            List<HivePartition> partitions)
    {
        throw new UnsupportedOperationException();
    }

    default String getValidWriteIds(List<SchemaTableName> tables, long currentTransactionId)
    {
        throw new UnsupportedOperationException();
    }

    default Optional<String> getConfigValue(String name)
    {
        return Optional.empty();
    }

    default long allocateWriteId(String dbName, String tableName, long transactionId)
    {
        throw new UnsupportedOperationException();
    }

    default void acquireTableWriteLock(
            AcidTransactionOwner transactionOwner,
            String queryId,
            long transactionId,
            String dbName,
            String tableName,
            DataOperationType operation,
            boolean isDynamicPartitionWrite)
    {
        throw new UnsupportedOperationException();
    }

    default void updateTableWriteId(String dbName, String tableName, long transactionId, long writeId, OptionalLong rowCountChange)
    {
        throw new UnsupportedOperationException();
    }

    default void alterPartitions(String dbName, String tableName, List<Partition> partitions, long writeId)
    {
        throw new UnsupportedOperationException();
    }

    default void addDynamicPartitions(String dbName, String tableName, List<String> partitionNames, long transactionId, long writeId, AcidOperation operation)
    {
        throw new UnsupportedOperationException();
    }

    default void alterTransactionalTable(Table table, long transactionId, long writeId, PrincipalPrivileges principalPrivileges)
    {
        throw new UnsupportedOperationException();
    }
}
