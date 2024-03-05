package com.datastrato.gravitino.trino.connector.util;

import com.datastrato.gravitino.Catalog;
import com.datastrato.gravitino.dto.rel.ColumnDTO;
import com.datastrato.gravitino.dto.rel.TableDTO;
import com.datastrato.gravitino.dto.util.DTOConverters;
import com.datastrato.gravitino.rel.Column;
import com.datastrato.gravitino.rel.Table;
import com.datastrato.gravitino.rel.expressions.Expression;
import com.datastrato.gravitino.rel.types.Types;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

public interface GravitinoSystemTable<T> {
    //scheamname
    String schemaName();

    // define a system table
    GravitinoSystemTable createTable();

    //show create table
    Table getTable();

    //access control
    boolean checkAccess();

    // query
    List<T> query(int pageStart, int pageSize);
    List<T> query(int pageStart, int pageSize, Expression[] filter);
    List<T> query(int pageStart, int pageSize, Expression[] filter, Map<String, String> options);


    // insert
    void insert(T entry);
    void insert(List<T> entries);

    // define the ttl of table
    int ttlDays();

    // the version of table
    int version();

    class SystemTableCatalog implements GravitinoSystemTable<Catalog> {

        String schemaName;
        String tableDTO;

        @Override
        public GravitinoSystemTable createTable() {
            Column[] columns = {
             Column.of("catalog", Types.StringType.get(), "catalog name"),
             Column.of("provider", Types.StringType.get()),
             Column.of("comment", Types.MapType.get()),
             Column.of("properties", Types.MapType.get()),
             Column.of("create_time", Types.DataTimeType.get()),
             Column.of("modification_time", Types.DataTimeType.get()),
            };
            TableDTO tableDTO = new TableDTO.Builder()
                .withName("catalog")
                .withColumns(DTOConverters.toDTOs(columns))
                .withProperties(Map.of())
                .withAudit(System.Audit)
                .build();

            return new SystemTableCatalog("metadata", tableDTO));
        }

        @Override
        public Table getTable() {
            return tableDTO;
        }

        @Override
        public boolean checkAccess(Session session) {
            return true;
        }

        @Override
        public List<Catalog> query(int pageStart, int pageSize) {
            return catalogManger.listCatalog().map(loadCatalog()).toList();
        }

        @Override
        public int ttlDays() {
            return 0;
        }

        @Override
        public int version() {
            return 0;
        }
    }
}


