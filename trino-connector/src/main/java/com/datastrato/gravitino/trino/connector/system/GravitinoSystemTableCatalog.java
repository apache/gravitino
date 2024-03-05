package com.datastrato.gravitino.trino.connector.system;

import com.datastrato.gravitino.trino.connector.catalog.CatalogConnectorManager;
import io.trino.spi.Page;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.BlockBuilderStatus;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.type.MapType;
import io.trino.spi.type.TypeOperators;

import java.util.List;

import static io.trino.spi.type.VarcharType.VARCHAR;

public class GravitinoSystemTableCatalog extends GravitinoSystemTable{

    public final static SchemaTableName TABLE_NAME= new SchemaTableName("system", "catalog");

    private static final List<ColumnMetadata> columnMetadata = List.of(
            ColumnMetadata.builder().setName("name").setType(VARCHAR).build(),
            ColumnMetadata.builder().setName("provider").setType(VARCHAR).build(),
            ColumnMetadata.builder()
                    .setName("properties")
                    .setType(new MapType(VARCHAR, VARCHAR, new TypeOperators()))
                    .build());

    private static final ConnectorTableMetadata metadata = new ConnectorTableMetadata(TABLE_NAME, columnMetadata);


    static
    {
        GravitinoSystemTable.registSystemTable(TABLE_NAME, new GravitinoSystemTableCatalog());
    }

    public Page loadPageData() {
        int size = catalogConnectorManager.getCatalogs().size();
        BlockBuilder nameColumnBuilder = VARCHAR.createBlockBuilder(null, size);
        BlockBuilder providerColumnBuilder = VARCHAR.createBlockBuilder(null, size);
        BlockBuilder propertyColumnBuilder = VARCHAR.createBlockBuilder(null, size);
        Page page = new Page(size, nameColumnBuilder, propertyColumnBuilder, propertyColumnBuilder);

        for (String catalogName : catalogConnectorManager.getCatalogs()) {
            //GravitionCatalog catalog = catalogConnectorManager.getCatalogConnector(catalogName).getCatalog();
        }
    }

    @Override
    public ConnectorTableMetadata getTableMetaData() {
        return metadata;
    }
}
