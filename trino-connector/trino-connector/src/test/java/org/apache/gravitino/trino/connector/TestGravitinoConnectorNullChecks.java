package org.apache.gravitino.trino.connector;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.*;

import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.transaction.IsolationLevel;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.trino.connector.catalog.CatalogConnectorContext;
import org.junit.jupiter.api.Test;

class TestGravitinoConnectorNullChecks {
    @Test
    void testBeginTransactionThrowsIfInternalConnectorIsNull() {
        CatalogConnectorContext mockContext = mock(CatalogConnectorContext.class);
        when(mockContext.getInternalConnector()).thenReturn(null);
        GravitinoConnector connector = new GravitinoConnector(mock(NameIdentifier.class), mockContext);
        assertThrows(NullPointerException.class,
                () -> connector.beginTransaction(mock(IsolationLevel.class), true, true));
    }

    @Test
    void testBeginTransactionThrowsIfInternalTransactionHandleIsNull() {
        CatalogConnectorContext mockContext = mock(CatalogConnectorContext.class);
        Connector mockInternalConnector = mock(Connector.class);
        when(mockContext.getInternalConnector()).thenReturn(mockInternalConnector);
        when(mockInternalConnector.beginTransaction(any(), anyBoolean(), anyBoolean())).thenReturn(null);
        GravitinoConnector connector = new GravitinoConnector(mock(NameIdentifier.class), mockContext);
        assertThrows(NullPointerException.class,
                () -> connector.beginTransaction(mock(IsolationLevel.class), true, true));
    }
}
