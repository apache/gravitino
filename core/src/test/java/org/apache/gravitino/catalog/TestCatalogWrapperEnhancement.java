package org.apache.gravitino.catalog;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import com.google.common.collect.Maps;
import java.util.Map;
import org.apache.gravitino.CatalogChange;
import org.apache.gravitino.catalog.BaseCatalog;
import org.apache.gravitino.utils.IsolatedClassLoader;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

public class TestCatalogWrapperEnhancement {

    @Mock
    private BaseCatalog mockCatalog;

    @Mock
    private IsolatedClassLoader mockClassLoader;

    private CatalogManager.CatalogWrapper catalogWrapper;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
        when(mockCatalog.provider()).thenReturn("test-provider");
        when(mockCatalog.properties()).thenReturn(Maps.newHashMap());
        catalogWrapper = new CatalogManager.CatalogWrapper(mockCatalog, mockClassLoader);
    }

    @Test
    void testHotUpdateProperties() {
        // Given
        Map<String, String> newProperties = Maps.newHashMap();
        newProperties.put("description", "new description");
        newProperties.put("comment", "new comment");

        // When
        assertDoesNotThrow(() -> catalogWrapper.hotUpdateProperties(newProperties));

        // Then
        verify(mockClassLoader, times(1)).withClassLoader(any());
    }

    @Test
    void testCanHotUpdateWithNonCriticalChanges() {
        // Given
        CatalogChange[] changes = {
                CatalogChange.setProperty("description", "new description"),
                CatalogChange.setProperty("comment", "new comment")
        };

        // When
        boolean canHotUpdate = catalogWrapper.canHotUpdate(changes);

        // Then
        assertTrue(canHotUpdate);
    }

    @Test
    void testCannotHotUpdateWithCriticalChanges() {
        // Given
        CatalogChange[] changes = {
                CatalogChange.setProperty("metastore.uris", "thrift://new-host:9083"),
                CatalogChange.setProperty("jdbc.url", "jdbc:mysql://new-host:3306/db")
        };

        // When
        boolean canHotUpdate = catalogWrapper.canHotUpdate(changes);

        // Then
        assertFalse(canHotUpdate);
    }

    @Test
    void testGetProviderAndOriginalConf() {
        // When
        String provider = catalogWrapper.getProvider();
        Map<String, String> originalConf = catalogWrapper.getOriginalConf();

        // Then
        assertEquals("test-provider", provider);
        assertNotNull(originalConf);
    }
}