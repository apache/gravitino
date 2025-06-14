package org.apache.gravitino.catalog;

import static org.junit.jupiter.api.Assertions.*;

import org.apache.gravitino.CatalogChange;
import org.junit.jupiter.api.Test;

public class TestCatalogPropertyAnalyzer {

    @Test
    void testCriticalPropertyChangesNeedRecreation() {
        // Given
        CatalogChange[] changes = {
                CatalogChange.setProperty("metastore.uris", "thrift://new-host:9083"),
                CatalogChange.setProperty("jdbc.url", "jdbc:mysql://new-host:3306/db")
        };

        // When
        CatalogPropertyAnalyzer.ChangeAnalysisResult result =
                CatalogPropertyAnalyzer.analyzeCatalogChanges(changes);

        // Then
        assertTrue(result.needsRecreation());
        assertFalse(result.hasHotUpdatableChanges());
    }

    @Test
    void testHotUpdatablePropertyChanges() {
        // Given
        CatalogChange[] changes = {
                CatalogChange.setProperty("description", "new description"),
                CatalogChange.setProperty("comment", "new comment")
        };

        // When
        CatalogPropertyAnalyzer.ChangeAnalysisResult result =
                CatalogPropertyAnalyzer.analyzeCatalogChanges(changes);

        // Then
        assertFalse(result.needsRecreation());
        assertTrue(result.hasHotUpdatableChanges());
    }

    @Test
    void testMixedPropertyChanges() {
        // Given
        CatalogChange[] changes = {
                CatalogChange.setProperty("metastore.uris", "thrift://new-host:9083"), // 关键属性
                CatalogChange.setProperty("description", "new description") // 热更新属性
        };

        // When
        CatalogPropertyAnalyzer.ChangeAnalysisResult result =
                CatalogPropertyAnalyzer.analyzeCatalogChanges(changes);

        // Then
        assertTrue(result.needsRecreation()); // 有关键属性变更
        assertTrue(result.hasHotUpdatableChanges()); // 也有热更新属性
    }

    @Test
    void testRemovePropertyChanges() {
        // Given
        CatalogChange[] changes = {
                CatalogChange.removeProperty("jdbc.driver") // 移除关键属性
        };

        // When
        CatalogPropertyAnalyzer.ChangeAnalysisResult result =
                CatalogPropertyAnalyzer.analyzeCatalogChanges(changes);

        // Then
        assertTrue(result.needsRecreation());
        assertFalse(result.hasHotUpdatableChanges());
    }

    @Test
    void testNonPropertyChanges() {
        // Given
        CatalogChange[] changes = {
                CatalogChange.renameCatalog("new-name"),
                CatalogChange.updateCatalogComment("new comment")
        };

        // When
        CatalogPropertyAnalyzer.ChangeAnalysisResult result =
                CatalogPropertyAnalyzer.analyzeCatalogChanges(changes);

        // Then
        assertFalse(result.needsRecreation());
        assertTrue(result.hasHotUpdatableChanges());
    }

    @Test
    void testEmptyChanges() {
        // Given
        CatalogChange[] changes = {};

        // When
        CatalogPropertyAnalyzer.ChangeAnalysisResult result =
                CatalogPropertyAnalyzer.analyzeCatalogChanges(changes);

        // Then
        assertFalse(result.needsRecreation());
        assertFalse(result.hasHotUpdatableChanges());
    }

    @Test
    void testAuthorizationPropertyChanges() {
        // Given
        CatalogChange[] changes = {
                CatalogChange.setProperty("authorization.enable", "true"),
                CatalogChange.setProperty("authentication.type", "kerberos")
        };

        // When
        CatalogPropertyAnalyzer.ChangeAnalysisResult result =
                CatalogPropertyAnalyzer.analyzeCatalogChanges(changes);

        // Then
        assertTrue(result.needsRecreation());
        assertFalse(result.hasHotUpdatableChanges());
    }
}