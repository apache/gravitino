package org.apache.gravitino.catalog;

import com.google.common.collect.Sets;
import java.util.Map;
import java.util.Set;
import org.apache.gravitino.CatalogChange;

/**
 * Catalog属性分析器，用于判断属性变更是否需要重新创建catalog实例
 */
public class CatalogPropertyAnalyzer {

    // 需要重新创建catalog的关键属性前缀
    private static final Set<String> CRITICAL_PROPERTY_PREFIXES = Sets.newHashSet(
            "gravitino.bypass.",
            "authentication.",
            "authorization.",
            "package-path",
            "conf-path",
            "jdbc.url",
            "jdbc.driver",
            "metastore.uris",
            "warehouse"
    );

    // 只需要热更新的属性前缀
    private static final Set<String> HOT_UPDATABLE_PREFIXES = Sets.newHashSet(
            "gravitino.catalog.cache.",
            "description",
            "comment"
    );

    /**
     * 分析catalog变更，判断是否需要重新创建实例
     */
    public static ChangeAnalysisResult analyzeCatalogChanges(CatalogChange... changes) {
        boolean needsRecreation = false;
        boolean hasHotUpdatableChanges = false;

        for (CatalogChange change : changes) {
            if (change instanceof CatalogChange.SetProperty) {
                CatalogChange.SetProperty setProperty = (CatalogChange.SetProperty) change;
                String property = setProperty.getProperty();

                if (isCriticalProperty(property)) {
                    needsRecreation = true;
                } else if (isHotUpdatableProperty(property)) {
                    hasHotUpdatableChanges = true;
                }
            } else if (change instanceof CatalogChange.RemoveProperty) {
                CatalogChange.RemoveProperty removeProperty = (CatalogChange.RemoveProperty) change;
                String property = removeProperty.getProperty();

                if (isCriticalProperty(property)) {
                    needsRecreation = true;
                }
            } else {
                // 其他类型的变更（如重命名）通常不需要重新创建实例
                hasHotUpdatableChanges = true;
            }
        }

        return new ChangeAnalysisResult(needsRecreation, hasHotUpdatableChanges);
    }

    private static boolean isCriticalProperty(String property) {
        return CRITICAL_PROPERTY_PREFIXES.stream()
                .anyMatch(property::startsWith);
    }

    private static boolean isHotUpdatableProperty(String property) {
        return HOT_UPDATABLE_PREFIXES.stream()
                .anyMatch(property::startsWith);
    }

    /**
     * 变更分析结果
     */
    public static class ChangeAnalysisResult {
        private final boolean needsRecreation;
        private final boolean hasHotUpdatableChanges;

        public ChangeAnalysisResult(boolean needsRecreation, boolean hasHotUpdatableChanges) {
            this.needsRecreation = needsRecreation;
            this.hasHotUpdatableChanges = hasHotUpdatableChanges;
        }

        public boolean needsRecreation() {
            return needsRecreation;
        }

        public boolean hasHotUpdatableChanges() {
            return hasHotUpdatableChanges;
        }
    }
}