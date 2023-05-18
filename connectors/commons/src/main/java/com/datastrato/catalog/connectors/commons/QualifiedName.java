package com.datastrato.catalog.connectors.commons;

import lombok.Getter;
import lombok.NonNull;

import javax.annotation.Nullable;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Objects;

/**
 * A fully qualified name that references a source of data.
 */
@Getter
public final class QualifiedName implements Serializable {
    private static final long serialVersionUID = -7916364073519921672L;
    private static final String CATALOG_CDE_NAME_PREFIX = "cde_";
    private final String catalogName;
    private final String databaseName;
    private final String partitionName;
    private final String tableName;
    private final String viewName;
    private final Type type;

    private QualifiedName(
        @NonNull final String catalogName,
        @Nullable final String databaseName,
        @Nullable final String tableName,
        @Nullable final String partitionName,
        @Nullable final String viewName
    ) {
        this.catalogName = standardizeRequired("catalogName", catalogName);
        // TODO: Temporary hack to support a certain catalog that has mixed case naming.
        final boolean forceLowerCase = !catalogName.startsWith(CATALOG_CDE_NAME_PREFIX);
        this.databaseName = standardizeOptional(databaseName, forceLowerCase);
        this.tableName = standardizeOptional(tableName, forceLowerCase);
        this.partitionName = standardizeOptional(partitionName, false);
        this.viewName = standardizeOptional(viewName, forceLowerCase);

        if (this.databaseName.isEmpty() && (!this.tableName.isEmpty() || !this.partitionName.isEmpty())) {
            throw new IllegalStateException("databaseName is not present but tableName or partitionName are present");
        } else if (this.tableName.isEmpty() && !this.partitionName.isEmpty()) {
            throw new IllegalStateException("tableName is not present but partitionName is present");
        }

        if (!this.viewName.isEmpty()) {
            type = Type.MVIEW;
        } else if (!this.partitionName.isEmpty()) {
            type = Type.PARTITION;
        } else if (!this.tableName.isEmpty()) {
            type = Type.TABLE;
        } else if (!this.databaseName.isEmpty()) {
            type = Type.DATABASE;
        } else {
            type = Type.CATALOG;
        }
    }

    private static String standardizeOptional(@Nullable final String value, final boolean forceLowerCase) {
        if (value == null) {
            return "";
        } else {
            String returnValue = value.trim();
            if (forceLowerCase) {
                returnValue = returnValue.toLowerCase();
            }
            return returnValue;
        }
    }

    private static String standardizeRequired(final String name, @Nullable final String value) {
        if (value == null) {
            throw new IllegalStateException(name + " cannot be null");
        }

        final String returnValue = value.trim();
        if (returnValue.isEmpty()) {
            throw new IllegalStateException(name + " cannot be an empty string");
        }

        return returnValue.toLowerCase();
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof QualifiedName)) {
            return false;
        }
        final QualifiedName that = (QualifiedName) o;
        return Objects.equals(catalogName, that.catalogName)
            && Objects.equals(databaseName, that.databaseName)
            && Objects.equals(partitionName, that.partitionName)
            && Objects.equals(tableName, that.tableName)
            && Objects.equals(viewName, that.viewName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(catalogName, databaseName, partitionName, tableName, viewName);
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder(catalogName);

        if (!databaseName.isEmpty()) {
            sb.append('/');
            sb.append(databaseName);
        }

        if (!tableName.isEmpty()) {
            sb.append('/');
            sb.append(tableName);
        }

        if (!partitionName.isEmpty()) {
            sb.append('/');
            sb.append(partitionName);
        }

        if (!viewName.isEmpty()) {
            sb.append('/');
            sb.append(viewName);
        }

        return sb.toString();
    }

    /**
     * Type of the connector resource.
     */
    public enum Type {
        /**
         * Catalog type.
         */
        CATALOG("^([^\\/]+)$"),

        /**
         * Database type.
         */
        DATABASE("^([^\\/]+)\\/([^\\/]+)$"),

        /**
         * Table type.
         */
        TABLE("^([^\\/]+)\\/([^\\/]+)\\/([^\\/]+)$"),

        /**
         * Partition type.
         */
        PARTITION("^(.*)$"),

        /**
         * MView type.
         */
        MVIEW("^([^\\/]+)\\/([^\\/]+)\\/([^\\/]+)\\/([^\\/]+)$");

        private final String regexValue;

        /**
         * Constructor.
         *
         * @param value category value.
         */
        Type(final String value) {
            this.regexValue = value;
        }

        /**
         * get Regex Value.
         * @return regex value
         */
        public String getRegexValue() {
            return regexValue;
        }

        /**
         * Type create from value.
         *
         * @param value string value
         * @return Type object
         */
        public static Type fromValue(final String value) {
            for (Type type : values()) {
                if (type.name().equalsIgnoreCase(value)) {
                    return type;
                }
            }
            throw new IllegalArgumentException(
                "Unknown enum type " + value + ", Allowed values are " + Arrays.toString(values()));
        }
    }
}
