/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.graviton.meta;

/**
 * The major and minor versions of a schema.
 */
public enum SchemaVersion {
    V_0_1(0, 1);

    public final int majorVersion;

    public final int minorVersion;

    /**
     * Constructs a SchemaVersion enum value with the provided version numbers.
     *
     * @param majorVersion The major version number.
     * @param minorVersion The minor version number.
     */
    SchemaVersion(int majorVersion, int minorVersion) {
        this.majorVersion = majorVersion;
        this.minorVersion = minorVersion;
    }

    /**
     * The major version number of the schema.
     *
     * @return The major version number.
     */
    public int getMajorVersion() {
        return majorVersion;
    }

    /**
     * The minor version number of the schema.
     *
     * @return The minor version number.
     */
    public int getMinorVersion() {
        return minorVersion;
    }

    /**
     * The SchemaVersion enum value corresponding to the provided major and minor version numbers.
     *
     * @param majorVersion The major version number.
     * @param minorVersion The minor version number.
     * @return The corresponding SchemaVersion enum value.
     * @throws IllegalArgumentException If no schema version is found.
     */
    public static SchemaVersion forValues(int majorVersion, int minorVersion) {
        for (SchemaVersion schemaVersion : SchemaVersion.values()) {
            if (schemaVersion.majorVersion == majorVersion
                    && schemaVersion.minorVersion == minorVersion) {
                return schemaVersion;
            }
        }

        throw new IllegalArgumentException(
            String.format(
                "No schema version found for major version %d and minor version %d",
                majorVersion, minorVersion));
    }
}
