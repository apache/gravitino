/*
 * Copyright 2024 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */
package com.datastrato.gravitino.rel;

import java.util.Objects;

/**
 * Capability that cannot be provided by a {@link TableCatalog} implementation.
 *
 * <p>TableCatalogs use {@link TableCatalog#inability()} to return a set of capabilities that it
 * does not support. The full set of capabilities is defined by {@link
 * TableCatalogInability.Capability}. The reason for the inability is provided by {@link
 * TableCatalogInability#unsupportedReason()}. For example, a TableCatalog may not support default
 * values for columns. In this case, it would return a set including {@link
 * TableCatalogInability.Capability#COLUMN_DEFAULT_VALUE} and a reason why it does not support it.
 */
public class TableCatalogInability {

  /**
   * Creates a new instance of {@link TableCatalogInability} for the given capability and
   * unsupported reason.
   *
   * @param capability The capability that is not supported.
   * @param reason The reason why the capability is not supported.
   * @return A new instance of {@link TableCatalogInability}.
   */
  public static TableCatalogInability unsupported(Capability capability, String reason) {
    return new TableCatalogInability(capability, reason);
  }

  private final Capability capability;
  private final String unsupportedReason;

  private TableCatalogInability(Capability capability, String unsupportedReason) {
    this.capability = capability;
    this.unsupportedReason = unsupportedReason;
  }

  /**
   * Indicates if the instance of {@link TableCatalogInability} is for the given capability.
   *
   * @param capability The capability to check.
   * @return True if the instance of {@link TableCatalogInability} is for the given capability,
   *     false otherwise.
   */
  public boolean isCapability(Capability capability) {
    return this.capability == capability;
  }

  /**
   * Returns the reason why the capability is not supported.
   *
   * @return The reason why the capability is not supported.
   */
  public String unsupportedReason() {
    return unsupportedReason;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    TableCatalogInability that = (TableCatalogInability) o;
    return capability == that.capability
        && Objects.equals(unsupportedReason, that.unsupportedReason);
  }

  @Override
  public int hashCode() {
    return Objects.hash(capability, unsupportedReason);
  }

  /** The capabilities that cannot be provided by a {@link TableCatalog} implementation. */
  public enum Capability {
    /** The TableCatalog does not support default values for columns. */
    COLUMN_DEFAULT_VALUE,
  }
}
