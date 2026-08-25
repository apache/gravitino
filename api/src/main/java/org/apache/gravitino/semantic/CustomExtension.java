/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.gravitino.semantic;

import com.google.common.base.Preconditions;
import java.util.Objects;
import javax.annotation.Nullable;
import org.apache.gravitino.annotation.Evolving;

/** Vendor-specific Semantic Model attributes retained for extensibility. */
@Evolving
public final class CustomExtension {

  private final String vendorName;
  private final String data;

  private CustomExtension(Builder builder) {
    this.vendorName = builder.vendorName;
    this.data = builder.data;
  }

  /**
   * Creates a builder for a custom extension.
   *
   * @return A new builder.
   */
  public static Builder builder() {
    return new Builder();
  }

  /**
   * Returns the vendor name associated with this extension.
   *
   * @return The vendor name.
   */
  public String vendorName() {
    return vendorName;
  }

  /**
   * Returns the vendor-specific data encoded as a JSON string.
   *
   * @return The extension data.
   */
  public String data() {
    return data;
  }

  /**
   * Compares this custom extension with another object.
   *
   * @param other The object to compare.
   * @return {@code true} if the object has the same vendor name and data.
   */
  @Override
  public boolean equals(@Nullable Object other) {
    if (this == other) {
      return true;
    }
    if (!(other instanceof CustomExtension)) {
      return false;
    }
    CustomExtension that = (CustomExtension) other;
    return vendorName.equals(that.vendorName) && data.equals(that.data);
  }

  /**
   * Returns the hash code of this custom extension.
   *
   * @return The hash code.
   */
  @Override
  public int hashCode() {
    return Objects.hash(vendorName, data);
  }

  /**
   * Returns a string representation of this custom extension.
   *
   * @return The string representation.
   */
  @Override
  public String toString() {
    return "CustomExtension{" + "vendorName='" + vendorName + '\'' + ", data='" + data + '\'' + '}';
  }

  /** A builder for {@link CustomExtension}. */
  public static final class Builder {

    private String vendorName;
    private String data;

    private Builder() {}

    /**
     * Sets the vendor name.
     *
     * @param vendorName The vendor name.
     * @return This builder.
     */
    public Builder withVendorName(String vendorName) {
      this.vendorName = vendorName;
      return this;
    }

    /**
     * Sets the vendor-specific data encoded as a JSON string.
     *
     * @param data The extension data.
     * @return This builder.
     */
    public Builder withData(String data) {
      this.data = data;
      return this;
    }

    /**
     * Builds a custom extension.
     *
     * @return The immutable custom extension.
     * @throws IllegalArgumentException If the vendor name or data is null.
     */
    public CustomExtension build() {
      Preconditions.checkArgument(vendorName != null, "vendorName must not be null");
      Preconditions.checkArgument(data != null, "data must not be null");
      return new CustomExtension(this);
    }
  }
}
