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
package org.apache.gravitino.hive;

import lombok.ToString;
import org.apache.gravitino.connector.BaseSchema;

/** Represents an Apache Hive Schema (Database) entity in the Hive Metastore catalog. */
@ToString
public class HiveSchema extends BaseSchema {
  private String catalogName;

  protected HiveSchema() {}

  public String catalogName() {
    return catalogName;
  }

  /** A builder class for constructing HiveSchema instances. */
  public static class Builder extends BaseSchemaBuilder<Builder, HiveSchema> {

    private String catalogName;

    /** Creates a new instance of {@link Builder}. */
    private Builder() {}

    /**
     * Sets the catalog name of the HiveSchema.
     *
     * @param catalogName The catalog name of the HiveSchema.
     * @return The builder instance.
     */
    public Builder withCatalogName(String catalogName) {
      this.catalogName = catalogName;
      return this;
    }

    /**
     * Internal method to build a HiveSchema instance using the provided values.
     *
     * @return A new HiveSchema instance with the configured values.
     */
    @Override
    protected HiveSchema internalBuild() {
      HiveSchema hiveSchema = new HiveSchema();
      hiveSchema.name = name;
      hiveSchema.comment = comment;
      hiveSchema.properties = properties;
      hiveSchema.auditInfo = auditInfo;
      hiveSchema.catalogName = catalogName;

      return hiveSchema;
    }
  }

  /**
   * Creates a new instance of {@link Builder}.
   *
   * @return The new instance.
   */
  public static Builder builder() {
    return new Builder();
  }
}
