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
package org.apache.gravitino.catalog.lakehouse.hudi;

import org.apache.gravitino.connector.BaseSchema;

/**
 * Represents a schema (database) from a Hudi catalog. Different Hudi catalog backends should extend
 * this abstract class and the inner Builder class to provide the conversion between the backend
 * schema and the HudiSchema.
 *
 * @param <DATABASE> the schema (database) type from the backend
 */
public abstract class HudiSchema<DATABASE> extends BaseSchema {
  /**
   * Converts the HudiSchema to the backend schema (database).
   *
   * @return the backend schema (database)
   */
  public abstract DATABASE fromHudiSchema();

  /**
   * Builder class for HudiSchema. The builder should be extended by the backend schema builder to
   * provide the conversion between the backend schema and the HudiSchema.
   *
   * @param <T> the schema (database) type from the backend
   */
  public abstract static class Builder<T> extends BaseSchemaBuilder<Builder<T>, HudiSchema> {

    /** The backend schema (database) to be converted to HudiSchema. */
    T backendSchema;

    /**
     * Sets the backend schema (database) to be converted to HudiSchema.
     *
     * @param backendSchema the backend schema (database)
     * @return this builder
     */
    public Builder<T> withBackendSchema(T backendSchema) {
      this.backendSchema = backendSchema;
      return this;
    }

    /**
     * Builds the HudiSchema from the backend schema (database).
     *
     * @return the HudiSchema
     */
    @Override
    protected HudiSchema<T> internalBuild() {
      return backendSchema == null ? simpleBuild() : buildFromSchema(backendSchema);
    }

    /**
     * Builds a simple HudiSchema without the backend schema (database).
     *
     * @return the HudiSchema
     */
    protected abstract HudiSchema<T> simpleBuild();

    /**
     * Builds the HudiSchema from the backend schema (database).
     *
     * @param schema the backend schema (database)
     * @return the HudiSchema
     */
    protected abstract HudiSchema<T> buildFromSchema(T schema);

    @Override
    public HudiSchema<T> build() {
      return internalBuild();
    }
  }
}
