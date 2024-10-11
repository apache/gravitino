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

public class TestHudiSchema extends HudiSchema<TestHudiSchema> {

  public static Builder builder() {
    return new Builder();
  }

  @Override
  public TestHudiSchema fromHudiSchema() {
    return this;
  }

  public static class Builder extends HudiSchema.Builder<TestHudiSchema> {

    @Override
    protected TestHudiSchema simpleBuild() {
      TestHudiSchema schema = new TestHudiSchema();
      schema.name = name;
      schema.comment = comment;
      schema.properties = properties;
      schema.auditInfo = auditInfo;
      return schema;
    }

    @Override
    protected HudiSchema<TestHudiSchema> buildFromSchema(TestHudiSchema schema) {
      return simpleBuild();
    }
  }
}
