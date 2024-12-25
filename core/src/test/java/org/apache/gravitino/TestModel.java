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
package org.apache.gravitino;

import org.apache.gravitino.connector.BaseModel;

public class TestModel extends BaseModel {

  public static class Builder extends BaseModelBuilder<Builder, TestModel> {

    private Builder() {}

    @Override
    protected TestModel internalBuild() {
      TestModel model = new TestModel();
      model.name = name;
      model.comment = comment;
      model.properties = properties;
      model.latestVersion = latestVersion;
      model.auditInfo = auditInfo;
      return model;
    }
  }

  public static Builder builder() {
    return new Builder();
  }
}
