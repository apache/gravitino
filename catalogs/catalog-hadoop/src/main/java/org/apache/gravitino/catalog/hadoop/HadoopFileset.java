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
package org.apache.gravitino.catalog.hadoop;

import org.apache.gravitino.connector.BaseFileset;

public class HadoopFileset extends BaseFileset {

  public static class Builder extends BaseFilesetBuilder<Builder, HadoopFileset> {
    /** Creates a new instance of {@link Builder}. */
    private Builder() {}

    @Override
    protected HadoopFileset internalBuild() {
      HadoopFileset fileset = new HadoopFileset();
      fileset.name = name;
      fileset.comment = comment;
      fileset.storageLocation = storageLocation;
      fileset.type = type;
      fileset.properties = properties;
      fileset.auditInfo = auditInfo;
      return fileset;
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
