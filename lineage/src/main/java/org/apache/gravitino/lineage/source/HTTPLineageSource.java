/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */

package org.apache.gravitino.lineage.source;

import com.google.common.collect.ImmutableSet;
import java.util.Map;
import java.util.Set;
import org.apache.gravitino.lineage.LineageDispatcher;
import org.apache.gravitino.lineage.source.rest.LineageOperations;
import org.apache.gravitino.server.web.SupportsRESTPackages;

public class HTTPLineageSource implements LineageSource, SupportsRESTPackages {
  @Override
  public void initialize(Map<String, String> configs, LineageDispatcher dispatcher) {}

  @Override
  public Set<String> getRESTPackages() {
    return ImmutableSet.of(LineageOperations.class.getPackage().getName());
  }
}
