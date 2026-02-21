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

package org.apache.gravitino.maintenance.optimizer.monitor.job;

import java.util.List;
import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.maintenance.optimizer.api.monitor.JobProvider;
import org.apache.gravitino.maintenance.optimizer.common.OptimizerEnv;

public class JobProviderForTest implements JobProvider {

  public static final String NAME = "job-provider-for-test";
  public static final NameIdentifier JOB1 = NameIdentifier.parse("test.db.job1");
  public static final NameIdentifier JOB2 = NameIdentifier.parse("test.db.job2");

  @Override
  public String name() {
    return NAME;
  }

  @Override
  public void initialize(OptimizerEnv optimizerEnv) {}

  @Override
  public List<NameIdentifier> jobIdentifiers(NameIdentifier tableIdentifier) {
    return List.of(JOB1, JOB2);
  }

  @Override
  public void close() throws Exception {}
}
