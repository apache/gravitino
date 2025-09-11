/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *  http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.gravitino.server.authorization.policy;

import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.function.Consumer;
import org.apache.gravitino.storage.relational.po.OwnerRelPO;
import org.apache.gravitino.storage.relational.service.OwnerMetaService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Async load owner policy */
public class OwnerPolicyAsyncLoader {

  private static final Logger LOG = LoggerFactory.getLogger(OwnerPolicyAsyncLoader.class);

  private final Executor executor;

  private OwnerPolicyAsyncLoader(Executor executor) {
    this.executor = executor;
  }

  public static OwnerPolicyAsyncLoader create(Executor executor) {
    Preconditions.checkNotNull(executor, "Executor can not be null");
    return new OwnerPolicyAsyncLoader(executor);
  }

  public void loadAllOwnerAsync(long pageSize, Consumer<List<OwnerRelPO>> consumer) {
    List<PageRequestGenerator.PageRequest> requests =
        PageRequestGenerator.generate(OwnerMetaService.getInstance().countAllOwner(), pageSize);
    requests.forEach(
        (request) -> {
          this.loadOwnerAsync(request).thenAcceptAsync(consumer);
        });
  }

  private CompletableFuture<List<OwnerRelPO>> loadOwnerAsync(
      PageRequestGenerator.PageRequest request) {
    return CompletableFuture.supplyAsync(
            () ->
                OwnerMetaService.getInstance()
                    .listAllOwner(request.getPageNum(), request.getPageSize()),
            executor)
        .exceptionallyAsync(
            ex -> {
              LOG.error("Load owner policy in GravitinoAuthorizer error:{}", ex.getMessage(), ex);
              return new ArrayList<>();
            });
  }
}
