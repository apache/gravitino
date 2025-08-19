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

import java.util.ArrayList;
import java.util.List;

/** PageRequestGenerator for building the list of CompletableFuture needed for paginated queries */
public class PageRequestGenerator {

  public static List<PageRequest> generate(long totalSize, long pageSize) {
    List<PageRequest> pageRequests = new ArrayList<>();
    for (int i = 1; i <= totalSize / pageSize + 1; i++) {
      pageRequests.add(new PageRequest(i, pageSize));
    }
    return pageRequests;
  }

  public static class PageRequest {
    private final long pageNum;
    private final long pageSize;

    public PageRequest(long pageSize, long pageNum) {
      this.pageSize = pageSize;
      this.pageNum = pageNum;
    }

    public Long getPageNum() {
      return pageNum;
    }

    public Long getPageSize() {
      return pageSize;
    }
  }
}
