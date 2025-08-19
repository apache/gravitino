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

import static org.junit.Assert.assertEquals;

import java.util.List;
import org.junit.jupiter.api.Test;

public class TestPageRequestGenerator {

  @Test
  public void test() {
    List<PageRequestGenerator.PageRequest> result = PageRequestGenerator.generate(100, 10);
    checkPageRequestList(10, result);
    result = PageRequestGenerator.generate(99, 10);
    checkPageRequestList(10, result);
    result = PageRequestGenerator.generate(101, 10);
    checkPageRequestList(11, result);
    result = PageRequestGenerator.generate(101, 1);
    checkPageRequestList(101, result);
  }

  private void checkPageRequestList(long pageNum, List<PageRequestGenerator.PageRequest> list) {
    assertEquals(pageNum, list.size());
    assertEquals(pageNum, (long) list.get((int) (pageNum - 1)).getPageNum());
  }
}
