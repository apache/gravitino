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

package org.apache.gravitino.cache;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.gravitino.NameIdentifier;

/** A segmented lock manager based on NameIdentifier hash. */
public class SegmentedLock {
  private final ReadWriteLock[] locks;

  public SegmentedLock(int numSegments) {
    locks = new ReentrantReadWriteLock[numSegments];
    for (int i = 0; i < numSegments; i++) {
      locks[i] = new ReentrantReadWriteLock();
    }
  }

  public ReadWriteLock getLock(NameIdentifier ident) {
    int hash = ident.hashCode();
    int segmentIndex = Math.abs(hash % locks.length);
    return locks[segmentIndex];
  }

  public Lock readLock(NameIdentifier ident) {
    return getLock(ident).readLock();
  }

  public Lock writeLock(NameIdentifier ident) {
    return getLock(ident).writeLock();
  }
}
