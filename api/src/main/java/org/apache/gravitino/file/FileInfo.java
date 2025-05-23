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
package org.apache.gravitino.file;

import java.time.Instant;

public interface FileInfo {

    /** @return Name of the file object. */
    String name();

    /** @return Whether this is a file (true). */
    boolean isFile();

    /** @return The file size in bytes. */
    long size();

    /** @return The last modification time as an Instant. */
    Instant lastModified();

    /** @return The block size in bytes, or 0 if not applicable. */
    long blockSize();

    /** @return The full path of the file or directory within the fileset. */
    String path();
}
