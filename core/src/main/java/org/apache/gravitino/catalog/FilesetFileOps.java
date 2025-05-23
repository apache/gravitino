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

package org.apache.gravitino.catalog;

import org.apache.gravitino.NameIdentifier;
import org.apache.gravitino.exceptions.NoSuchFilesetException;
import org.apache.gravitino.file.FileInfo;

/**
 * The {@code FilesetFileOps} interface defines operations for managing files within filesets.
 * This interface is designed to be used internally by the server and not exposed to public
 * client APIs to avoid confusion.
 */
public interface FilesetFileOps {

    /**
     * List files and directories within a fileset.
     *
     * @param ident A fileset identifier.
     * @param path The path within the fileset to list files from. Empty string for root.
     * @param limit Maximum number of files to return.
     * @param offset Starting position in the list for pagination.
     * @return Array of FileInfo objects containing file metadata.
     * @throws NoSuchFilesetException If the fileset does not exist.
     */
    FileInfo[] listFiles(NameIdentifier ident, String path, int limit, int offset)
        throws NoSuchFilesetException;
}
