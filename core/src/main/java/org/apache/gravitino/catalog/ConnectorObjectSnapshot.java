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

import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.gravitino.Audit;
import org.apache.gravitino.Schema;
import org.apache.gravitino.dto.util.DTOConverters;
import org.apache.gravitino.file.FileInfo;
import org.apache.gravitino.file.Fileset;
import org.apache.gravitino.messaging.Topic;
import org.apache.gravitino.model.Model;
import org.apache.gravitino.model.ModelVersion;
import org.apache.gravitino.rel.Table;
import org.apache.gravitino.rel.View;
import org.apache.gravitino.rel.partitions.Partition;

/** Creates parent-ClassLoader metadata snapshots from connector-backed operation results. */
final class ConnectorObjectSnapshot {

  private ConnectorObjectSnapshot() {}

  @SuppressWarnings("unchecked")
  static <R> R detach(R result) {
    if (result instanceof Table) {
      return (R) DTOConverters.toDTO((Table) result);
    } else if (result instanceof View) {
      return (R) DTOConverters.toDTO((View) result);
    } else if (result instanceof Schema) {
      return (R) DTOConverters.toDTO((Schema) result);
    } else if (result instanceof Fileset) {
      return (R) new FilesetSnapshot((Fileset) result);
    } else if (result instanceof Topic) {
      return (R) DTOConverters.toDTO((Topic) result);
    } else if (result instanceof Model) {
      return (R) DTOConverters.toDTO((Model) result);
    } else if (result instanceof ModelVersion) {
      return (R) DTOConverters.toDTO((ModelVersion) result);
    } else if (result instanceof Partition) {
      return (R) DTOConverters.toDTO((Partition) result);
    } else if (result instanceof ModelVersion[]) {
      return (R) DTOConverters.toDTOs((ModelVersion[]) result);
    } else if (result instanceof Partition[]) {
      return (R) DTOConverters.toDTOs((Partition[]) result);
    } else if (result instanceof FileInfo[]) {
      return (R) DTOConverters.toDTO((FileInfo[]) result);
    }
    return result;
  }

  private static final class FilesetSnapshot implements Fileset {

    private final String name;
    @Nullable private final String comment;
    private final Type type;
    private final Map<String, String> storageLocations;
    @Nullable private final Map<String, String> properties;
    private final Audit audit;

    private FilesetSnapshot(Fileset fileset) {
      this.name = fileset.name();
      this.comment = fileset.comment();
      this.type = fileset.type();
      this.storageLocations = new HashMap<>(fileset.storageLocations());
      this.properties = copyNullable(fileset.properties());
      this.audit = DTOConverters.toDTO(fileset.auditInfo());
    }

    @Override
    public String name() {
      return name;
    }

    @Nullable
    @Override
    public String comment() {
      return comment;
    }

    @Override
    public Type type() {
      return type;
    }

    @Override
    public Map<String, String> storageLocations() {
      return storageLocations;
    }

    @Nullable
    @Override
    public Map<String, String> properties() {
      return properties;
    }

    @Override
    public Audit auditInfo() {
      return audit;
    }

    @Nullable
    private static Map<String, String> copyNullable(@Nullable Map<String, String> source) {
      return source == null ? null : new HashMap<>(source);
    }
  }
}
