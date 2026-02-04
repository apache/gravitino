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
package org.apache.gravitino.rel;

import java.util.Collections;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.gravitino.Auditable;
import org.apache.gravitino.Namespace;
import org.apache.gravitino.annotation.Unstable;

/**
 * An interface representing a view in a {@link Namespace}. It defines the basic properties of a
 * view. A catalog implementation with {@link ViewCatalog} should implement this interface.
 */
@Unstable
public interface View extends Auditable {

  /**
   * @return The name of the view.
   */
  String name();

  /**
   * @return The comment of the view, null if no comment is set.
   */
  @Nullable
  default String comment() {
    return null;
  }

  /**
   * @return The properties of the view, empty map if no properties are set.
   */
  default Map<String, String> properties() {
    return Collections.emptyMap();
  }
}
