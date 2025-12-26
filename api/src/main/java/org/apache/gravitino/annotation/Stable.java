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
package org.apache.gravitino.annotation;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * APIs that can evolve while retaining compatibility for feature releases (0.5.0 to 0.6.0). The
 * compatibility can be broken only in major releases (1.x to 2.x).
 */
@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target({
  ElementType.PACKAGE,
  ElementType.CONSTRUCTOR,
  ElementType.METHOD,
  ElementType.TYPE,
  ElementType.FIELD
})
public @interface Stable {

  /**
   * @return The version when the API was first marked stable.
   */
  String since();
}
