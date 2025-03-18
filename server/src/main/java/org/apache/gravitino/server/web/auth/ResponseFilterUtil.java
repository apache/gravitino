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
package org.apache.gravitino.server.web.auth;

import java.util.Arrays;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * The ResponseFilterUtil uses GravitinoAuthorizer to filter the returned result array to ensure
 * that only elements with access rights are returned.
 */
public class ResponseFilterUtil {

  public static <E> List<E> filter(
      List<E> list, String metalake, String act, Function<E, String> coverToResourceId) {
    GravitinoAuthorizer gravitinoAuthorizer =
        GravitinoAuthorizerProvider.INSTANCE.getGravitinoAuthorizer();
    return list.stream()
        .filter(
            e ->
                gravitinoAuthorizer.authorize(
                    getUserIdFromRequestContext(), metalake, coverToResourceId.apply(e), act))
        .collect(Collectors.toList());
  }

  public static <E> List<E> filter(
      E[] arrs, String metalake, String act, Function<E, String> coverToResourceId) {
    GravitinoAuthorizer gravitinoAuthorizer =
        GravitinoAuthorizerProvider.INSTANCE.getGravitinoAuthorizer();
    return Arrays.stream(arrs)
        .filter(
            e ->
                gravitinoAuthorizer.authorize(
                    getUserIdFromRequestContext(), metalake, coverToResourceId.apply(e), act))
        .collect(Collectors.toList());
  }

  private static String getUserIdFromRequestContext() {
    return "3606534323078438382";
  }
}
