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
package org.apache.gravitino.paimon.service;

import java.util.List;
import javax.ws.rs.NotFoundException;
import javax.ws.rs.core.Response;
import org.apache.paimon.rest.RESTApi;
import org.apache.paimon.rest.exceptions.BadRequestException;
import org.apache.paimon.rest.exceptions.NotImplementedException;
import org.apache.paimon.rest.exceptions.ServiceFailureException;
import org.apache.paimon.rest.responses.ErrorResponse;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/** Tests error mapping without reinterpreting user-controlled messages as format strings. */
public class TestPaimonExceptionMapper {

  @Test
  void testPercentQuotesAndEscapesArePreservedInJsonErrors() throws Exception {
    String message = "invalid %s %candidate 100% \"quoted\" \\ newline\n";
    List<Exception> failures =
        List.of(
            new IllegalArgumentException(message),
            new BadRequestException("%s", message),
            new NotFoundException(message),
            new UnsupportedOperationException(message),
            new IllegalStateException(message),
            new ServiceFailureException("%s", message),
            new NotImplementedException("%s", message));
    List<Integer> statuses = List.of(400, 400, 404, 501, 500, 500, 501);
    for (int i = 0; i < failures.size(); i++) {
      try (Response response = new PaimonExceptionMapper().toResponse(failures.get(i))) {
        ErrorResponse error = RESTApi.fromJson((String) response.getEntity(), ErrorResponse.class);
        Assertions.assertEquals(statuses.get(i).intValue(), response.getStatus());
        Assertions.assertEquals(response.getStatus(), error.getCode());
        Assertions.assertTrue(error.getMessage().contains(message));
      }
    }
  }

  @Test
  void testInvalidNumericPageTokenWithPercentReturnsBadRequest() {
    BadRequestException error =
        Assertions.assertThrows(
            BadRequestException.class,
            () ->
                PaimonRESTUtils.buildPagedEntitiesNumeric(List.of(1L), Long::longValue, 10, "%s"));
    Assertions.assertTrue(error.getMessage().contains("%s"));
  }

  @Test
  void testMalformedJsonWithPercentReturnsBadRequest() {
    Assertions.assertThrows(
        BadRequestException.class, () -> PaimonRESTUtils.fromJson("{\"%s\":", ErrorResponse.class));
  }
}
