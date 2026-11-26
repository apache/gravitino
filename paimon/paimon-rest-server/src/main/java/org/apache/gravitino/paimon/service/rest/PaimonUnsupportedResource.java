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
package org.apache.gravitino.paimon.service.rest;

import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.apache.paimon.rest.exceptions.NotImplementedException;

/** Handler for explicitly unsupported protocol families and their sub-resources. */
@Produces(MediaType.APPLICATION_JSON)
public class PaimonUnsupportedResource {

  /**
   * Locates a nested unsupported resource.
   *
   * @return the unsupported resource handler
   */
  @Path("{remaining:.+}")
  public PaimonUnsupportedResource nestedResources() {
    return new PaimonUnsupportedResource();
  }

  /**
   * Rejects reads of unsupported resources.
   *
   * @return never; always throws
   */
  @GET
  public Response get() {
    throw new NotImplementedException("This Paimon REST operation is not supported yet.");
  }

  /**
   * Rejects creates and updates of unsupported resources.
   *
   * @return never; always throws
   */
  @POST
  public Response post() {
    throw new NotImplementedException("This Paimon REST operation is not supported yet.");
  }

  /**
   * Rejects replacements of unsupported resources.
   *
   * @return never; always throws
   */
  @PUT
  public Response put() {
    throw new NotImplementedException("This Paimon REST operation is not supported yet.");
  }

  /**
   * Rejects deletes of unsupported resources.
   *
   * @return never; always throws
   */
  @DELETE
  public Response delete() {
    throw new NotImplementedException("This Paimon REST operation is not supported yet.");
  }
}
