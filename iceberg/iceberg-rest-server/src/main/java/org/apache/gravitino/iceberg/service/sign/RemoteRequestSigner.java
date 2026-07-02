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
package org.apache.gravitino.iceberg.service.sign;

import java.util.Map;
import org.apache.gravitino.credential.Credential;
import org.apache.gravitino.iceberg.common.IcebergConfig;
import org.apache.iceberg.rest.requests.RemoteSignRequest;
import org.apache.iceberg.rest.responses.RemoteSignResponse;

/** Signs object-storage HTTP requests for Iceberg REST remote-signing. */
public interface RemoteRequestSigner {

  /**
   * Returns the provider handled by this signer.
   *
   * @return provider
   */
  RemoteSignSupport.Provider provider();

  /**
   * Builds client config injected on load/create/register when remote-signing is requested.
   *
   * @param icebergConfig catalog configuration
   * @param signerEndpoint IRC signer endpoint for the table
   * @return client config properties
   */
  Map<String, String> clientConfig(IcebergConfig icebergConfig, String signerEndpoint);

  /**
   * Signs a remote request described by the Iceberg REST {@link RemoteSignRequest}.
   *
   * @param request remote sign request from the Iceberg REST client
   * @param credential Gravitino credential used to sign the request
   * @return signed URI and headers for the client to call object storage directly
   */
  RemoteSignResponse sign(RemoteSignRequest request, Credential credential);
}
