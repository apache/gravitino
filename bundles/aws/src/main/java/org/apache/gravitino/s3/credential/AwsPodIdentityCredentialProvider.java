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

package org.apache.gravitino.s3.credential;

import org.apache.gravitino.credential.AwsPodIdentityCredential;
import org.apache.gravitino.credential.CredentialProviderDelegator;

/**
 * AWS EKS Pod Identity credential provider that supports both basic Pod Identity credentials and
 * fine-grained path-based access control using AWS session policies.
 *
 * <p>This provider operates in two modes:
 *
 * <ul>
 *   <li><b>Basic Pod Identity mode</b>: For non-path-based credential contexts, returns credentials
 *       with full permissions of the IAM role associated with the Kubernetes service account.
 *   <li><b>Fine-grained mode</b>: For path-based credential contexts (e.g., table access with
 *       vended credentials), uses AWS session policies to restrict permissions to specific S3 paths
 * </ul>
 *
 * <p>The fine-grained mode leverages AWS session policies with AssumeRole to create temporary
 * credentials with restricted permissions. Session policies can only reduce (not expand) the
 * permissions already granted by the IAM role.
 *
 * <p>Prerequisites for fine-grained mode:
 *
 * <ul>
 *   <li>EKS cluster with Pod Identity agent installed and configured
 *   <li>IAM role associated with the Kubernetes service account
 *   <li>Target IAM role configured to allow assumption from the Pod Identity role
 *   <li>Optional: s3-role-arn for assuming different role (if not provided, assumes the role ARN
 *       specified in s3-role-arn)
 * </ul>
 */
public class AwsPodIdentityCredentialProvider
    extends CredentialProviderDelegator<AwsPodIdentityCredential> {

  @Override
  public String credentialType() {
    return AwsPodIdentityCredential.AWS_POD_IDENTITY_CREDENTIAL_TYPE;
  }

  @Override
  public String getGeneratorClassName() {
    return "org.apache.gravitino.s3.credential.AwsPodIdentityCredentialGenerator";
  }
}
