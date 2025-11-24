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

import java.util.Map;
import org.apache.gravitino.credential.AwsIrsaCredential;
import org.apache.gravitino.credential.Credential;
import org.apache.gravitino.credential.CredentialContext;
import org.apache.gravitino.credential.CredentialGenerator;
import org.apache.gravitino.credential.CredentialProvider;

/**
 * AWS IRSA credential provider that supports both basic IRSA credentials and fine-grained
 * path-based access control using AWS session policies.
 *
 * <p>This provider operates in two modes:
 *
 * <ul>
 *   <li><b>Basic IRSA mode</b>: For non-path-based credential contexts, returns credentials with
 *       full permissions of the associated IAM role (backward compatibility)
 *   <li><b>Fine-grained mode</b>: For path-based credential contexts (e.g., table access with
 *       vended credentials), uses AWS session policies to restrict permissions to specific S3 paths
 * </ul>
 *
 * <p>The fine-grained mode leverages AWS session policies with AssumeRoleWithWebIdentity to create
 * temporary credentials with restricted permissions. Session policies can only reduce (not expand)
 * the permissions already granted by the IAM role:
 *
 * <ul>
 *   <li>s3:GetObject, s3:GetObjectVersion for read access to specific table paths only
 *   <li>s3:ListBucket with s3:prefix conditions limiting to table directories only
 *   <li>s3:PutObject, s3:DeleteObject for write operations on specific paths only
 *   <li>s3:GetBucketLocation for bucket metadata access
 * </ul>
 *
 * <p>Prerequisites for fine-grained mode:
 *
 * <ul>
 *   <li>EKS cluster with IRSA properly configured
 *   <li>AWS_WEB_IDENTITY_TOKEN_FILE environment variable pointing to service account token
 *   <li>IAM role configured for IRSA with broad S3 permissions (session policy will restrict them)
 *   <li>Optional: s3-role-arn for assuming different role (if not provided, uses IRSA role
 *       directly)
 * </ul>
 */
public class AwsIrsaCredentialProvider implements CredentialProvider {

  private Map<String, String> properties;
  private volatile CredentialGenerator<AwsIrsaCredential> generator;

  @Override
  public void initialize(Map<String, String> properties) {
    this.properties = properties;
  }

  @Override
  public void close() {
    // No external resources to close
  }

  @Override
  public String credentialType() {
    return AwsIrsaCredential.AWS_IRSA_CREDENTIAL_TYPE;
  }

  @Override
  public String getGeneratorClassName() {
    return "org.apache.gravitino.s3.credential.AwsIrsaCredentialGenerator";
  }

  @Override
  public Credential getCredential(CredentialContext context) {
    // Double-checked locking for lazy loading the generator
    if (generator == null) {
      synchronized (this) {
        if (generator == null) {
          this.generator = loadGenerator();
        }
      }
    }

    try {
      return generator.generate(properties, context);
    } catch (Exception e) {
      throw new RuntimeException("Failed to generate AWS IRSA credential", e);
    }
  }
}
