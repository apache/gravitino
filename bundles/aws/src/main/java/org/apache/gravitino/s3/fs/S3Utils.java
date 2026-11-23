/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */

package org.apache.gravitino.s3.fs;

import com.google.common.collect.ImmutableSet;
import java.util.Set;
import org.apache.gravitino.credential.AwsIrsaCredential;
import org.apache.gravitino.credential.Credential;
import org.apache.gravitino.credential.S3SecretKeyCredential;
import org.apache.gravitino.credential.S3TokenCredential;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class S3Utils {

  private static final Logger LOG = LoggerFactory.getLogger(S3Utils.class);

  /**
   * A class that only exists in Hadoop 3.4 and later, whose S3A connector is built on the AWS SDK
   * v2. Hadoop 3.3 and earlier are built on the AWS SDK v1.
   */
  private static final String HADOOP_AWS_SDK_V2_MARKER_CLASS =
      "org.apache.hadoop.fs.s3a.adapter.AwsV1BindingSupport";

  private static final String SDK_V1_CREDENTIALS_PROVIDER =
      "com.amazonaws.auth.AWSCredentialsProvider";

  private static final String SDK_V2_CREDENTIALS_PROVIDER =
      "software.amazon.awssdk.auth.credentials.AwsCredentialsProvider";

  /**
   * AWS SDK v1 credentials providers that Hadoop 3.4 and later map to their own providers by name,
   * so the v1 classes are not needed on the classpath.
   */
  private static final Set<String> HADOOP_REMAPPED_SDK_V1_PROVIDERS =
      ImmutableSet.of(
          "com.amazonaws.auth.AnonymousAWSCredentials",
          "com.amazonaws.auth.EC2ContainerCredentialsProviderWrapper",
          "com.amazonaws.auth.InstanceProfileCredentialsProvider",
          "com.amazonaws.auth.EnvironmentVariableCredentialsProvider",
          "com.amazonaws.auth.profile.ProfileCredentialsProvider");

  /**
   * Check whether the S3A connector of Hadoop on the classpath is built on the AWS SDK v2, that is,
   * whether it is Hadoop 3.4 or later.
   *
   * @return True if the S3A connector uses the AWS SDK v2, false if it uses the AWS SDK v1.
   */
  static boolean isHadoopAwsSdkV2() {
    return loadClass(HADOOP_AWS_SDK_V2_MARKER_CLASS) != null;
  }

  /**
   * Check whether the class is a credentials provider that the S3A connector on the classpath can
   * use. Hadoop 3.4 and later accept AWS SDK v2 providers and, through an adapter, AWS SDK v1
   * providers. Hadoop 3.3 and earlier only accept AWS SDK v1 providers, even if the AWS SDK v2 is
   * also on the classpath. The SDK interfaces are resolved by name, because only one of the two
   * SDKs may be on the classpath.
   *
   * @param providerClass The class to check.
   * @return True if the S3A connector on the classpath can use the class as a credentials provider.
   */
  static boolean isCredentialsProvider(Class<?> providerClass) {
    return isAssignableTo(providerClass, SDK_V1_CREDENTIALS_PROVIDER)
        || (isHadoopAwsSdkV2() && isAssignableTo(providerClass, SDK_V2_CREDENTIALS_PROVIDER));
  }

  /**
   * Check whether the name is an AWS SDK v1 credentials provider that Hadoop 3.4 and later map to
   * an AWS SDK v2 provider by name, so that the class does not need to exist.
   *
   * @param providerName The credentials provider class name.
   * @return True if the S3A connector remaps the name.
   */
  static boolean isRemappedByHadoop(String providerName) {
    return isHadoopAwsSdkV2() && HADOOP_REMAPPED_SDK_V1_PROVIDERS.contains(providerName);
  }

  private static boolean isAssignableTo(Class<?> providerClass, String interfaceName) {
    Class<?> providerInterface = loadClass(interfaceName);
    return providerInterface != null && providerInterface.isAssignableFrom(providerClass);
  }

  private static Class<?> loadClass(String className) {
    try {
      return Class.forName(className, false, S3Utils.class.getClassLoader());
    } catch (ClassNotFoundException e) {
      return null;
    } catch (LinkageError e) {
      LOG.warn("Class {} is present but cannot be loaded, treating it as absent", className, e);
      return null;
    }
  }

  /**
   * Get the credential from the credential array. Using dynamic credential first, if not found,
   * uses static credential.
   *
   * @param credentials The credential array.
   * @return A credential. Null if not found.
   */
  static Credential getSuitableCredential(Credential[] credentials) {
    // Use dynamic credential if found.
    for (Credential credential : credentials) {
      if (credential instanceof S3TokenCredential) {
        return credential;
      }
    }
    for (Credential credential : credentials) {
      if (credential instanceof AwsIrsaCredential) {
        return credential;
      }
    }
    // If dynamic credential not found, use the static one
    for (Credential credential : credentials) {
      if (credential instanceof S3SecretKeyCredential) {
        return credential;
      }
    }
    return null;
  }
}
