package org.apache.gravitino.s3.credential;

import java.util.Map;
import org.apache.gravitino.credential.AwsIrsaCredential;
import org.apache.gravitino.credential.Credential;
import org.apache.gravitino.credential.CredentialContext;
import org.apache.gravitino.credential.CredentialProvider;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;
import software.amazon.awssdk.auth.credentials.WebIdentityTokenFileCredentialsProvider;

public class AwsIrsaCredentialProvider implements CredentialProvider {

  private WebIdentityTokenFileCredentialsProvider credentialsProvider;

  @Override
  public void initialize(Map<String, String> properties) {
    // Use WebIdentityTokenFileCredentialsProvider for IRSA
    this.credentialsProvider = WebIdentityTokenFileCredentialsProvider.create();
  }

  @Override
  public void close() {}

  @Override
  public String credentialType() {
    return AwsIrsaCredential.AWS_IRSA_CREDENTIAL_TYPE;
  }

  @Override
  public Credential getCredential(CredentialContext context) {
    AwsCredentials creds = credentialsProvider.resolveCredentials();
    if (creds instanceof AwsSessionCredentials) {
      AwsSessionCredentials sessionCreds = (AwsSessionCredentials) creds;
      long expiration =
          sessionCreds.expirationTime().isPresent()
              ? sessionCreds.expirationTime().get().toEpochMilli()
              : 0L;
      return new AwsIrsaCredential(
          sessionCreds.accessKeyId(),
          sessionCreds.secretAccessKey(),
          sessionCreds.sessionToken(),
          expiration);
    } else {
      throw new IllegalStateException(
          "AWS IRSA credentials must be of type AwsSessionCredentials. "
              + "Check your EKS/IRSA configuration. Got: "
              + creds.getClass().getName());
    }
  }
}
