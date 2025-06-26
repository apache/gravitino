package org.apache.gravitino.credential;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;

/** Generic AWS IRSA credential. */
public class AwsIrsaCredential implements Credential {

  public static final String AWS_IRSA_CREDENTIAL_TYPE = "aws-irsa-token";
  public static final String ACCESS_KEY_ID = "access-key-id";
  public static final String SECRET_ACCESS_KEY = "secret-access-key";
  public static final String SESSION_TOKEN = "session-token";

  private String accessKeyId;
  private String secretAccessKey;
  private String sessionToken;
  private long expireTimeInMs;

  public AwsIrsaCredential(String accessKeyId, String secretAccessKey, String sessionToken, long expireTimeInMs) {
    validate(accessKeyId, secretAccessKey, sessionToken);
    this.accessKeyId = accessKeyId;
    this.secretAccessKey = secretAccessKey;
    this.sessionToken = sessionToken;
    this.expireTimeInMs = expireTimeInMs;
  }

  public AwsIrsaCredential() {}

  @Override
  public String credentialType() {
    return AWS_IRSA_CREDENTIAL_TYPE;
  }

  @Override
  public long expireTimeInMs() {
    return expireTimeInMs;
  }

  @Override
  public Map<String, String> credentialInfo() {
    ImmutableMap.Builder<String, String> builder = new ImmutableMap.Builder<>();
    builder.put(ACCESS_KEY_ID, accessKeyId);
    builder.put(SECRET_ACCESS_KEY, secretAccessKey);
    if (sessionToken != null) {
      builder.put(SESSION_TOKEN, sessionToken);
    }
    return builder.build();
  }

  @Override
  public void initialize(Map<String, String> credentialInfo, long expireTimeInMs) {
    String accessKeyId = credentialInfo.get(ACCESS_KEY_ID);
    String secretAccessKey = credentialInfo.get(SECRET_ACCESS_KEY);
    String sessionToken = credentialInfo.get(SESSION_TOKEN);
    validate(accessKeyId, secretAccessKey, sessionToken);
    this.accessKeyId = accessKeyId;
    this.secretAccessKey = secretAccessKey;
    this.sessionToken = sessionToken;
    this.expireTimeInMs = expireTimeInMs;
  }

  public String accessKeyId() {
    return accessKeyId;
  }

  public String secretAccessKey() {
    return secretAccessKey;
  }

  public String sessionToken() {
    return sessionToken;
  }

  private void validate(String accessKeyId, String secretAccessKey, String sessionToken) {
    Preconditions.checkArgument(StringUtils.isNotBlank(accessKeyId), "Access key Id should not be empty");
    Preconditions.checkArgument(StringUtils.isNotBlank(secretAccessKey), "Secret access key should not be empty");
    Preconditions.checkArgument(StringUtils.isNotBlank(sessionToken), "Session token should not be empty");
  }
} 