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
package org.apache.gravitino.encryption.kms.azure;

import com.azure.core.credential.AccessToken;
import com.azure.core.credential.TokenCredential;
import com.azure.core.http.HttpClient;
import com.azure.core.http.HttpHeaderName;
import com.azure.core.http.HttpHeaders;
import com.azure.core.http.HttpRequest;
import com.azure.core.http.HttpResponse;
import com.azure.core.http.policy.FixedDelayOptions;
import com.azure.core.http.policy.RetryOptions;
import com.azure.security.keyvault.keys.KeyClient;
import com.azure.security.keyvault.keys.KeyClientBuilder;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import javax.annotation.Nullable;
import org.apache.gravitino.encryption.kms.KmsApi;
import org.apache.gravitino.encryption.kms.KmsClient;
import org.apache.gravitino.encryption.kms.KmsKeyProperties;
import org.apache.gravitino.encryption.kms.KmsReference;
import org.apache.gravitino.encryption.kms.TestKmsClientContract;
import org.apache.gravitino.exceptions.ConnectionFailedException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public class TestAzureKeyVaultKmsClient extends TestKmsClientContract {

  private static final String SOURCE = "azure-primary";
  private static final String VAULT_URL = "https://example.vault.azure.net";
  private static final String VERSION = "0123456789abcdef0123456789abcdef";
  private static final Instant NOW = Instant.parse("2030-01-01T00:00:00Z");
  private static final Clock CLOCK = Clock.fixed(NOW, ZoneOffset.UTC);
  private static final TokenCredential CREDENTIAL =
      request ->
          Mono.just(
              new AccessToken(
                  "test-token",
                  OffsetDateTime.ofInstant(NOW.plus(Duration.ofDays(1)), ZoneOffset.UTC)));

  private final RecordingHttpClient httpClient = new RecordingHttpClient(this::defaultResponse);
  private final AzureKeyVaultKmsClient client = createClient(httpClient, CLOCK);

  @Override
  protected KmsClient client() {
    return client;
  }

  @Override
  protected KmsReference usableKey() {
    return reference("usable");
  }

  @Override
  protected KmsReference missingKey() {
    return reference("missing");
  }

  @Test
  void testRequestsLatestAndPinnedVersionsThroughSdk() {
    KmsReference latest = reference("usable");
    KmsReference pinned = reference("usable", VERSION);

    client.getKeyProperties(latest);
    Assertions.assertEquals("/keys/usable/", httpClient.lastRequest().getUrl().getPath());

    client.getKeyProperties(pinned);
    Assertions.assertEquals("/keys/usable/" + VERSION, httpClient.lastRequest().getUrl().getPath());
    Assertions.assertTrue(httpClient.lastRequest().getUrl().getQuery().contains("api-version="));
  }

  @Test
  void testPreservesExactRequestedReference() {
    KmsReference reference = reference("usable", VERSION);

    Assertions.assertSame(reference, client.getKeyProperties(reference).reference());
  }

  @Test
  void testNormalizesEffectiveEnabledState() {
    Assertions.assertTrue(properties(true, null, null, "\"wrapKey\"").enabled());
    Assertions.assertFalse(properties(false, null, null, "\"wrapKey\"").enabled());
    Assertions.assertFalse(properties(null, null, null, "\"wrapKey\"").enabled());
    Assertions.assertFalse(properties(true, NOW.plusSeconds(1), null, "\"wrapKey\"").enabled());
    Assertions.assertTrue(properties(true, NOW, null, "\"wrapKey\"").enabled());
    Assertions.assertFalse(properties(true, null, NOW, "\"wrapKey\"").enabled());
    Assertions.assertTrue(properties(true, null, NOW.plusSeconds(1), "\"wrapKey\"").enabled());
  }

  @Test
  void testMapsOnlyNativeWrapCapabilities() {
    KmsKeyProperties encryptDecrypt = properties(true, null, null, "\"encrypt\",\"decrypt\"");
    Assertions.assertFalse(encryptDecrypt.supportsWrapping());
    Assertions.assertFalse(encryptDecrypt.supportsUnwrapping());

    KmsKeyProperties wrap = properties(true, null, null, "\"wrapKey\"");
    Assertions.assertTrue(wrap.supportsWrapping());
    Assertions.assertFalse(wrap.supportsUnwrapping());

    KmsKeyProperties unwrap = properties(true, null, null, "\"unwrapKey\"");
    Assertions.assertFalse(unwrap.supportsWrapping());
    Assertions.assertTrue(unwrap.supportsUnwrapping());
  }

  @Test
  void testMissingKeyHasAllCapabilitiesDisabled() {
    KmsKeyProperties properties = client.getKeyProperties(missingKey());

    Assertions.assertFalse(properties.present());
    Assertions.assertFalse(properties.enabled());
    Assertions.assertFalse(properties.supportsWrapping());
    Assertions.assertFalse(properties.supportsUnwrapping());
  }

  @ParameterizedTest
  @ValueSource(ints = {400, 401, 403, 408, 429, 500, 503})
  void testNormalizesProviderFailures(int statusCode) {
    AzureKeyVaultKmsClient failingClient =
        createClient(
            new RecordingHttpClient(request -> response(request, statusCode, errorJson())), CLOCK);

    Assertions.assertThrows(
        ConnectionFailedException.class, () -> failingClient.getKeyProperties(reference("usable")));
  }

  @Test
  void testNormalizesTransportFailure() {
    AzureKeyVaultKmsClient failingClient =
        createClient(
            new RecordingHttpClient(
                request -> {
                  throw new IllegalStateException("transport unavailable");
                }),
            CLOCK);

    Assertions.assertThrows(
        ConnectionFailedException.class, () -> failingClient.getKeyProperties(reference("usable")));
  }

  @Test
  void testNormalizesMalformedSuccessfulResponse() {
    AzureKeyVaultKmsClient malformedClient =
        createClient(
            new RecordingHttpClient(request -> response(request, 200, "{\"key\":null}")), CLOCK);

    Assertions.assertThrows(
        ConnectionFailedException.class,
        () -> malformedClient.getKeyProperties(reference("usable")));
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "usable",
        "http://example.vault.azure.net/keys/usable",
        "https://other.vault.azure.net/keys/usable",
        "https://example.vault.azure.net/secrets/usable",
        "https://example.vault.azure.net/keys",
        "https://example.vault.azure.net/keys/usable/",
        "https://example.vault.azure.net/keys/usable/version/extra",
        "https://example.vault.azure.net/keys/usable?version=latest",
        "https://example.vault.azure.net/keys/usable#fragment",
        "https://user@example.vault.azure.net/keys/usable",
        "https://example.vault.azure.net:443/keys/usable",
        "https://example.vault.azure.net/keys/encoded%2Fname",
        "https://example.vault.azure.net/keys/not_valid"
      })
  void testRejectsMalformedAndCrossVaultKeyIds(String keyId) {
    int requestCount = httpClient.requestCount();

    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> client.getKeyProperties(new KmsReference(KmsApi.AZURE_KEY_VAULT, SOURCE, keyId)));
    Assertions.assertEquals(requestCount, httpClient.requestCount());
  }

  @Test
  void testVaultOriginComparisonIsCaseInsensitive() {
    KmsReference reference =
        new KmsReference(
            KmsApi.AZURE_KEY_VAULT, SOURCE, "HTTPS://EXAMPLE.VAULT.AZURE.NET/keys/usable");

    Assertions.assertTrue(client.getKeyProperties(reference).present());
  }

  @Test
  void testRejectsInvalidClientConstruction() {
    KeyClient sdkClient = createSdkClient(new RecordingHttpClient(this::defaultResponse));

    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> new AzureKeyVaultKmsClient(" ", URI.create(VAULT_URL), sdkClient, CLOCK));
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () ->
            new AzureKeyVaultKmsClient(
                SOURCE, URI.create("http://example.vault.azure.net"), sdkClient, CLOCK));
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () ->
            new AzureKeyVaultKmsClient(SOURCE, URI.create(VAULT_URL + "/keys"), sdkClient, CLOCK));
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> new AzureKeyVaultKmsClient(SOURCE, URI.create(VAULT_URL), null, CLOCK));
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> new AzureKeyVaultKmsClient(SOURCE, URI.create(VAULT_URL), sdkClient, null));
  }

  private KmsKeyProperties properties(
      @Nullable Boolean enabled,
      @Nullable Instant notBefore,
      @Nullable Instant expiresOn,
      String operations) {
    RecordingHttpClient transport =
        new RecordingHttpClient(
            request ->
                response(
                    request, 200, keyJson("temporal", enabled, notBefore, expiresOn, operations)));
    return createClient(transport, CLOCK).getKeyProperties(reference("temporal"));
  }

  private HttpResponse defaultResponse(HttpRequest request) {
    if (request.getUrl().getPath().contains("/missing")) {
      return response(request, 404, errorJson());
    }
    return response(request, 200, keyJson("usable", true, null, null, "\"wrapKey\",\"unwrapKey\""));
  }

  private static AzureKeyVaultKmsClient createClient(HttpClient httpClient, Clock clock) {
    return new AzureKeyVaultKmsClient(
        SOURCE, URI.create(VAULT_URL), createSdkClient(httpClient), clock);
  }

  private static KeyClient createSdkClient(HttpClient httpClient) {
    return new KeyClientBuilder()
        .vaultUrl(VAULT_URL)
        .credential(CREDENTIAL)
        .httpClient(httpClient)
        .retryOptions(new RetryOptions(new FixedDelayOptions(0, Duration.ZERO)))
        .buildClient();
  }

  private static KmsReference reference(String name) {
    return new KmsReference(KmsApi.AZURE_KEY_VAULT, SOURCE, VAULT_URL + "/keys/" + name);
  }

  private static KmsReference reference(String name, String version) {
    return new KmsReference(
        KmsApi.AZURE_KEY_VAULT, SOURCE, VAULT_URL + "/keys/" + name + "/" + version);
  }

  private static String keyJson(
      String name,
      @Nullable Boolean enabled,
      @Nullable Instant notBefore,
      @Nullable Instant expiresOn,
      String operations) {
    String enabledValue = enabled == null ? "null" : enabled.toString();
    String notBeforeField = notBefore == null ? "" : ",\"nbf\":" + notBefore.getEpochSecond();
    String expiresOnField = expiresOn == null ? "" : ",\"exp\":" + expiresOn.getEpochSecond();
    return String.format(
        "{\"key\":{\"kid\":\"%s/keys/%s/%s\",\"kty\":\"RSA\","
            + "\"key_ops\":[%s],\"n\":\"AQAB\",\"e\":\"AQAB\"},"
            + "\"attributes\":{\"enabled\":%s,\"recoveryLevel\":\"Recoverable\"%s%s}}",
        VAULT_URL, name, VERSION, operations, enabledValue, notBeforeField, expiresOnField);
  }

  private static String errorJson() {
    return "{\"error\":{\"code\":\"KeyNotFound\",\"message\":\"not found\"}}";
  }

  private static HttpResponse response(HttpRequest request, int statusCode, String body) {
    return new StaticHttpResponse(request, statusCode, body);
  }

  private static final class RecordingHttpClient implements HttpClient {

    private final Function<HttpRequest, HttpResponse> responseFactory;
    private final List<HttpRequest> requests = new ArrayList<>();

    private RecordingHttpClient(Function<HttpRequest, HttpResponse> responseFactory) {
      this.responseFactory = responseFactory;
    }

    @Override
    public Mono<HttpResponse> send(HttpRequest request) {
      requests.add(request);
      return Mono.fromSupplier(() -> responseFactory.apply(request));
    }

    private HttpRequest lastRequest() {
      return requests.get(requests.size() - 1);
    }

    private int requestCount() {
      return requests.size();
    }
  }

  private static final class StaticHttpResponse extends HttpResponse {

    private final int statusCode;
    private final byte[] body;
    private final HttpHeaders headers;

    private StaticHttpResponse(HttpRequest request, int statusCode, String body) {
      super(request);
      this.statusCode = statusCode;
      this.body = body.getBytes(StandardCharsets.UTF_8);
      this.headers =
          new HttpHeaders()
              .set(HttpHeaderName.CONTENT_TYPE, "application/json")
              .set(HttpHeaderName.CONTENT_LENGTH, Integer.toString(this.body.length));
    }

    @Override
    public int getStatusCode() {
      return statusCode;
    }

    @SuppressWarnings("deprecation")
    @Override
    public String getHeaderValue(String name) {
      return headers.getValue(name);
    }

    @Override
    public HttpHeaders getHeaders() {
      return headers;
    }

    @Override
    public Flux<ByteBuffer> getBody() {
      return Flux.defer(() -> Flux.just(ByteBuffer.wrap(body)));
    }

    @Override
    public Mono<byte[]> getBodyAsByteArray() {
      return Mono.just(body.clone());
    }

    @Override
    public Mono<String> getBodyAsString() {
      return Mono.just(new String(body, StandardCharsets.UTF_8));
    }

    @Override
    public Mono<String> getBodyAsString(Charset charset) {
      return Mono.just(new String(body, charset));
    }
  }
}
