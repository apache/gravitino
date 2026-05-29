package org.apache.gravitino.client;

import java.security.cert.X509Certificate;
import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.SSLContext;
import org.apache.hc.client5.http.ssl.NoopHostnameVerifier;
import org.apache.hc.core5.ssl.SSLContexts;
import org.apache.hc.core5.ssl.TrustStrategy;

public class LocalTestTLSConfigurer implements TLSConfigurer {

  @Override
  public SSLContext sslContext() {
    try {
      return SSLContexts.custom()
          .loadTrustMaterial(
              null, (TrustStrategy) (X509Certificate[] chain, String authType) -> true)
          .build();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public HostnameVerifier hostnameVerifier() {
    return NoopHostnameVerifier.INSTANCE;
  }
}
