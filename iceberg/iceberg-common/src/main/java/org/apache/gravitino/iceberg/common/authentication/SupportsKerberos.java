package org.apache.gravitino.iceberg.common.authentication;

import org.apache.gravitino.iceberg.common.authentication.kerberos.KerberosClient;

/**
 * An interface to indicate that the implementing class supports Kerberos authentication and can
 * provide a Kerberos client.
 */
public interface SupportsKerberos {

  /**
   * Get the Kerberos client.
   *
   * @return the Kerberos client
   */
  KerberosClient getKerberosClient();
}
