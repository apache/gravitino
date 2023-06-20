package com.datastrato.graviton.connector.hive;

import com.datastrato.graviton.connectors.core.MetaOperation;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.MetaStoreUtils;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore;
import org.apache.hadoop.hive.shims.ShimLoader;
import org.apache.hadoop.hive.shims.Utils;
import org.apache.hadoop.hive.thrift.HadoopThriftAuthBridge;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.thrift.TException;

import org.apache.hadoop.conf.Configuration;

import javax.security.auth.login.LoginException;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.security.PrivilegedExceptionAction;
import java.util.*;
import java.util.concurrent.TimeUnit;

import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFastFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.datanucleus.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HiveMetadata implements MetaOperation<Database>
{
  private static final Logger LOG = LoggerFactory.getLogger(HiveMetadata.class);
  private static final String HIVE_SITE = "conf/hive-site.xml";
  private static final String CORE_SITE = "conf/core-site.xml";
  private static final String METASTORE_URI = "hive.metastore.uris";
  private static final String PRINCIPAL_KEY = "hive.metastore.kerberos.principal";
  private static final long SOCKET_TIMEOUT_MS = TimeUnit.SECONDS.toMillis(600);

  protected ThriftHiveMetastore.Iface client;
  private TTransport transport;
  private URI serverURI;

  private final String confDir = "";

//  public static CachingHiveMetastoreBuilder builder()
//  {
//    return new CachingHiveMetastoreBuilder();
//  }
//
//  @Immutable
//  public static class CachingHiveMetastoreBuilder
//  {
//    private String confDir;
//
//    public CachingHiveMetastoreBuilder() {}
//
//    private CachingHiveMetastoreBuilder(String confDir)
//    {
//      this.confDir = confDir;
//    }
//
//    public CachingHiveMetastoreBuilder confDir(String confDir)
//    {
//      this.confDir = confDir;
//      return this;
//    }
//
//    public HiveMetadata build()
//    {
//      return new HiveMetadata(confDir);
//    }
//  }

  protected HiveMetadata(String confDir)
  {
    try {
      getClient(getHMServerUri(null, null));
    } catch (Exception e) {
      LOG.error(e.getMessage());
      e.printStackTrace();
    }
  }

  @Override
  public void close() {
    if ((transport != null) && transport.isOpen()) {
      LOG.debug("Closing thrift transport");
      transport.close();
    }
  }

  /**
   * Create a client to Hive Metastore.
   * If principal is specified, create kerberos client.
   *
   * @param uri server uri
   * @throws MetaException        if fails to login using kerberos credentials
   * @throws IOException          if fails connecting to metastore
   * @throws InterruptedException if interrupted during kerberos setup
   */
  private void getClient(URI uri) throws TException, IOException, InterruptedException, URISyntaxException, LoginException {
    HiveConf conf = new HiveConf();
    addResource(conf, HIVE_SITE);
    if (uri != null) {
      conf.set(METASTORE_URI, uri.toString());
    }

    // Pick up the first URI from the list of available URIs
    serverURI = uri != null ? uri : new URI(conf.get(METASTORE_URI).split(",")[0]);

    String principal = conf.get(PRINCIPAL_KEY);

    if (principal == null) {
      open(conf, serverURI);
      return;
    }

    LOG.debug("Opening kerberos connection to HMS");
    addResource(conf, CORE_SITE);

    Configuration hadoopConf = new Configuration();
    addResource(hadoopConf, HIVE_SITE);
    addResource(hadoopConf, CORE_SITE);

    // Kerberos
    UserGroupInformation.setConfiguration(hadoopConf);
    UserGroupInformation.getLoginUser().doAs(
            (PrivilegedExceptionAction<TTransport>) () -> open(conf, serverURI)
    );
  }


  @Override
  public Optional<Database> getDatabase(String databaseName) throws TException {
    return Optional.of(client.get_database(databaseName));
  }

  @Override
  public List<String> getAllDatabases(String filter) throws TException {
//    if (filter == null || filter.isEmpty()) {
      return new ArrayList<>(client.get_all_databases());
//    }
//    return client.get_all_databases()
//            .stream()
//            .filter(n -> n.matches(filter)).toList();
  }

  @Override
  public void createDatabase(Database database) throws Exception {
    try {
      client.create_database(database);
    } catch (TException e) {
      LOG.error(e.getMessage());
      e.printStackTrace();
      throw e;
    }
  }

  @Override
  public void dropDatabase(String databaseName, boolean deleteData) throws Exception {
    try {
      client.drop_database(databaseName, deleteData, true);
    } catch (TException e) {
      LOG.error(e.getMessage());
      e.printStackTrace();
      throw e;
    }
  }

  @Override
  public void renameDatabase(String databaseName, String newDatabaseName) throws Exception {
    try {
      client.alter_database(databaseName, new Database(newDatabaseName, null, null, null));
    } catch (TException e) {
      LOG.error(e.getMessage());
      e.printStackTrace();
      throw e;
    }
  }

  private TTransport open(HiveConf conf, URI uri) throws TException, IOException, LoginException {
    boolean useSasl = conf.getBoolVar(HiveConf.ConfVars.METASTORE_USE_THRIFT_SASL);
    boolean useFramedTransport = conf.getBoolVar(HiveConf.ConfVars.METASTORE_USE_THRIFT_FRAMED_TRANSPORT);
    boolean useCompactProtocol = conf.getBoolVar(HiveConf.ConfVars.METASTORE_USE_THRIFT_COMPACT_PROTOCOL);
    LOG.debug("Connecting to {}, framedTransport = {}", uri, useFramedTransport);

    transport = new TSocket(uri.getHost(), uri.getPort(), (int) SOCKET_TIMEOUT_MS);

    if (useSasl) {
      LOG.debug("Using SASL authentication");
      HadoopThriftAuthBridge.Client authBridge = ShimLoader.getHadoopThriftAuthBridge().createClient();
      // check if we should use delegation tokens to authenticate
      // the call below gets hold of the tokens if they are set up by hadoop
      // this should happen on the map/reduce tasks if the client added the
      // tokens into hadoop's credential store in the front end during job
      // submission.
      String tokenSig = conf.get("hive.metastore.token.signature");
      // tokenSig could be null
      String tokenStrForm = Utils.getTokenStrForm(tokenSig);
      if (tokenStrForm != null) {
        LOG.debug("Using delegation tokens");
        // authenticate using delegation tokens via the "DIGEST" mechanism
        transport = authBridge.createClientTransport(null, uri.getHost(), "DIGEST",
                tokenStrForm, transport, MetaStoreUtils.getMetaStoreSaslProperties(conf));
      } else {
        LOG.debug("Using principal");
        String principalConfig = conf.getVar(HiveConf.ConfVars.METASTORE_KERBEROS_PRINCIPAL);
        LOG.debug("Using principal {}", principalConfig);
        transport = authBridge.createClientTransport(principalConfig, uri.getHost(), "KERBEROS",
                null, transport, MetaStoreUtils.getMetaStoreSaslProperties(conf));
      }
    }

    transport = useFramedTransport ? new TFastFramedTransport(transport) : transport;
    TProtocol protocol = useCompactProtocol ?
            new TCompactProtocol(transport) :
            new TBinaryProtocol(transport);
    client = new ThriftHiveMetastore.Client(protocol);
    transport.open();

    if (!useSasl && conf.getBoolVar(HiveConf.ConfVars.METASTORE_EXECUTE_SET_UGI)) {
      UserGroupInformation ugi = Utils.getUGI();
      client.set_ugi(ugi.getUserName(), Arrays.asList(ugi.getGroupNames()));
      LOG.debug("Set UGI for {}", ugi.getUserName());
    }
    LOG.debug("Connected to metastore, using compact protocol = {}", useCompactProtocol);

    return transport;
  }

  public static URI getHMServerUri(String host, String portString) throws URISyntaxException {
    if (host == null) {
      host = "localhost";
    }
    host = host.trim();

    int port = 9083;
    if (portString != null) {
      port = Integer.parseInt(portString);
    }

    LOG.info("Connecting to {}:{}", host, port);

    return new URI("thrift", null, host, port, null, null, null);
  }

  private void addResource(Configuration conf, String res) throws MalformedURLException {
    if (StringUtils.isEmpty(confDir)) {
      // load the configuration from the classpath
      InputStream inStream = HiveMetadata.class.getClassLoader().getResourceAsStream(res);
      if (inStream != null) {
        LOG.debug("Adding configuration resource {}", res);
        conf.addResource(inStream);
      } else {
        LOG.debug("Configuration {} does not exist", res);
      }
    } else {
      // load the configuration from the conf directory
      File f = new File(confDir + "/" + res);
      if (f.exists() && !f.isDirectory()) {
        LOG.debug("Adding configuration resource {}", res);
        conf.addResource(f.toURI().toURL());
      } else {
        LOG.debug("Configuration {} does not exist", res);
      }
    }
  }
}
