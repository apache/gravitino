# External Secret Management Integration for Apache Gravitino

---


## Background

Apache Gravitino currently supports credential vending for cloud storage systems (S3, GCS, ADLS, OSS) through various credential providers. However, sensitive credentials (access keys, secret keys, passwords, API tokens) are currently configured directly in configuration files or catalog properties as plain text, which poses significant security risks in production environments.

Modern cloud-native applications follow the principle of **never storing secrets in database as plain-text or in  configuration files** and instead rely on external secret management systems that provide:
- Centralized secret storage with encryption at rest
- Fine-grained access control and audit logging
- Automatic secret rotation
- Secret versioning and rollback capabilities
- Integration with cloud IAM systems

### Security Risks  Today

1. **Plain Text Catalog Credentials in Database:**
   When users create catalogs via UI/API, passwords are stored as plain text:
   ```properties
   # PostgreSQL Catalog
   jdbc-password = MyDatabasePassword123
   
   # MySQL Catalog  
   jdbc-password = SuperSecret456
   
   # Hive Catalog
   authentication.kerberos.keytab-uri = /path/to/keytab
   ```
   These credentials are stored in Gravitino's backend database in plain text and accessible to anyone with database access.

2. **Lack of Secret Rotation:** When catalog passwords are rotated, users must manually update the catalog, which exposes the new password again as plain text.

3. **Compliance Requirements:** Many industries (finance, healthcare, government) mandate external secret management for SOC2, HIPAA, PCI-DSS compliance.


### Industry Best Practices

Many organizations running applications in production already use an external secret management solution, such as:
- AWS Secrets Manager
- HashiCorp Vault
- Azure Key Vault
- Google Secret Manager
- Kubernetes Secrets with external secret operators

Gravitino should integrate seamlessly with these existing secret management infrastructures and provide an extensible framework to support any other vault providers.

## Current State

### Understanding CredentialProvider vs SecretProvider

It's important to clarify the relationship between two similar-sounding concepts:

| Component | Purpose | Layer | Changes Needed |
|-----------|---------|-------|----------------|
| **CredentialProvider** (existing) | Vends credentials to clients (Iceberg, Spark, etc.) for accessing cloud storage | Application layer |  None - works transparently |
| **SecretProvider** (proposed) | Fetches secrets from external systems (AWS Secrets Manager, Vault) | Infrastructure layer |  New abstraction |

**Key Point:** `SecretProvider` is a **prerequisite layer** that runs **before** `CredentialProvider`. It resolves secret references (`${secret:...}`) to actual values, then passes those resolved values to `CredentialProvider` implementations.

**Example Flow[Postgresql catalog]:**
```
User creates PostgreSQL catalog via UI with:
  jdbc-password = ${secret:prod-postgres-catalog/password}
              ↓
Gravitino stores the reference "${secret:prod-postgres-catalog/password}" in backend DB
              ↓
When catalog needs to connect to PostgreSQL:
  SecretProvider: Calls AWS Secrets Manager API → returns "mySecureP@ssw0rd"
              ↓
  Catalog/CredentialProvider: Receives resolved password "mySecureP@ssw0rd" and connects to PostgreSQL
              ↓ (no code changes needed!)
Client: Uses the PostgreSQL catalog to query tables
```

They work together at different layers, and existing `CredentialProvider` implementations require **zero code changes**.

### Catalog Credential Flow Today

```
┌─────────────────────────────────────────────────────────────┐
│  User Creates Catalog via UI/API                             │
│  ┌───────────────────────────────────────────────────────┐ │
│  │ Catalog: "prod-postgres"                             │ │
│  │ Provider: "jdbc-postgresql"                          │ │
│  │ Properties:                                          │ │
│  │   jdbc-url = jdbc:postgresql://db.example.com/prod   │ │
│  │   jdbc-user = app_user                               │ │
│  │   jdbc-password = PlainTextPassword123               │ │
│  └───────────────────────────────────────────────────────┘ │
└─────────────────────────────────────────────────────────────┘
                            │
                            ▼
┌─────────────────────────────────────────────────────────────┐
│  Gravitino Backend Database                                  │
│  - Stores catalog properties including plain text password  │
│  - Password visible in database and logs                     │
└─────────────────────────────────────────────────────────────┘
                            │
                            ▼
┌─────────────────────────────────────────────────────────────┐
│  Catalog.initialize(properties)                              │
│  - Reads password from properties Map                        │
│  - Connects to external database with plain text password   │
└─────────────────────────────────────────────────────────────┘
```

## Proposed Solution

### High-Level Architecture

Introduce a **Secret Provider** abstraction layer that resolves secret references :

```
┌──────────────────────────────────────────────────────────────────┐
│  Gravitino Server Configuration (gravitino.conf)                  │
│  ┌────────────────────────────────────────────────────────────┐  │
│  │ # Global secret provider configuration                     │  │
│  │ secret-provider = aws-secrets-manager                      │  │
│  │ secret-provider.region = us-east-1                         │  │
│  │ secret-provider.secret-path-prefix = gravitino/prod/       │  │
│  └────────────────────────────────────────────────────────────┘  │
└──────────────────────────────────────────────────────────────────┘
                            │
                            ▼
┌──────────────────────────────────────────────────────────────────┐
│  SecretProviderFactory                                            │
│  - Creates appropriate SecretProvider based on configuration      │
└──────────────────────────────────────────────────────────────────┘
                            │
                            ▼
┌──────────────────────────────────────────────────────────────────┐
│  SecretProvider Interface                                         │
│  ┌────────────────────────────────────────────────────────────┐  │
│  │ + resolveSecret(secretReference): String                   │  │
│  │ + resolveSecrets(secretReferences): Map<String, String>    │  │
│  │ + refreshSecrets(): void                                   │  │
│  └────────────────────────────────────────────────────────────┘  │
└──────────────────────────────────────────────────────────────────┘
         │                    │                    │
         ▼                    ▼                    ▼
┌──────────────┐  ┌──────────────────┐  ┌─────────────────┐
│ AWS Secrets  │  │  HashiCorp       │  │  Plain Text     │
│ Manager      │  │  Vault           │  │  (Backward      │
│ Provider     │  │  Provider        │  │  Compatibility) │
└──────────────┘  └──────────────────┘  └─────────────────┘
                            │
                            ▼
┌──────────────────────────────────────────────────────────────────┐
│  SecretCache                                                     │
│  - TTL-based caching                                             │
│  - Thread-safe access                                            │
└──────────────────────────────────────────────────────────────────┘
                            │
                            ▼
┌──────────────────────────────────────────────────────────────────┐
│  CredentialProviders                                             │
│  - Receives resolved catalog credentials transparently           │
│  - No code changes required                                      │
└──────────────────────────────────────────────────────────────────┘
```

### Hybrid Approach: Global vs Explicit Provider (Catalog Properties)

The design supports **two complementary syntax patterns for catalog properties**:

1. **Global Provider (Recommended for single-tenant):**
   - Configure one secret provider in `gravitino.conf`
   - Use simple syntax in catalog properties: `${secret:path/to/secret}`
   - Simpler to manage, easier to migrate providers

2. **Explicit Provider (Multi-tenant scenarios):**
   - Configure multiple secret providers in `gravitino.conf`
   - Use provider-specific syntax in catalog properties: `${secret:provider-type:path/to/secret}`
   - Each catalog can use different secret management systems
   - Enables team-specific or compliance-driven provider selection

**Example (Catalog Property Values):**
```properties
# Catalog property using global provider
jdbc-password = ${secret:postgres/password}

# Catalog property with explicit provider override
jdbc-password = ${secret:vault:postgres/password}
```

This hybrid approach provides **simplicity by default** while enabling **flexibility when needed**.

## Goals (V1 Scope: Catalog Properties Only)

**Primary Focus:** Secret resolution for **catalog create/update/read operations only**.

1. **Provide pluggable external secret management** through a well-defined abstraction
2. **Implement AWS Secrets Manager integration** as the first reference implementation
3. **Support secret references in catalog properties** (`jdbc-password`, `jdbc-user`, Kerberos keytabs, etc.)
4. **Maintain backward compatibility** with existing plain-text catalog properties
5. **Implement API redaction rules** to prevent secret exposure in responses
6. **Minimize performance overhead** through intelligent caching

## Non-Goals (Deferred to Future Work)

1.  **Server configuration secret interpolation** (e.g., `gravitino.conf`, `gravitino-iceberg-rest-server.conf`) — **V2 feature**
2.  Encrypting Gravitino's internal metadata store (separate concern) 

## Design Details

### Core Components

#### 1. SecretProvider Interface

```java
package org.apache.gravitino.secret;

/**
 * An interface for resolving secret references to actual secret values.
 * 
 * <p>Implementations provide integration with external secret management
 * systems like AWS Secrets Manager, HashiCorp Vault, Azure Key Vault, etc.
 * 
 * <p>Secret providers should implement caching and refresh strategies to
 * minimize external API calls while ensuring secrets are reasonably fresh.
 */
public interface SecretProvider extends Closeable {
  
  /**
   * Initializes the secret provider with configuration properties.
   *
   * @param properties Configuration properties specific to the provider
   */
  void initialize(Map<String, String> properties);
  
  /**
   * Resolves a single secret reference to its actual value.
   *
   * @param secretReference The secret reference in provider-specific format
   *                       e.g., "secret-name", "path/to/secret", "secret-name#key"
   * @return The resolved secret value, or null if not found
   * @throws SecretResolutionException if secret resolution fails
   */
  @Nullable
  String resolveSecret(String secretReference);
  
  /**
   * Resolves multiple secret references in batch for efficiency.
   *
   * @param secretReferences Collection of secret references to resolve
   * @return Map of secret references to their resolved values
   * @throws SecretResolutionException if batch resolution fails
   */
  Map<String, String> resolveSecrets(Collection<String> secretReferences);
  
  /**
   * Returns the provider type identifier.
   *
   * @return Provider type (e.g., "aws-secrets-manager", "vault")
   */
  String providerType();
  
  /**
   * Forces a refresh of cached secrets from the external system.
   * This is typically called on a schedule or when secrets are suspected stale.
   */
  void refreshSecrets();
}
```

#### 2. SecretReferenceResolver

Handles parsing and resolving secret references in configuration values:

```java
package org.apache.gravitino.secret;

/**
 * Resolves configuration properties that contain secret references.
 * 
 * <p>Supports hybrid syntax:
 * - Global provider: ${secret:reference-name} or ${secret:path/to/secret#key}
 * - Explicit provider: ${secret:provider-type:reference-name} or ${secret:provider-type:secret#key}
 * 
 * <p>Examples:
 * - ${secret:postgres-password} -> uses global provider
 * - ${secret:db-creds#password} -> uses global provider, extracts "password" key from JSON
 * - ${secret:aws-sm:prod-db/password} -> explicitly uses AWS Secrets Manager
 * - ${secret:vault:hive/keytab} -> explicitly uses HashiCorp Vault
 * - Plain text values are returned as-is for backward compatibility
 */
public class SecretReferenceResolver {
  
  private static final Pattern SECRET_REFERENCE_PATTERN = 
      Pattern.compile("\\$\\{secret:([^}]+)\\}");
  
  private final SecretProvider defaultProvider;
  private final Map<String, SecretProvider> providerMap;
  
  public SecretReferenceResolver(
      SecretProvider defaultProvider,
      Map<String, SecretProvider> additionalProviders) {
    this.defaultProvider = defaultProvider;
    this.providerMap = additionalProviders;
  }
  
  /**
   * Resolves all secret references in a properties map.
   *
   * @param properties Configuration properties that may contain secret references
   * @return New map with secret references resolved to actual values
   */
  public Map<String, String> resolveSecrets(Map<String, String> properties);
  
  /**
   * Checks if a value contains a secret reference.
   */
  public boolean isSecretReference(String value);
  
  /**
   * Parses secret reference and determines which provider to use.
   * 
   * @param secretReference The full reference (e.g., "aws-sm:db/password" or "db/password")
   * @return ParsedReference containing provider type and secret path
   */
  private ParsedReference parseSecretReference(String secretReference) {
    // Check if explicit provider is specified
    String[] parts = secretReference.split(":", 2);
    if (parts.length == 2 && isKnownProvider(parts[0])) {
      // Explicit provider: ${secret:aws-sm:path/to/secret}
      return new ParsedReference(parts[0], parts[1]);
    }
    // Global provider: ${secret:path/to/secret}
    return new ParsedReference(null, secretReference);
  }
  
  /**
   * Resolves a single secret reference to its value.
   */
  private String resolveSecretReference(ParsedReference parsed) {
    SecretProvider provider = parsed.providerType != null
        ? providerMap.get(parsed.providerType)
        : defaultProvider;
    
    if (provider == null) {
      throw new SecretResolutionException(
          "Secret provider not configured: " + parsed.providerType);
    }
    
    return provider.resolveSecret(parsed.secretPath);
  }
  
  static class ParsedReference {
    String providerType; // null means use default
    String secretPath;
    
    ParsedReference(String providerType, String secretPath) {
      this.providerType = providerType;
      this.secretPath = secretPath;
    }
  }
}
```

#### 3. SecretCache

```java
package org.apache.gravitino.secret;

/**
 * Thread-safe cache for resolved secrets with TTL-based expiration.
 * 
 * <p>Provides automatic background refresh before expiration to ensure
 * continuous availability of secrets without cache misses.
 */
public class SecretCache {
  
  private final LoadingCache<String, CachedSecret> cache;
  private final SecretProvider provider;
  private final ScheduledExecutorService refreshExecutor;
  
  public SecretCache(
      SecretProvider provider, 
      Duration ttl,
      Duration refreshBeforeExpiry,
      int maxSize);
  
  /**
   * Gets a secret from cache, loading from provider if necessary.
   */
  public String get(String secretReference);
  
  /**
   * Gets multiple secrets in batch.
   */
  public Map<String, String> getAll(Collection<String> secretReferences);
  
  /**
   * Proactively refreshes secrets approaching expiration.
   */
  private void refreshExpiringSecrets();
  
  static class CachedSecret {
    String value;
    Instant loadedAt;
    Instant expiresAt;
  }
}
```

#### 4. AWS Secrets Manager Provider (Reference Implementation)

```java
package org.apache.gravitino.secret.aws;

/**
 * SecretProvider implementation for AWS Secrets Manager.
 * 
 * <p>Configuration properties:
 * - secret-provider.region: AWS region (required)
 * - secret-provider.secret-path-prefix: Prefix for all secret names
 * - secret-provider.endpoint: Custom endpoint for testing/LocalStack
 * - secret-provider.cache-ttl-seconds: Cache TTL (default: 300)
 * - secret-provider.cache-max-size: Max cached secrets (default: 100)
 * 
 * <p>Authentication via standard AWS credential provider chain:
 * 1. Environment variables (AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY)
 * 2. System properties
 * 3. Web identity token (EKS IRSA)
 * 4. EC2 instance profile
 * 5. ECS task role
 */
public class AwsSecretsManagerProvider implements SecretProvider {
  
  private SecretsManagerClient secretsManagerClient;
  private SecretCache cache;
  private String secretPathPrefix;
  
  @Override
  public void initialize(Map<String, String> properties) {
    String region = properties.get("secret-provider.region");
    String endpoint = properties.get("secret-provider.endpoint");
    this.secretPathPrefix = properties.getOrDefault(
        "secret-provider.secret-path-prefix", "");
    
    SecretsManagerClientBuilder builder = SecretsManagerClient.builder()
        .region(Region.of(region));
    
    if (endpoint != null) {
      builder.endpointOverride(URI.create(endpoint));
    }
    
    this.secretsManagerClient = builder.build();
    
    int ttlSeconds = Integer.parseInt(
        properties.getOrDefault("secret-provider.cache-ttl-seconds", "300"));
    int maxSize = Integer.parseInt(
        properties.getOrDefault("secret-provider.cache-max-size", "100"));
    
    this.cache = new SecretCache(
        this::loadSecret, 
        Duration.ofSeconds(ttlSeconds),
        Duration.ofSeconds(ttlSeconds / 5), // Refresh at 80% of TTL
        maxSize);
  }
  
  @Override
  public String resolveSecret(String secretReference) {
    return cache.get(buildSecretName(secretReference));
  }
  
  @Override
  public Map<String, String> resolveSecrets(
      Collection<String> secretReferences) {
    return cache.getAll(
        secretReferences.stream()
            .map(this::buildSecretName)
            .collect(Collectors.toList()));
  }
  
  private String loadSecret(String secretName) {
    try {
      GetSecretValueRequest request = GetSecretValueRequest.builder()
          .secretId(secretName)
          .build();
      
      GetSecretValueResponse response = 
          secretsManagerClient.getSecretValue(request);
      
      // Handle both string and JSON secrets
      if (response.secretString() != null) {
        return parseSecretValue(secretReference, response.secretString());
      } else {
        throw new SecretResolutionException(
            "Binary secrets not supported: " + secretName);
      }
    } catch (ResourceNotFoundException e) {
      LOG.warn("Secret not found in AWS Secrets Manager: {}", secretName);
      return null;
    } catch (SecretsManagerException e) {
      throw new SecretResolutionException(
          "Failed to retrieve secret: " + secretName, e);
    }
  }
  
  /**
   * Parses secret value, supporting both plain text and JSON with key selection.
   * 
   * Examples:
   * - "s3-credentials" -> returns entire secret string
   * - "s3-credentials#access-key-id" -> parses JSON and returns specific key
   */
  private String parseSecretValue(String secretReference, String secretString) {
    if (secretReference.contains("#")) {
      String[] parts = secretReference.split("#", 2);
      String key = parts[1];
      try {
        JsonNode jsonNode = objectMapper.readTree(secretString);
        return jsonNode.get(key).asText();
      } catch (JsonProcessingException e) {
        throw new SecretResolutionException(
            "Failed to parse JSON secret: " + secretReference, e);
      }
    }
    return secretString;
  }
  
  private String buildSecretName(String secretReference) {
    String ref = secretReference.split("#")[0]; // Remove key suffix if present
    return secretPathPrefix.isEmpty() ? ref : secretPathPrefix + ref;
  }
  
  @Override
  public String providerType() {
    return "aws-secrets-manager";
  }
  
  @Override
  public void refreshSecrets() {
    cache.invalidateAll();
  }
  
  @Override
  public void close() throws IOException {
    if (secretsManagerClient != null) {
      secretsManagerClient.close();
    }
    cache.close();
  }
}
```

#### 5. SecretProviderFactory

```java
package org.apache.gravitino.secret;

/**
 * Factory for creating SecretProvider instances.
 * 
 * <p>Supports hybrid approach:
 * - Creates default/global provider from "secret-provider" config
 * - Creates additional providers from "secret-provider.{type}.enabled" configs
 * 
 * Uses ServiceLoader pattern for discovery, similar to CredentialProvider.
 */
public class SecretProviderFactory {
  
  private static final Map<String, Class<? extends SecretProvider>> PROVIDERS = 
      new ConcurrentHashMap<>();
  
  static {
    // Discover providers via ServiceLoader
    ServiceLoader<SecretProvider> loader = 
        ServiceLoader.load(SecretProvider.class);
    for (SecretProvider provider : loader) {
      PROVIDERS.put(provider.providerType(), provider.getClass());
    }
  }
  
  /**
   * Creates default secret provider and additional providers for multi-tenant.
   *
   * @param properties Configuration properties
   * @return SecretProviderManager containing default and additional providers
   */
  public static SecretProviderManager createSecretProviders(
      Map<String, String> properties) {
    
    // Create default provider
    String defaultProviderType = properties.get("secret-provider");
    SecretProvider defaultProvider = createProvider(defaultProviderType, properties);
    
    // Create additional providers for explicit syntax
    Map<String, SecretProvider> additionalProviders = new HashMap<>();
    for (String providerType : PROVIDERS.keySet()) {
      String enabledKey = "secret-provider." + providerType + ".enabled";
      if ("true".equals(properties.get(enabledKey))) {
        SecretProvider provider = createProvider(providerType, 
            filterPropertiesForProvider(properties, providerType));
        additionalProviders.put(providerType, provider);
      }
    }
    
    return new SecretProviderManager(defaultProvider, additionalProviders);
  }
  
  /**
   * Creates a single secret provider instance.
   */
  private static SecretProvider createProvider(
      String providerType,
      Map<String, String> properties) {
    
    // Backward compatibility: no provider = plain text
    if (providerType == null || "none".equals(providerType)) {
      return new PlainTextSecretProvider();
    }
    
    Class<? extends SecretProvider> providerClass = PROVIDERS.get(providerType);
    if (providerClass == null) {
      throw new IllegalArgumentException(
          "Unknown secret provider type: " + providerType);
    }
    
    try {
      SecretProvider provider = providerClass.getDeclaredConstructor()
          .newInstance();
      provider.initialize(properties);
      return provider;
    } catch (Exception e) {
      throw new RuntimeException(
          "Failed to create secret provider: " + providerType, e);
    }
  }
  
  /**
   * Filters properties for a specific provider.
   * E.g., "secret-provider.vault.address" -> "address"
   */
  private static Map<String, String> filterPropertiesForProvider(
      Map<String, String> properties, 
      String providerType) {
    String prefix = "secret-provider." + providerType + ".";
    return properties.entrySet().stream()
        .filter(e -> e.getKey().startsWith(prefix))
        .collect(Collectors.toMap(
            e -> e.getKey().substring(prefix.length()),
            Map.Entry::getValue));
  }
}

/**
 * Manages multiple secret providers for hybrid approach.
 */
class SecretProviderManager {
  private final SecretProvider defaultProvider;
  private final Map<String, SecretProvider> additionalProviders;
  
  SecretProviderManager(
      SecretProvider defaultProvider,
      Map<String, SecretProvider> additionalProviders) {
    this.defaultProvider = defaultProvider;
    this.additionalProviders = additionalProviders;
  }
  
  public SecretProvider getDefaultProvider() {
    return defaultProvider;
  }
  
  public SecretProvider getProvider(String providerType) {
    return additionalProviders.get(providerType);
  }
  
  public Map<String, SecretProvider> getAllProviders() {
    return Collections.unmodifiableMap(additionalProviders);
  }
}
```

### Secret Reference Syntax

Support flexible secret reference formats with **hybrid approach** - global provider (simpler) or explicit provider (multi-tenant):

#### Global Provider Syntax (Recommended)

Uses the globally configured `secret-provider` from configuration:

1. **Simple reference:** `${secret:secret-name}`
   - Uses global provider
   - Resolves to entire secret value
   - Example: `${secret:postgres-password}` → `mySecurePassword`

2. **JSON key reference:** `${secret:secret-name#key}`
   - Uses global provider
   - Parses JSON secret and extracts specific key
   - Example: `${secret:db-creds#password}` → extracts `password` from JSON

3. **Path-based reference:** `${secret:path/to/secret}`
   - Uses global provider
   - Supports hierarchical secret organization
   - Example: `${secret:gravitino/prod/postgres-catalog/password}`

#### Explicit Provider Syntax (Multi-Tenant)

Allows overriding the global provider for specific secrets:

4. **Provider-specific reference:** `${secret:provider-type:secret-name}`
   - Explicitly specifies which provider to use
   - Example: `${secret:aws-sm:prod-postgres/password}` → uses AWS Secrets Manager
   - Example: `${secret:vault:hive/keytab}` → uses HashiCorp Vault

5. **Provider-specific with JSON key:** `${secret:provider-type:secret-name#key}`
   - Combines explicit provider with JSON key extraction
   - Example: `${secret:aws-sm:db-creds#password}`

#### Backward Compatibility

6. **Plain text:** `actual-value`
   - If no `${secret:...}` syntax, treated as literal value
   - Example: `MyPlainPassword` → `MyPlainPassword`

#### Provider Type Identifiers

- `aws-sm` - AWS Secrets Manager
- `vault` - HashiCorp Vault
- `azure-kv` - Azure Key Vault
- `gcp-sm` - Google Secret Manager
- `k8s` - Kubernetes Secrets

### Service Discovery

Use Java ServiceLoader for provider discovery:

```
META-INF/services/org.apache.gravitino.secret.SecretProvider
```

Content:
```
org.apache.gravitino.secret.aws.AwsSecretsManagerProvider
org.apache.gravitino.secret.vault.VaultSecretProvider (future)
org.apache.gravitino.secret.azure.AzureKeyVaultProvider (future)
```

## Configuration Examples

### Example 1: Global Provider - PostgreSQL Catalog

**Step 1: Store secrets in AWS Secrets Manager**
```bash
# Create secret for PostgreSQL catalog credentials
aws secretsmanager create-secret \
    --name gravitino/prod/postgres-catalog \
    --secret-string '{
      "jdbc-user": "app_user",
      "jdbc-password": "SuperSecurePassword123"
    }' \
    --region us-east-1
```

**Step 2: Configure Gravitino with global secret provider**
```properties
# gravitino.conf

# Enable AWS Secrets Manager as global provider
secret-provider = aws-secrets-manager
secret-provider.region = us-east-1
secret-provider.secret-path-prefix = gravitino/prod/
secret-provider.cache-ttl-seconds = 300
```

**Step 3: Create catalog via UI/API with secret references**
```json
// POST /api/metalakes/{metalake}/catalogs
{
  "name": "prod-postgres",
  "type": "RELATIONAL",
  "provider": "jdbc-postgresql",
  "properties": {
    "jdbc-url": "jdbc:postgresql://db.example.com:5432/production",
    "jdbc-user": "${secret:postgres-catalog#jdbc-user}",
    "jdbc-password": "${secret:postgres-catalog#jdbc-password}",
    "jdbc-driver": "org.postgresql.Driver"
  }
}
```

**Result:** Gravitino stores the references `${secret:...}` in its backend DB and resolves them at runtime using AWS Secrets Manager.

### Example 2: Explicit Provider Syntax - Multi-Tenant Scenario

For organizations where different teams use different secret management systems:

**Step 1: Configure multiple secret providers**
```properties
# gravitino.conf

# Global provider (default)
secret-provider = aws-secrets-manager
secret-provider.region = us-east-1
secret-provider.secret-path-prefix = gravitino/prod/

# Additional providers for multi-tenant
secret-provider.vault.enabled = true
secret-provider.vault.address = https://vault.example.com
secret-provider.vault.namespace = gravitino

secret-provider.k8s.enabled = true
secret-provider.k8s.namespace = gravitino-secrets
```

**Step 2: Create catalogs with explicit provider references**

**Team A - Uses AWS Secrets Manager (global default):**
```json
// PostgreSQL catalog using global provider
{
  "name": "team-a-postgres",
  "type": "RELATIONAL",
  "provider": "jdbc-postgresql",
  "properties": {
    "jdbc-url": "jdbc:postgresql://team-a-db.example.com/prod",
    "jdbc-user": "team_a_user",
    "jdbc-password": "${secret:team-a/postgres#password}"
  }
}
```

**Team B - Uses HashiCorp Vault (explicit):**
```json
// MySQL catalog explicitly using Vault
{
  "name": "team-b-mysql",
  "type": "RELATIONAL",
  "provider": "jdbc-mysql",
  "properties": {
    "jdbc-url": "jdbc:mysql://team-b-db.example.com:3306/prod",
    "jdbc-user": "team_b_user",
    "jdbc-password": "${secret:vault:team-b/mysql/password}"
  }
}
```

**Team C - Uses Kubernetes Secrets (explicit):**
```json
// Hive catalog using K8s secrets
{
  "name": "team-c-hive",
  "type": "RELATIONAL", 
  "provider": "hive",
  "properties": {
    "metastore.uris": "thrift://hive-metastore:9083",
    "authentication.type": "KERBEROS",
    "authentication.kerberos.principal": "${secret:k8s:team-c-hive-principal}",
    "authentication.kerberos.keytab-uri": "${secret:k8s:team-c-hive-keytab}"
  }
}
```

**Result:** Each team can use their preferred secret management system while sharing the same Gravitino instance.

### Example 3: Backward Compatible (No Secret Provider)

```properties
# No secret provider configured - plain text works as before
credential-providers = s3-secret-key
s3-access-key-id = AKIAIOSFODNN7EXAMPLE
s3-secret-access-key = wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY
```

### Example 4: LocalStack for Testing

```properties
# gravitino.conf - Use LocalStack for local development/testing
secret-provider = aws-secrets-manager
secret-provider.region = us-east-1
secret-provider.endpoint = http://localhost:4566
secret-provider.secret-path-prefix = gravitino/dev/
```

Then create catalogs with secret references:
```json
{
  "name": "dev-postgres",
  "type": "RELATIONAL",
  "provider": "jdbc-postgresql",
  "properties": {
    "jdbc-url": "jdbc:postgresql://localhost:5432/dev",
    "jdbc-user": "dev_user",
    "jdbc-password": "${secret:dev-postgres#password}"
  }
}
```

## Testing Strategy

### Unit Tests

1. **SecretReferenceResolver Tests**
   - Parse various secret reference formats
   - Handle malformed references
   - Backward compatibility with plain text

2. **SecretCache Tests**
   - TTL expiration
   - Background refresh
   - Thread safety under concurrent load
   - Cache eviction policies

3. **AwsSecretsManagerProvider Tests**
   - Mock AWS SDK calls
   - Test error handling (secret not found, throttling, network errors)
   - JSON parsing for key extraction
   - Prefix handling

### Integration Tests

1. **LocalStack Integration**
   ```java
   @Test
   @Tag("gravitino-docker-test")
   public void testAwsSecretsManagerWithLocalStack() {
     // Start LocalStack container with Secrets Manager
     // Create secrets via AWS SDK
     // Configure Gravitino to use LocalStack endpoint
     // Verify secret resolution
   }
   ```

2. **End-to-End Credential Vending**
   - Create catalog with secret references
   - Verify credentials generated correctly
   - Test credential refresh on rotation

3. **Performance Tests**
   - Measure secret resolution latency
   - Test cache hit rates
   - Concurrent secret access

### Security Tests

1. **Secret Masking in Logs**
   - Verify secrets never logged
   - Check error messages don't leak secrets

2. **Permission Tests**
   - Verify IAM policy enforcement
   - Test access denied scenarios

## Alternatives Considered

### Alternative 1: Environment Variable Substitution

**Approach:** Support `${env:VAR_NAME}` syntax in configuration.

**Pros:**
- Simple implementation
- Wide platform support
- No external dependencies

**Cons:**
- Environment variables visible in process listings (`ps aux`)
- No secret rotation without process restart
- No centralized audit logging

### Alternative 2: App-managed key encryption

**Approach:** Use static key supplied through application configs and encrypt sensitive data in gravitino backend store itself.

**Pros:**
- Full control over implementation
- Easy to implement 
**Cons:**
- Less secure and not possible to rotate the encryption keys if compromised 

## Future Work

### Phase 2: Server Configuration Secret Interpolation (V2)

**Extend secret resolution beyond catalog properties to server configuration:**

#### Motivation: Why External Secret Management for Server Config?

**Security Standards Perspective:**

Modern security best practices and compliance standards (SOC2, HIPAA, PCI-DSS, NIST 800-53) recommend **never storing secrets in configuration files**. Two common approaches exist:

| Approach | Security Profile | Use Case |
|----------|------------------|----------|
| **External Secret Manager (Recommended)** |  High - Secrets never in config files<br> Runtime rotation without redeployment<br> Centralized audit logging<br> Encrypted at rest in secret manager<br>⚠️ Requires external dependency | Production deployments, enterprises with compliance requirements |
| **Deploy-time Injection (Alternative)** | ⚠️ Medium - Secrets in Kubernetes ConfigMaps/Secrets<br> Base64 encoded, not encrypted by default<br> Visible via kubectl<br> Requires redeployment for rotation<br> No runtime dependencies | Development, air-gapped environments |

**Recommendation:** Use external secret management (runtime resolution) for production deployments. This aligns with how AWS EKS, Google GKE, and Azure AKS secure production workloads.

#### Scope

1. **Gravitino Server Config (`gravitino.conf`)**
   - Support secret references in main server configuration
   - Use cases: database credentials for metadata store, internal service credentials
   
2. **Iceberg REST Server Config**
   - Support secret references for credential provider configuration
   ```properties
   # gravitino-iceberg-rest-server.conf
   gravitino.iceberg-rest.credential-providers = s3-token
   gravitino.iceberg-rest.s3-access-key-id = ${secret:s3-creds#access-key}
   gravitino.iceberg-rest.s3-secret-access-key = ${secret:s3-creds#secret-key}
   gravitino.iceberg-rest.s3-role-arn = ${secret:s3-creds#role-arn}
   ```

3. **Auxiliary Service Configurations**
   - Support for any future auxiliary services
   - Consistent secret reference syntax across all config files

#### Solving the Bootstrap Problem

**Challenge:** How does Gravitino authenticate to the secret manager if the authentication credentials are also secrets?

**Solution: Two-Phase Initialization with Clear Separation**

```properties
# gravitino.conf

# ================================================================
# PHASE 1: Bootstrap Configuration (MUST be plain text or K8s env)
# Secret provider configuration itself CANNOT use secret references
# ================================================================

# Secret provider type and connection details
secret-provider = aws-secrets-manager
secret-provider.region = us-east-1
secret-provider.cache-ttl-seconds = 300

# Authentication: Use IAM Roles (IRSA/Workload Identity) - NO credentials needed
# OR fall back to AWS SDK default credential chain (env vars from K8s Secrets)

# ================================================================
# PHASE 2: Application Configuration (Secret references allowed)
# Resolved AFTER SecretProvider is initialized
# ================================================================

# Metadata store credentials
gravitino.entity.store.relational.jdbcUrl = jdbc:postgresql://db:5432/gravitino
gravitino.entity.store.relational.jdbcUser = ${secret:gravitino-metastore/username}
gravitino.entity.store.relational.jdbcPassword = ${secret:gravitino-metastore/password}

# Iceberg REST S3 credentials
gravitino.iceberg-rest.s3-access-key-id = ${secret:iceberg-s3/access-key}
gravitino.iceberg-rest.s3-secret-access-key = ${secret:iceberg-s3/secret-key}
gravitino.iceberg-rest.s3-role-arn = ${secret:iceberg-s3/role-arn}

# OAuth authentication
gravitino.authenticator.oauth.defaultSignKey = ${secret:gravitino-oauth/signing-key}
gravitino.authenticator.oauth.clientId = ${secret:oauth-provider/client-id}

# Kerberos keytab (for Hive/HDFS)
gravitino.authenticator.kerberos.keytab-uri = ${secret:kerberos/keytab-base64}
```

**Bootstrap Authentication Options:**

**Option 1: Cloud IAM Roles (Recommended for Production)**

AWS EKS with IRSA:
```yaml
# Kubernetes ServiceAccount with IAM role annotation
apiVersion: v1
kind: ServiceAccount
metadata:
  name: gravitino
  annotations:
    eks.amazonaws.com/role-arn: arn:aws:iam::123456789012:role/GravitinoSecretAccess
```

Google GKE with Workload Identity:
```yaml
apiVersion: v1
kind: ServiceAccount
metadata:
  name: gravitino
  annotations:
    iam.gke.io/gcp-service-account: gravitino@project-id.iam.gserviceaccount.com
```

Azure AKS with Managed Identity:
```yaml
apiVersion: v1
kind: ServiceAccount
metadata:
  name: gravitino
  annotations:
    azure.workload.identity/client-id: <client-id>
```

**Option 2: Environment Variables from Kubernetes Secrets (Fallback)**

```yaml
# Kubernetes Secret for secret manager authentication
apiVersion: v1
kind: Secret
metadata:
  name: gravitino-secret-provider-auth
type: Opaque
data:
  aws-access-key-id: <base64>
  aws-secret-access-key: <base64>

---
# Pod spec references the secret
spec:
  containers:
  - name: gravitino
    env:
    - name: AWS_ACCESS_KEY_ID
      valueFrom:
        secretKeyRef:
          name: gravitino-secret-provider-auth
          key: aws-access-key-id
    - name: AWS_SECRET_ACCESS_KEY
      valueFrom:
        secretKeyRef:
          name: gravitino-secret-provider-auth
          key: aws-secret-access-key
```

Gravitino uses AWS SDK default credential chain, which automatically picks up env vars.

#### Configuration Loading Order

```
1. Load gravitino.conf from disk
2. Parse secret-provider.* properties (Phase 1)
3. Initialize SecretProvider with Phase 1 config
4. Authenticate to secret manager using:
   - Cloud IAM role (IRSA/Workload Identity) OR
   - Environment variables from K8s Secrets OR
   - AWS SDK default credential chain
5. Parse remaining properties (Phase 2)
6. Resolve all ${secret:...} references using SecretProvider
7. Start Gravitino with fully resolved configuration
```

#### Security Benefits

Compared to deploy-time injection (Helm values.yaml → ConfigMaps):

| Security Aspect | External Secret Manager | Deploy-time Injection |
|-----------------|------------------------|----------------------|
| **Secrets in config files** |  Never |  Yes (in ConfigMaps) |
| **Secrets in git history** |  Never (only references) | ⚠️ Risk if values.yaml committed |
| **Secrets visible in K8s** |  No (only in secret manager) |  Yes via `kubectl get configmap -o yaml` |
| **Encryption at rest** |  AWS KMS / Vault transit | ⚠️ Only if K8s etcd encryption enabled |
| **Secret rotation** |  Without redeployment |  Requires pod restart |
| **Audit logging** |  Centralized (who/when/what) | ⚠️ Limited (K8s audit logs only) |
| **Compliance** |  SOC2/HIPAA/PCI-DSS compliant | ⚠️ May not meet requirements |
| **Defense in depth** |  Config file leak doesn't expose secrets |  Config/ConfigMap leak exposes secrets |

#### Example: Full Server Configuration with Secrets

```properties
# gravitino.conf - Production Configuration

# ================================================================
# PHASE 1: Secret Provider Bootstrap (Plain Text)
# ================================================================
secret-provider = aws-secrets-manager
secret-provider.region = us-east-1
secret-provider.cache-ttl-seconds = 300
secret-provider.cache-max-size = 200
secret-provider.secret-path-prefix = gravitino/prod/

# ================================================================
# PHASE 2: Application Configuration (Secret References)
# ================================================================

# Gravitino metadata store
gravitino.entity.store = relational
gravitino.entity.store.relational = JDBCBackend
gravitino.entity.store.relational.jdbcUrl = jdbc:postgresql://prod-db.example.com:5432/gravitino
gravitino.entity.store.relational.jdbcDriver = org.postgresql.Driver
gravitino.entity.store.relational.jdbcUser = ${secret:metastore-db#username}
gravitino.entity.store.relational.jdbcPassword = ${secret:metastore-db#password}

# Iceberg REST service
gravitino.auxService.names = iceberg-rest
gravitino.iceberg-rest.catalog-backend = jdbc
gravitino.iceberg-rest.uri = jdbc:postgresql://iceberg-metastore.example.com:5432/iceberg
gravitino.iceberg-rest.jdbc-user = ${secret:iceberg-metastore-db#username}
gravitino.iceberg-rest.jdbc-password = ${secret:iceberg-metastore-db#password}
gravitino.iceberg-rest.warehouse = s3://prod-datalake/warehouse/

# S3 credential vending (Gravitino's own S3 credentials)
gravitino.iceberg-rest.credential-providers = s3-token
gravitino.iceberg-rest.s3-access-key-id = ${secret:iceberg-s3-admin#access-key-id}
gravitino.iceberg-rest.s3-secret-access-key = ${secret:iceberg-s3-admin#secret-access-key}
gravitino.iceberg-rest.s3-role-arn = ${secret:iceberg-s3-admin#role-arn}
gravitino.iceberg-rest.s3-region = us-east-1

# OAuth authentication
gravitino.authenticators = oauth
gravitino.authenticator.oauth.serverUri = https://auth.example.com
gravitino.authenticator.oauth.defaultSignKey = ${secret:oauth-config#signing-key}
gravitino.authenticator.oauth.serviceAudience = ${secret:oauth-config#audience}
```

**AWS Secrets Manager Secret Structure:**

```json
// Secret: gravitino/prod/metastore-db
{
  "username": "gravitino_app",
  "password": "SuperSecurePassword123!"
}

// Secret: gravitino/prod/iceberg-metastore-db
{
  "username": "iceberg_admin",
  "password": "AnotherSecurePassword456!"
}

// Secret: gravitino/prod/iceberg-s3-admin
{
  "access-key-id": "AKIAIOSFODNN7EXAMPLE",
  "secret-access-key": "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
  "role-arn": "arn:aws:iam::123456789012:role/IcebergDataAccess"
}

// Secret: gravitino/prod/oauth-config
{
  "signing-key": "base64-encoded-jwt-signing-key",
  "audience": "gravitino-production"
}
```

#### Design Considerations

1. **Clear Configuration Boundaries:**
   - Secret provider configuration: MUST be plain text or K8s env vars
   - Everything else: CAN use secret references
   - Document which properties can use `${secret:...}` syntax

2. **Error Handling:**
   - If SecretProvider initialization fails → Gravitino MUST fail fast with clear error
   - If secret resolution fails during Phase 2 → Log which secret failed, fail startup
   - Provide validation CLI tool: `gravitino-server.sh validate-config`

3. **Configuration Validation:**
   - Detect circular dependencies (secret provider config referencing secrets)
   - Warn if plain-text secrets detected in config (suggest migration)
   - Validate secret reference syntax at startup

4. **Backward Compatibility:**
   - If `secret-provider` not configured → All values treated as plain text
   - Existing plain-text configs continue to work
   - Migration path: Add secret provider → Replace values with references → Test

5. **Testing:**
   - Unit tests with LocalStack for AWS Secrets Manager
   - Integration tests for bootstrap scenarios
   - Security tests: Verify secrets never logged

#### Alternative: Deploy-time Injection

For users who cannot use external secret managers (air-gapped, development):

```yaml
# values.yaml - Helm chart
icebergRest:
  s3:
    # Reference Kubernetes Secret
    accessKeyId: "{{ .Values.secrets.s3AccessKey }}"
    secretAccessKey: "{{ .Values.secrets.s3SecretKey }}"

# Kubernetes Secret (created separately)
---
apiVersion: v1
kind: Secret
metadata:
  name: gravitino-secrets
type: Opaque
data:
  s3AccessKey: <base64>
  s3SecretKey: <base64>
```

**Trade-offs:**
-  Simpler, no runtime dependencies
-  Secrets in ConfigMaps (visible via kubectl)
-  Rotation requires redeployment



### Phase 3: Additional Secret Providers

1. **HashiCorp Vault Provider**
   - Support KV v1 and v2
   - AppRole and Kubernetes authentication
   - Dynamic secret generation
   
2. **Azure Key Vault Provider**
   - Managed Identity authentication
   - Secret versioning

3. **Google Secret Manager Provider**
   - Workload Identity authentication
   - IAM integration

4. **Kubernetes Secrets Provider**
   - Direct integration with k8s secret API
   - Support for External Secrets Operator



### Phase 4: Advanced Features

1. **Secret Versioning**
   - Support pinning to specific secret versions
   - Gradual rollout of rotated secrets

2. **Multi-Region Secret Replication**
   - Automatic failover to replica regions
   - Cross-region secret synchronization

3. **Metrics and Monitoring**
   ```
   gravitino.secret.resolution.time.seconds
   gravitino.secret.cache.hit.ratio
   gravitino.secret.refresh.failures
   ```

## Backward Compatibility

**100% backward compatible:**

1. **No secret provider configured:** Plain text configuration works as before
2. **Existing catalogs:** No changes required unless migrating to secret provider
3. **API compatibility:** No changes to public APIs
4. **Configuration format:** Existing configs remain valid

**Migration is opt-in** at operator discretion.

## Open Questions (V1 Scope)

1. **Secret rotation detection strategy (V1)**
   - **Option A:** Rely purely on cache TTL for eventual consistency
     - Simpler implementation, no external dependencies
     - Secret changes visible within cache TTL window (e.g., 5 minutes)
   - **Option B:** Proactive polling with change detection
     - Check secret version/timestamp on scheduled interval
     - Only refresh if changed, reducing API calls

2. **Catalog initialization failure behavior (V1)**
   - If secret provider fails during catalog initialization, should Gravitino:
     - **Fail fast** (recommended): Catalog initialization fails, user gets clear error
     - **Defer resolution**: Initialize catalog but fail on first actual connection attempt

3. **Performance target for V1 (catalog operations only)**
   - Acceptable latency for secret resolution during catalog initialization:
     - Cached: <50ms (in-memory lookup)
     - Cache miss: <250ms (external API call to AWS Secrets Manager)
   - Should we fail catalog creation if secret resolution exceeds timeout?
   - **Recommendation:** 5-second timeout for catalog initialization secret resolution

4. **API response redaction rules (V1)**
   - When reading catalog properties via API, how should secret references be returned?
     - **Option A:** Return the reference as-is: `"jdbc-password": "${secret:db#password}"`
     - **Option B:** Redact completely: `"jdbc-password": "***"`
     - **Option C:** Show masked reference: `"jdbc-password": "${secret:db#***}"`
   - **Recommendation:** Option A (return reference as-is) for transparency and debuggability

5. **Backward compatibility guarantee**
   - Should we guarantee that catalogs created with plain-text passwords continue to work indefinitely?
   - Or set a deprecation timeline for plain-text catalog credentials?
   - **Recommendation:** Keep backward compatibility in V1, add deprecation warning in V2, require migration in V3 (2+ years)

## Summary

## V1 Scope: Catalog Properties Only

**This design is intentionally scoped to catalog properties for the initial implementation.**

### V1 Scope (Catalog Properties Only)
 **SecretProvider abstraction** with AWS Secrets Manager implementation  
 **Catalog property secret resolution** during create/update/read operations  
 **Caching layer** with configurable TTL for performance  
 **API redaction rules** to prevent secret exposure  
 **Backward compatibility** with plain-text catalog credentials  
 **Hybrid syntax** supporting both global and explicit provider references  
 **Comprehensive testing** with LocalStack integration  

### V2 and Beyond
 **Server configuration secret interpolation** (`gravitino.conf`, REST server configs)  
 **Additional providers** (K8s Secrets, Vault, Azure Key Vault, Google Secret Manager)  


**Key Takeaway:** V1 validates the abstraction with a focused scope (catalog properties only), enabling faster review and iteration. Server config support comes in V2 once the design is proven.

---

## References

1. [AWS Secrets Manager Documentation](https://docs.aws.amazon.com/secretsmanager/)
2. [Gravitino Credential Vending Documentation](docs/security/credential-vending.md)

