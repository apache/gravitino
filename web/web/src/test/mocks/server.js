import { setupServer } from 'msw/node'
import { http, HttpResponse } from 'msw'

const handlers = [
  // Mock auth config endpoint (matches the actual API endpoint used by factory)
  http.get('/configs', () => {
    return HttpResponse.json({
      'gravitino.authenticator': 'oauth',
      'gravitino.authenticator.oauth.provider': 'oidc',
      'gravitino.authenticator.oauth.authority': 'https://test-oidc.example.com',
      'gravitino.authenticator.oauth.clientId': 'test-client-id',
      'gravitino.authenticator.oauth.scope': 'openid profile email'
    })
  }),

  // Mock OIDC discovery endpoint
  http.get('https://test-oidc.example.com/.well-known/openid-configuration', () => {
    return HttpResponse.json({
      issuer: 'https://test-oidc.example.com',
      authorization_endpoint: 'https://test-oidc.example.com/auth',
      token_endpoint: 'https://test-oidc.example.com/token',
      userinfo_endpoint: 'https://test-oidc.example.com/userinfo',
      jwks_uri: 'https://test-oidc.example.com/.well-known/jwks',
      scopes_supported: ['openid', 'profile', 'email'],
      response_types_supported: ['code'],
      grant_types_supported: ['authorization_code'],
      subject_types_supported: ['public']
    })
  }),

  // Mock JWKS endpoint
  http.get('https://test-oidc.example.com/.well-known/jwks', () => {
    return HttpResponse.json({
      keys: [
        {
          kty: 'RSA',
          use: 'sig',
          kid: 'test-key-id',
          n: 'test-modulus',
          e: 'AQAB'
        }
      ]
    })
  })
]

export const server = setupServer(...handlers)
