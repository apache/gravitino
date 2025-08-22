import { beforeAll, afterEach, afterAll, vi } from 'vitest'
import { server } from './mocks/server'

// Start server before all tests
beforeAll(() => server.listen({ onUnhandledRequest: 'error' }))

// Reset handlers after each test for test isolation
afterEach(() => {
  server.resetHandlers()
})

// Clean up after the tests are finished
afterAll(() => server.close())

// Mock window.location - Required for OIDC redirect URI construction
Object.defineProperty(window, 'location', {
  value: {
    href: 'http://localhost:3000',
    origin: 'http://localhost:3000',
    pathname: '/',
    search: '',
    hash: ''
  },
  writable: true
})
