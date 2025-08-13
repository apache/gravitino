import '@testing-library/jest-dom'
import { beforeAll, afterEach, afterAll, vi } from 'vitest'
import { cleanup } from '@testing-library/react'
import { server } from './mocks/server'
import React from 'react'

// Make React available globally for JSX transformation
globalThis.React = React

// Start server before all tests
beforeAll(() => server.listen({ onUnhandledRequest: 'error' }))

// Reset handlers after each test `important for test isolation`
afterEach(() => {
  cleanup()
  server.resetHandlers()
})

// Clean up after the tests are finished
afterAll(() => server.close())

// Mock Next.js router - Required for components that use Next.js navigation
const mockRouter = {
  push: vi.fn(),
  replace: vi.fn(),
  prefetch: vi.fn(),
  back: vi.fn(),
  forward: vi.fn(),
  refresh: vi.fn(),
  pathname: '/',
  query: {},
  asPath: '/'
}

vi.mock('next/navigation', () => ({
  useRouter: () => mockRouter,
  usePathname: () => '/',
  useSearchParams: () => new URLSearchParams()
}))

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

// Global test utilities
global.mockRouter = mockRouter
