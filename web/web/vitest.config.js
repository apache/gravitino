import { defineConfig } from 'vitest/config'
import { resolve } from 'path'

export default defineConfig({
  esbuild: {
    jsx: 'automatic',
    jsxImportSource: 'react'
  },
  define: {
    global: 'globalThis'
  },
  test: {
    globals: true,
    environment: 'jsdom',
    setupFiles: ['./src/test/setup.js'],
    include: ['src/**/*.{test,spec}.{js,ts,jsx,tsx}'],
    exclude: ['node_modules', 'dist', '.next'],
    coverage: {
      provider: 'v8',
      reporter: ['text', 'json', 'html'],
      exclude: [
        'node_modules/',
        'src/test/',
        '**/*.config.js',
        '**/*.d.ts',
        'coverage/**'
      ]
    }
  },
  resolve: {
    alias: {
      '@': resolve(__dirname, './src')
    }
  }
})
