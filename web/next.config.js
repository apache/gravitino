/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */

const isProdEnv = process.env.NODE_ENV === 'production'

/** @type {import('next').NextConfig} */
const nextConfig = {
  ...(isProdEnv
    ? {}
    : {
        async rewrites() {
          return {
            fallback: [
              {
                source: '/api/:path*',
                destination: 'http://localhost:8090/api/:path*'
              }
            ]
          }
        }
      }),
  output: process.env.OUTPUT_MODE || 'standalone',
  distDir: 'dist',
  trailingSlash: false,
  reactStrictMode: false
}

module.exports = nextConfig
