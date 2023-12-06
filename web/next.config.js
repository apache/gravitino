/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */

const isProdEnv = process.env.NODE_ENV === 'production'

const apiUrl = process.env.NEXT_PUBLIC_API_URL
const oauthUrl = process.env.NEXT_PUBLIC_OAUTH_URL

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
                destination: `${apiUrl}/:path*`
              },
              {
                source: '/oauth2/token',
                destination: `${oauthUrl}`
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
