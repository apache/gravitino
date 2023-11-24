/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */

const path = require('path')

/** @type {import('next').NextConfig} */

const isProdEnv = process.env.NODE_ENV === 'production'

const devConfig = isProdEnv
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
    }

module.exports = {
  ...devConfig,
  distDir: 'dist',
  output: process.env.OUTPUT_MODE || 'standalone',
  trailingSlash: true,
  reactStrictMode: false,
  webpack: config => {
    config.resolve.alias = {
      ...config.resolve.alias
    }
    config.resolve.alias['src'] = path.join(__dirname, 'src')

    return config
  }
}
