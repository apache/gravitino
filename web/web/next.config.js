/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

const withBundleAnalyzer = require('@next/bundle-analyzer')({
  enabled: process.env.ANALYZE === 'true'
})

const isProdEnv = process.env.NODE_ENV === 'production'

const apiUrl = process.env.NEXT_PUBLIC_API_URL
const oauthUri = process.env.NEXT_PUBLIC_OAUTH_URI
const oauthPath = process.env.NEXT_PUBLIC_OAUTH_PATH

/** @type {import('next').NextConfig} */
const nextConfig = {
  ...(isProdEnv
    ? {}
    : {
        // ** Just for development
        async rewrites() {
          return {
            fallback: [
              {
                source: '/api/:path*',
                destination: `${apiUrl}/api/:path*`
              },
              {
                source: '/configs',
                destination: `${apiUrl}/configs`
              },
              {
                source: `${oauthPath}`,
                destination: `${oauthUri}${oauthPath}`
              }
            ]
          }
        }
      }),
  output: process.env.OUTPUT_MODE || 'standalone',
  basePath: process.env.BASE_PATH,
  distDir: process.env.DIST_DIR,
  trailingSlash: false,
  reactStrictMode: true
}

module.exports = withBundleAnalyzer(nextConfig)
