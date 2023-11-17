const path = require('path')

/** @type {import('next').NextConfig} */

// console.log('output: ', process.env.OUTPUT_MODE)

module.exports = {
  output: process.env.OUTPUT_MODE || 'standalone', // export/standalone
  trailingSlash: true,
  reactStrictMode: false,
  webpack: config => {
    config.resolve.alias = {
      ...config.resolve.alias
    }
    config.resolve.alias['src'] = path.join(__dirname, 'src')

    // config.watchOptions = {
    //   poll: 1000,
    //   aggregateTimeout: 300
    // }

    return config
  }
}
