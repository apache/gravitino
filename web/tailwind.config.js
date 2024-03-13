/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

import tailwindTheme from './src/lib/theme/tailwind'

/** @type {import('tailwindcss').Config} */
const tailwindConfig = {
  darkMode: 'class',
  corePlugins: {
    preflight: false
  },
  important: true,
  prefix: 'twc-',
  content: [
    './src/**/*.{js,ts,jsx,tsx,mdx}',
    './app/**/*.{js,ts,jsx,tsx,mdx}',
    './components/**/*.{js,ts,jsx,tsx,mdx}',
    './lib/**/*.{js,ts,jsx,tsx,mdx}',
    './pages/**/*.{js,ts,jsx,tsx,mdx}',
    './views/**/*.{js,ts,jsx,tsx,mdx}'
  ],
  theme: tailwindTheme,
  variants: {
    extends: {}
  },
  plugins: []
}

module.exports = tailwindConfig
