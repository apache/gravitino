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

import tailwindTheme from './src/lib/theme/tailwind'
const { fontFamily } = require('tailwindcss/defaultTheme')

/** @type {import('tailwindcss').Config} */
const config = {
  darkMode: 'selector',
  corePlugins: {
    preflight: false
  },
  content: ['./src/**/*.{js,css}'],
  theme: {
    container: {
      center: true,
      padding: '1.5rem',
      screens: {
        '2xl': '1440px'
      }
    },
    extend: {
      fontFamily: {
        sans: ['var(--font-sans)', ...fontFamily.sans]
      },
      backgroundColor: {
        colorDarkBgContainer: '#141414',
        colorDarkBg: '#1f1f1f',
        colorColumnBg: 'rgba(60,60,60,1)',
        primary: '#1677ff',
        defaultPrimary: '#0369a1',
        borderBlack: '#303030',
        borderWhite: '#ffffff'
      },
      colors: {
        primary: '#1677ff',
        defaultPrimary: '#0369a1',
        borderBlack: '#303030',
        borderWhite: 'rgba(5,5,5,0.06)'
      },
      backgroundImage: {
        loginBg: 'url("/image/login-bg.jpg")'
      }
    }
  },
  plugins: []
}

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

module.exports = process.env.BUILD_TARGET === 'old' ? tailwindConfig : config
