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

// Format tokens rather than parsed values to preserve numbers, key order, and duplicate keys.
export const formatVendorJson = data => {
  try {
    const parsed = JSON.parse(data)
    if (!parsed || typeof parsed !== 'object') return null
  } catch {
    return null
  }
  const tokens = data.match(/"(?:\\.|[^"\\])*"|[{}\[\],:]|[^\s{}\[\],:]+/g)
  let depth = 0
  let formatted = ''
  const newline = () => '\n' + '  '.repeat(depth)
  tokens.forEach((token, index) => {
    if (token === '{' || token === '[') {
      depth++
      formatted += token
      if (!['}', ']'].includes(tokens[index + 1])) formatted += newline()
    } else if (token === '}' || token === ']') {
      depth--
      if (!['{', '['].includes(tokens[index - 1])) formatted += newline()
      formatted += token
    } else if (token === ',') formatted += ',' + newline()
    else if (token === ':') formatted += ': '
    else formatted += token
  })

  return formatted
}
