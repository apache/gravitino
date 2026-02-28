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

import _, { intersectionWith, isEqual, mergeWith, unionWith } from 'lodash-es'
import { isArray, isObject } from './is'
import { ColumnSpesicalType } from '@/config'
import dayjs from 'dayjs'
import { original } from '@reduxjs/toolkit'

const DATE_TIME_FORMAT = 'YYYY-MM-DD HH:mm:ss'

const getColumnType = type => {
  if (typeof type === 'string') {
    return type
  } else {
    return type.type
  }
}

export const isDevEnv = process.env.NODE_ENV === 'development'

export const isProdEnv = process.env.NODE_ENV === 'production'

export const to = (promise, errExt) => {
  return promise
    .then(data => [null, data])
    .catch(err => {
      if (errExt) {
        const error = Object.assign({}, err, errExt)

        return [error, undefined]
      }

      return [err, undefined]
    })
}

export const loggerVersion = version => {
  console.log(
    `Gravitino Version: %c${version}`,
    `color: white; background-color: #6062E0; padding: 2px; border-radius: 4px;`
  )
}

export const loggerGitHubInfo = (stars, forks) => {
  console.log(
    `Gravitino GitHubInfo: %c Stars ${stars}, Forks ${forks}`,
    `color: white; background-color: #6062E0; padding: 2px; border-radius: 4px;`
  )
}

export const genUpdates = (originalData, newData, disableUpdate = false) => {
  const updates = []

  if (originalData.name !== newData.name) {
    updates.push({ '@type': 'rename', newName: newData.name })
  }

  if (originalData.uri !== newData.uri) {
    updates.push({ '@type': 'updateUri', newUri: newData.uri })
  }

  const originalUris = originalData.uris || {}
  const newUris = newData.uris || {}

  for (const key in originalUris) {
    if (!(key in newUris)) {
      updates.push({ '@type': 'removeUri', uriName: key })
    }
  }

  for (const key in newUris) {
    if (originalUris[key] !== newUris[key]) {
      if (originalUris[key] === undefined) {
        updates.push({ '@type': 'addUri', uriName: key, uri: newUris[key] })
      } else {
        updates.push({ '@type': 'updateUri', uriName: key, newUri: newUris[key] })
      }
    }
  }

  const originalAliases = originalData.aliases || []
  const newAliases = newData.aliases || []

  const aliasesToAdd = newAliases.filter(alias => !originalAliases.includes(alias))
  const aliasesToRemove = originalAliases.filter(alias => !newAliases.includes(alias))

  if (aliasesToAdd.length > 0 || aliasesToRemove.length > 0) {
    updates.push({
      '@type': 'updateAliases',
      aliasesToAdd: aliasesToAdd,
      aliasesToRemove: aliasesToRemove
    })
  }

  if (originalData.comment !== newData.comment) {
    updates.push({ '@type': 'updateComment', newComment: newData.comment })
  }

  const originalContent = originalData.content || {}
  const newContent = newData.content || {}

  if (JSON.stringify(originalContent) !== JSON.stringify(newContent)) {
    updates.push({ '@type': 'updateContent', policyType: 'custom', newContent: newContent })
  }

  const originalProperties = originalData.properties || {}
  const newProperties = newData.properties || {}

  for (const key in originalProperties) {
    if (!(key in newProperties)) {
      updates.push({ '@type': 'removeProperty', property: key })
    }
  }

  for (const key in newProperties) {
    if (originalProperties[key] !== newProperties[key]) {
      if (originalProperties[key] === undefined) {
        updates.push({ '@type': 'setProperty', property: key, value: newProperties[key] })
      } else {
        updates.push({ '@type': 'setProperty', property: key, value: newProperties[key] })
      }
    }
  }

  const originalColumnsMap = _.keyBy(originalData.columns || [], 'uniqueId')
  const newColumnsMap = _.keyBy(newData.columns || [], 'uniqueId')

  for (const key in newColumnsMap) {
    if (!(key in originalColumnsMap)) {
      const { uniqueId, ...newColumn } = newColumnsMap[key]
      updates.push({
        '@type': 'addColumn',
        fieldName: [newColumn.name],
        type: newColumn.type,
        nullable: newColumn.nullable,
        comment: newColumn.comment,
        ...(newColumn.defaultValue !== undefined && newColumn.defaultValue !== null
          ? { defaultValue: newColumn.defaultValue }
          : {})
      })
    }
  }

  for (const key in originalColumnsMap) {
    if (!(key in newColumnsMap)) {
      updates.push({ '@type': 'deleteColumn', fieldName: [originalColumnsMap[key].name] })
    } else {
      if (originalColumnsMap[key].name !== newColumnsMap[key].name) {
        updates.push({
          '@type': 'renameColumn',
          oldFieldName: [originalColumnsMap[key].name],
          newFieldName: newColumnsMap[key].name
        })
      }
      if (
        getColumnType(originalColumnsMap[key].type) !== getColumnType(newColumnsMap[key].type).toLowerCase() ||
        (ColumnSpesicalType.includes(getColumnType(newColumnsMap[key].type).toLowerCase()) && !disableUpdate)
      ) {
        updates.push({
          '@type': 'updateColumnType',
          fieldName: [newColumnsMap[key].name],
          newType: newColumnsMap[key].type
        })
      }
      if (originalColumnsMap[key].nullable !== newColumnsMap[key].nullable) {
        updates.push({
          '@type': 'updateColumnNullability',
          fieldName: [newColumnsMap[key].name],
          nullable: newColumnsMap[key].nullable
        })
      }
      if (JSON.stringify(originalColumnsMap[key].defaultValue) !== JSON.stringify(newColumnsMap[key].defaultValue)) {
        updates.push({
          '@type': 'updateColumnDefaultValue',
          fieldName: [newColumnsMap[key].name],
          newDefaultValue: newColumnsMap[key].defaultValue
        })
      }
      if (
        (!originalColumnsMap[key].comment && newColumnsMap[key].comment) ||
        (originalColumnsMap[key].comment && !newColumnsMap[key].comment) ||
        (originalColumnsMap[key].comment &&
          newColumnsMap[key].comment &&
          originalColumnsMap[key].comment !== newColumnsMap[key].comment)
      ) {
        updates.push({
          '@type': 'updateColumnComment',
          fieldName: [newColumnsMap[key].name],
          newComment: newColumnsMap[key].comment
        })
      }
    }
  }

  // job template updateTemplate
  if (
    JSON.stringify(originalData) !== JSON.stringify(newData) &&
    originalData.jobType &&
    originalData.name === newData.name &&
    originalData.comment === newData.comment
  ) {
    const newTemplate = {
      '@type': newData.jobType
    }
    originalData.executable !== newData.executable && (newTemplate['newExecutable'] = newData.executable)
    JSON.stringify(originalData.arguments) !== JSON.stringify(newData.arguments) &&
      (newTemplate['newArguments'] = newData.arguments)
    JSON.stringify(originalData.environments) !== JSON.stringify(newData.environments) &&
      (newTemplate['newEnvironments'] = newData.environments)
    JSON.stringify(originalData.customFields) !== JSON.stringify(newData.customFields) &&
      (newTemplate['newCustomFields'] = newData.customFields)
    if (newData.jobType === 'shell') {
      JSON.stringify(originalData.scripts) !== JSON.stringify(newData.scripts) &&
        (newTemplate['newScripts'] = newData.scripts)
    } else {
      originalData.className !== newData.className && (newTemplate['newClassName'] = newData.className)
      JSON.stringify(originalData.jars) !== JSON.stringify(newData.jars) && (newTemplate['newJars'] = newData.jars)
      JSON.stringify(originalData.files) !== JSON.stringify(newData.files) && (newTemplate['newFiles'] = newData.files)
      JSON.stringify(originalData.archives) !== JSON.stringify(newData.archives) &&
        (newTemplate['newArchives'] = newData.archives)
      JSON.stringify(originalData.configs) !== JSON.stringify(newData.configs) &&
        (newTemplate['newConfigs'] = newData.configs)
    }
    updates.push({ '@type': 'updateTemplate', newTemplate })
  }

  return updates
}

export const hasNull = obj => {
  return Object.keys(obj).some(key => obj[key] === null)
}

/*
 * The following functions originally come from the MIT licensed Vben project.
 */

export const deepMerge = (source, target, mergeArrays = 'replace') => {
  if (!target) {
    return source
  }
  if (!source) {
    return target
  }

  return mergeWith({}, source, target, (sourceValue, targetValue) => {
    if (isArray(targetValue) && isArray(sourceValue)) {
      switch (mergeArrays) {
        case 'union':
          return unionWith(sourceValue, targetValue, isEqual)
        case 'intersection':
          return intersectionWith(sourceValue, targetValue, isEqual)
        case 'concat':
          return sourceValue.concat(targetValue)
        case 'replace':
          return targetValue
        default:
          throw new Error(`Unknown merge array strategy: ${mergeArrays}`)
      }
    }
    if (isObject(targetValue) && isObject(sourceValue)) {
      return deepMerge(sourceValue, targetValue, mergeArrays)
    }

    return undefined
  })
}

export function setObjToUrlParams(baseUrl, obj) {
  let parameters = ''
  for (const key in obj) {
    parameters += key + '=' + encodeURIComponent(obj[key]) + '&'
  }
  parameters = parameters.replace(/&$/, '')

  return /\?$/.test(baseUrl) ? baseUrl + parameters : baseUrl.replace(/\/?$/, '?') + parameters
}

export function extractPlaceholder(str) {
  const regex = /\{\{(.*?)\}\}/g
  let matches = []
  let match

  while ((match = regex.exec(str)) !== null) {
    matches.push(match[1])
  }

  return matches
}

export const updateTreeData = (list = [], key, children = []) => {
  return list.map(node => {
    if (node.key === key) {
      return {
        ...node,
        isLeaf: children?.length === 0,
        children
      }
    }

    // Always recurse if node has children property (even if empty array)
    if (node.children) {
      return {
        ...node,
        children: updateTreeData(node.children, key, children)
      }
    }

    return node
  })
}

export const findInTree = (tree, key, value) => {
  let result = null

  const found = _.find(tree, node => node[key] === value)
  if (found) {
    result = found
  } else {
    _.forEach(tree, node => {
      if (_.isEmpty(result) && node.children) {
        result = findInTree(node.children, key, value)
      }
    })
  }

  return result
}

export const formatNumber = num => {
  if (num >= 1000000) {
    return (num / 1000000).toFixed(1).replace(/\.0$/, '') + 'm'
  } else if (num >= 1000) {
    return (num / 1000).toFixed(1).replace(/\.0$/, '') + 'k'
  }

  return num.toString()
}

export const copyToClipboard = async text => {
  if (navigator.clipboard && window.isSecureContext) {
    return navigator.clipboard.writeText(text)
  } else {
    const textArea = document.createElement('textarea')
    textArea.value = text
    textArea.style.position = 'fixed'
    textArea.style.left = '-999999px'
    document.body.appendChild(textArea)
    textArea.focus()
    textArea.select()
    try {
      document.execCommand('copy')
    } catch (err) {
      throw new Error('Failed to copy path')
    }
    document.body.removeChild(textArea)

    return Promise.resolve()
  }
}

export const formatFileSize = sizeInBytes => {
  if (sizeInBytes === undefined || sizeInBytes === null) return '-'
  if (sizeInBytes < 1024) return `${sizeInBytes} B`
  if (sizeInBytes < 1024 * 1024) return `${(sizeInBytes / 1024).toFixed(2)} KB`
  if (sizeInBytes < 1024 * 1024 * 1024) return `${(sizeInBytes / (1024 * 1024)).toFixed(2)} MB`

  return `${(sizeInBytes / (1024 * 1024 * 1024)).toFixed(2)} GB`
}

export const formatLastModified = timestamp => {
  if (timestamp === undefined || timestamp === null) return '-'
  try {
    const date = new Date(Number(timestamp))
    const year = date.getFullYear()
    const month = (date.getMonth() + 1).toString().padStart(2, '0')
    const day = date.getDate().toString().padStart(2, '0')
    const hours = date.getHours().toString().padStart(2, '0')
    const minutes = date.getMinutes().toString().padStart(2, '0')
    const seconds = date.getSeconds().toString().padStart(2, '0')

    return `${year}-${month}-${day} ${hours}:${minutes}:${seconds}`
  } catch (e) {
    console.error('Error formatting lastModified:', e)

    return String(timestamp) // Fallback to string representation of bigint
  }
}

export function capitalizeFirstLetter(input) {
  if (!input) return input

  return input.charAt(0).toUpperCase() + input.slice(1)
}

export function extractNumbersInParentheses(str) {
  let regex = /\((\d+(?:,\d+)*)\)/g
  let matches = []
  let match

  while ((match = regex.exec(str)) !== null) {
    matches.push(match[1])
  }

  return matches[0] || ''
}

export function sanitizeText(str, options = {}) {
  const defaults = {
    removeTabs: true,
    removeNewlines: false,
    removeMultipleSpaces: false,
    trim: true,
    maxLength: null
  }

  const config = { ...defaults, ...options }
  let result = str

  if (config.removeTabs) {
    result = result.replace(/\t/g, ' ')
  }

  if (config.removeNewlines) {
    result = result.replace(/[\n\r]/g, ' ')
  }

  if (config.removeMultipleSpaces) {
    result = result.replace(/\s+/g, ' ')
  }

  if (config.trim) {
    result = result.trim()
  }

  if (config.maxLength && result.length > config.maxLength) {
    result = result.substring(0, config.maxLength)
  }

  return result
}

export function generateRandomColor() {
  return (
    '#' +
    Math.floor(Math.random() * 16777215)
      .toString(16)
      .padStart(6, '0')
  )
}

export const formatToDateTime = (date, format = DATE_TIME_FORMAT) => {
  return dayjs(date).format(format)
}

export const getJobParamValues = params => {
  if (!params) return []

  // Match the content within the double curly braces {{...}}
  const regex = /\{\{([^}]+)\}\}/g
  const matches = []
  let match

  while ((match = regex.exec(params)) !== null) {
    matches.push(match[1])
  }

  return matches
}
