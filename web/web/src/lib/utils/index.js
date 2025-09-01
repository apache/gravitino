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

export const genUpdates = (originalData, newData) => {
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
        comment: newColumn.comment
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
      if ((originalColumnsMap[key].type + originalColumnsMap[key].typeSuffix || '') !== newColumnsMap[key].type) {
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
      if (originalColumnsMap[key].comment !== newColumnsMap[key].comment) {
        updates.push({
          '@type': 'updateColumnComment',
          fieldName: [newColumnsMap[key].name],
          newComment: newColumnsMap[key].comment
        })
      }
    }
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
    if (node.children && node.children.length > 0) {
      return {
        ...node,
        isLeaf: node.children.length === 0,
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
