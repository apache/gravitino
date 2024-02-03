/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
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

export const genUpdates = (originalData, newData) => {
  const updates = []

  if (originalData.name !== newData.name) {
    updates.push({ '@type': 'rename', newName: newData.name })
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
        children
      }
    }
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
