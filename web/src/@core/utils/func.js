/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */

export const randomColor = () => {
  let maxVal = 0xffffff
  let randomNumber = Math.random() * maxVal
  randomNumber = Math.floor(randomNumber)
  randomNumber = randomNumber.toString(16)
  let randColor = randomNumber.padStart(6, 0)

  return `#${randColor.toUpperCase()}`
}

export function setObjToUrlParams(baseUrl, obj) {
  let parameters = ''
  for (const key in obj) {
    parameters += key + '=' + encodeURIComponent(obj[key]) + '&'
  }
  parameters = parameters.replace(/&$/, '')

  return /\?$/.test(baseUrl) ? baseUrl + parameters : baseUrl.replace(/\/?$/, '?') + parameters
}

export function deepMerge(src = {}, target = {}) {
  let key
  for (key in target) {
    src[key] = isObject(src[key]) ? deepMerge(src[key], target[key]) : (src[key] = target[key])
  }

  return src
}

export const parseNum = (number, m = 0) => {
  return Math.round(Math.pow(10, m) * number) / Math.pow(10, m)
}

/**
 * Calculate the page number based on the total number of items.
 * @param {number} count
 * @param {number} pageSize
 * @returns
 */
export function pagerCount(count, pageSize) {
  if (typeof count == 'number') {
    if (count > 0) {
      try {
        let _pagerCount = count % pageSize == 0 ? count / pageSize : count / pageSize + 1
        let c = _pagerCount.toFixed(0)
        _pagerCount = c > _pagerCount ? c - 1 : c

        return _pagerCount
      } catch (error) {
        return 0
      }
    } else {
      return 0
    }
  } else {
    return 0
  }
}

export const genUpdateMetalakeUpdates = (originalData, newData) => {
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
