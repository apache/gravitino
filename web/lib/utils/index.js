/*
 * Copyright 2023 Datastrato Pvt Ltd.
 * This software is licensed under the Apache License version 2.
 */

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
