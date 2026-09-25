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

import { isEqual, isPlainObject } from 'lodash-es'

const SOURCE_NAMESPACE_LENGTH = 2
const DEFINITION_INDENT = 2

const isNonBlankString = value => typeof value === 'string' && value.trim() !== ''

const duplicatesOf = names => {
  const seen = new Set()
  const duplicates = new Set()

  names.forEach(name => {
    if (seen.has(name)) {
      duplicates.add(name)
    }
    seen.add(name)
  })

  return [...duplicates]
}

const validateSource = (dataset, errors) => {
  const { name, source } = dataset

  if (!isPlainObject(source)) {
    errors.push(`Dataset "${name}" must have a source with a namespace and a name.`)

    return
  }

  const { namespace } = source

  if (!Array.isArray(namespace) || namespace.length !== SOURCE_NAMESPACE_LENGTH || !namespace.every(isNonBlankString)) {
    errors.push(`Dataset "${name}" must have a source namespace of exactly [catalog, schema].`)
  }

  if (!isNonBlankString(source.name)) {
    errors.push(`Dataset "${name}" must have a source with a non-empty name.`)
  }
}

const validateDatasets = (datasets, errors) => {
  datasets.forEach((dataset, index) => {
    if (!isPlainObject(dataset) || !isNonBlankString(dataset.name)) {
      errors.push(`datasets[${index}] must have a non-empty name.`)

      return
    }

    validateSource(dataset, errors)

    const fields = dataset.fields
    if (fields !== undefined && !Array.isArray(fields)) {
      errors.push(`Dataset "${dataset.name}" must declare fields as an array.`)

      return
    }

    const duplicateFields = duplicatesOf((fields || []).map(field => field?.name))
    if (duplicateFields.length) {
      errors.push(`Field names must be unique within dataset "${dataset.name}": ${duplicateFields.join(', ')}.`)
    }
  })

  const duplicateDatasets = duplicatesOf(datasets.map(dataset => dataset?.name).filter(isNonBlankString))
  if (duplicateDatasets.length) {
    errors.push(`Dataset names must be unique within the semantic model: ${duplicateDatasets.join(', ')}.`)
  }
}

const validateRelationships = (relationships, datasetNames, errors) => {
  relationships.forEach((relationship, index) => {
    if (!isPlainObject(relationship) || !isNonBlankString(relationship.name)) {
      errors.push(`relationships[${index}] must have a non-empty name.`)

      return
    }

    const { name, from, to, fromColumns, toColumns } = relationship

    ;[from, to].forEach(endpoint => {
      if (!datasetNames.has(endpoint)) {
        errors.push(`Relationship "${name}" references an unknown dataset "${endpoint}".`)
      }
    })

    if (!Array.isArray(fromColumns) || !Array.isArray(toColumns) || !fromColumns.length || !toColumns.length) {
      errors.push(`Relationship "${name}" must have non-empty fromColumns and toColumns.`)

      return
    }

    if (fromColumns.length !== toColumns.length) {
      errors.push(`Relationship "${name}" must have the same number of fromColumns and toColumns.`)
    }
  })

  const duplicates = duplicatesOf(relationships.map(relationship => relationship?.name).filter(isNonBlankString))
  if (duplicates.length) {
    errors.push(`Relationship names must be unique within the semantic model: ${duplicates.join(', ')}.`)
  }
}

const validateMetrics = (metrics, errors) => {
  metrics.forEach((metric, index) => {
    if (!isPlainObject(metric) || !isNonBlankString(metric.name)) {
      errors.push(`metrics[${index}] must have a non-empty name.`)
    }
  })

  const duplicates = duplicatesOf(metrics.map(metric => metric?.name).filter(isNonBlankString))
  if (duplicates.length) {
    errors.push(`Metric names must be unique within the semantic model: ${duplicates.join(', ')}.`)
  }
}

/**
 * Checks the structural rules the server also enforces, so obvious mistakes are reported before the
 * request is sent. The server stays the authority: it additionally resolves every dataset source
 * and its columns against the catalog, which the browser cannot do.
 *
 * @param {unknown} definition The parsed semantic model definition.
 * @returns {string[]} The problems found, empty when the definition looks well formed.
 */
export const validateSemanticModelDefinition = definition => {
  const errors = []

  if (!isPlainObject(definition)) {
    return ['The definition must be a JSON object.']
  }

  const { datasets, relationships, metrics } = definition

  if (!Array.isArray(datasets) || !datasets.length) {
    return ['The definition must declare at least one dataset.']
  }

  validateDatasets(datasets, errors)

  const datasetNames = new Set(datasets.map(dataset => dataset?.name).filter(isNonBlankString))

  if (relationships !== undefined) {
    if (Array.isArray(relationships)) {
      validateRelationships(relationships, datasetNames, errors)
    } else {
      errors.push('The definition must declare relationships as an array.')
    }
  }

  if (metrics !== undefined) {
    if (Array.isArray(metrics)) {
      validateMetrics(metrics, errors)
    } else {
      errors.push('The definition must declare metrics as an array.')
    }
  }

  return errors
}

/**
 * Parses the definition authored in the dialog and validates it.
 *
 * @param {string} text The raw JSON text.
 * @returns {{ definition: object|null, errors: string[] }} The definition, or the problems found.
 */
export const parseSemanticModelDefinition = text => {
  if (!isNonBlankString(text)) {
    return { definition: null, errors: ['The definition is required.'] }
  }

  let parsed
  try {
    parsed = JSON.parse(text)
  } catch (err) {
    return { definition: null, errors: [`The definition is not valid JSON: ${err.message}`] }
  }

  const errors = validateSemanticModelDefinition(parsed)

  return errors.length ? { definition: null, errors } : { definition: parsed, errors: [] }
}

/**
 * Renders a definition for the JSON editor.
 *
 * @param {object|null|undefined} definition The definition to render.
 * @returns {string} The pretty-printed definition, or an empty string when there is none.
 */
export const formatSemanticModelDefinition = definition => {
  if (!definition) {
    return ''
  }

  return JSON.stringify(definition, null, DEFINITION_INDENT)
}

/**
 * Builds the atomic alter batch from what actually changed, so an unchanged definition is not
 * revalidated against its sources.
 *
 * @param {object} originalData The loaded semantic model.
 * @param {object} newData The edited semantic model.
 * @returns {object[]} The ordered updates, empty when nothing changed.
 */
export const genSemanticModelUpdates = (originalData, newData) => {
  const updates = []

  if (originalData.name !== newData.name) {
    updates.push({ '@type': 'rename', newName: newData.name })
  }

  const originalComment = originalData.comment || ''
  const newComment = newData.comment || ''

  if (originalComment !== newComment) {
    updates.push({ '@type': 'updateComment', newComment: newComment === '' ? null : newComment })
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
      updates.push({ '@type': 'setProperty', property: key, value: newProperties[key] })
    }
  }

  if (newData.definition && !isEqual(originalData.definition, newData.definition)) {
    updates.push({ '@type': 'replaceDefinition', definition: newData.definition })
  }

  return updates
}
