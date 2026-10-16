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

import { cloneDeep, isEqual } from 'lodash-es'

const editableProperties = {
  model: [],
  dataset: ['description'],
  field: ['label', 'description', 'datatype'],
  metric: ['description', 'datatype'],
  relationship: ['from', 'to', 'fromColumns', 'toColumns']
}

const missingMetadataMessage = 'The selected metadata no longer exists. Reload the model and try again.'

/** Finds the selected entity without flattening dataset-scoped field names. */
export const getSemanticMetadataEntity = (definition, target) => {
  const entity =
    target.kind === 'model'
      ? definition
      : target.kind === 'field'
        ? definition?.datasets
            ?.find(dataset => dataset.name === target.datasetName)
            ?.fields?.find(field => field.name === target.name)
        : definition?.[`${target.kind}s`]?.find(item => item.name === target.name)
  if (!entity) throw new Error(missingMetadataMessage)

  return entity
}

/** Supplies form values while keeping the original wire representation separate. */
export const semanticMetadataFormValues = entity => {
  if (!entity) throw new Error(missingMetadataMessage)

  return {
    ...cloneDeep(entity),
    dialects: cloneDeep(entity.expression?.dialects || []),
    contextFormat: typeof entity.aiContext === 'string' ? 'text' : 'structured',
    contextText: typeof entity.aiContext === 'string' ? entity.aiContext : '',
    instructions: typeof entity.aiContext === 'string' ? entity.aiContext : entity.aiContext?.instructions || '',
    synonyms: cloneDeep(entity.aiContext?.synonyms || []),
    examples: cloneDeep(entity.aiContext?.examples || [])
  }
}

/** Applies only edited form values, preserving every other entity and extension in the definition. */
export const updateSemanticMetadata = (definition, target, values) => {
  const updated = cloneDeep(definition)
  const entity = getSemanticMetadataEntity(updated, target)
  const initial = semanticMetadataFormValues(entity)
  for (const key of editableProperties[target.kind]) {
    const value = key === 'datatype' && values[key] === '' ? null : values[key]
    if (!isEqual(initial[key], value)) entity[key] = value ?? null
  }
  if (['field', 'metric'].includes(target.kind) && !isEqual(initial.dialects, values.dialects)) {
    entity.expression = { ...entity.expression, dialects: cloneDeep(values.dialects) }
  }

  if (values.contextFormat === 'text') {
    if (initial.contextFormat !== 'text' || initial.contextText !== values.contextText) {
      // Existing structured guidance stays structured so its extra keys cannot be discarded.
      if (entity.aiContext && typeof entity.aiContext === 'object') {
        entity.aiContext.instructions = values.contextText
      } else {
        entity.aiContext = values.contextText
      }
    }
  } else {
    const changedFormat = initial.contextFormat !== values.contextFormat

    const changedKeys = ['instructions', 'synonyms', 'examples'].filter(
      key => changedFormat || !isEqual(initial[key], values[key])
    )
    if (changedKeys.length) {
      entity.aiContext = typeof entity.aiContext === 'object' && entity.aiContext ? entity.aiContext : {}
      for (const key of changedKeys) entity.aiContext[key] = cloneDeep(values[key])
    }
  }

  return updated
}
