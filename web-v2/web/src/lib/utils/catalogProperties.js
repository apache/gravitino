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

/**
 * A property is required when it is flagged as such, or when it is the warehouse of a Hive/JDBC
 * backed Iceberg catalog.
 *
 * @param {object} prop property definition from `providerBase[provider].defaultProps`
 * @param {object} context current dialog state
 * @param {string} context.currentProvider selected catalog provider
 * @param {string} context.catalogBackend selected `catalog-backend` value
 * @returns {boolean} whether the property must be filled in
 */
export const isCatalogRequiredField = (prop, { currentProvider, catalogBackend } = {}) => {
  return Boolean(
    prop.required ||
    (prop.key === 'warehouse' && currentProvider === 'lakehouse-iceberg' && ['hive', 'jdbc'].includes(catalogBackend))
  )
}

/**
 * Decides whether a catalog property field is hidden in the create/edit dialog.
 *
 * In edit mode only the fields the catalog reports are rendered, plus the required ones and the
 * `region`/`location` fields, so the dialog does not lose properties it cannot round-trip. Fields
 * declared with `alwaysVisible` (e.g. optional connector properties that were never set) stay
 * visible even when the catalog response omits them.
 *
 * @param {object} prop property definition from `providerBase[provider].defaultProps`
 * @param {object} context current dialog state
 * @param {boolean} context.editCatalog whether the dialog edits an existing catalog
 * @param {object} context.catalogProperties properties loaded from the edited catalog
 * @param {string} context.catalogBackend selected `catalog-backend` value
 * @param {string} context.authType selected `authentication.type` value
 * @param {string} context.currentProvider selected catalog provider
 * @returns {boolean} whether the property field is hidden
 */
export const isCatalogPropertyHidden = (prop, context = {}) => {
  const { editCatalog, catalogProperties, catalogBackend, authType, currentProvider } = context
  const { parentField, hide, key } = prop

  // Visibility driven by another field always wins.
  if (parentField === 'catalog-backend') {
    return Boolean(catalogBackend && hide && hide.includes(catalogBackend))
  }

  if (parentField === 'authentication.type') {
    return Boolean(!authType || (hide && hide.includes(authType)))
  }

  if (editCatalog && prop.alwaysVisible) {
    return false
  }

  // In edit mode, hide props not present in the loaded catalog response.
  if (editCatalog && catalogProperties && !(key in catalogProperties)) {
    return true
  }

  // Outside edit mode every field is rendered; in edit mode only required/region/location survive.
  return Boolean(
    editCatalog &&
    !['region', 'location'].includes(key) &&
    !isCatalogRequiredField(prop, { currentProvider, catalogBackend })
  )
}

/**
 * Merges the dialog's free-form property rows with the declared default properties into the
 * `properties` map sent to the server. Default properties are applied last so they win on conflicts.
 *
 * @param {object} args
 * @param {object} args.values current form values, including the free-form `properties` rows
 * @param {Array<object>} args.defaultProps default properties rendered as dedicated form fields
 * @param {boolean} args.editCatalog whether the dialog edits an existing catalog
 * @param {object} args.catalogProperties properties loaded from the edited catalog
 * @returns {object} the `properties` map to submit
 */
export const buildCatalogProperties = ({
  values = {},
  defaultProps = [],
  editCatalog = false,
  catalogProperties = {}
}) => {
  const properties = {}

  const appendProperty = (item, isDefaultProp) => {
    const { key } = item

    if (key === 'location' || key.startsWith('location-')) {
      if (item.value) {
        properties[key] = item.prefix ? item.prefix + item.value : item.value
      }

      return
    }

    const value = values[key] || (item.value instanceof Array ? item.value.join(',') : item.value)

    // Fields flagged alwaysVisible are rendered on the edit dialog even when the catalog does not
    // report them yet. Keep them absent until the user gives them a value, otherwise saving an
    // unrelated change would write empty or default-valued properties.
    if (isDefaultProp && editCatalog && !(key in catalogProperties)) {
      const defaultValue = item.value instanceof Array ? item.value.join(',') : item.value
      if (value === '' || value == null || value === defaultValue) {
        return
      }
    }

    properties[key] = value
  }

  ;(values.properties || []).forEach(item => appendProperty(item, false))
  defaultProps.forEach(item => appendProperty(item, true))

  return properties
}
