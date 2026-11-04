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

import { describe, expect, it } from 'vitest'
import { buildCatalogProperties, isCatalogPropertyHidden, isCatalogRequiredField } from '@/lib/utils/catalogProperties'
import { providerBase } from '@/config/catalog'

const glueProps = providerBase.glue.defaultProps
const prop = key => glueProps.find(item => item.key === key)

describe('isCatalogRequiredField', () => {
  it('treats props flagged as required as required', () => {
    expect(isCatalogRequiredField(prop('aws-region'), { currentProvider: 'glue' })).toBe(true)
  })

  it('requires the warehouse of hive and jdbc backed Iceberg catalogs', () => {
    const warehouse = { key: 'warehouse', required: false }

    expect(isCatalogRequiredField(warehouse, { currentProvider: 'lakehouse-iceberg', catalogBackend: 'jdbc' })).toBe(
      true
    )
    expect(isCatalogRequiredField(warehouse, { currentProvider: 'lakehouse-iceberg', catalogBackend: 'hive' })).toBe(
      true
    )
    expect(isCatalogRequiredField(warehouse, { currentProvider: 'lakehouse-iceberg', catalogBackend: 'rest' })).toBe(
      false
    )
    expect(isCatalogRequiredField(warehouse, { currentProvider: 'glue' })).toBe(false)
  })
})

describe('isCatalogPropertyHidden', () => {
  it('renders every field while creating a catalog', () => {
    const context = { editCatalog: false, catalogProperties: {} }

    glueProps.forEach(item => {
      expect(isCatalogPropertyHidden(item, context)).toBe(false)
    })
  })

  it('keeps optional Glue properties visible on the edit dialog when the catalog omits them', () => {
    const context = { editCatalog: true, catalogProperties: { 'aws-region': 'us-east-1', warehouse: 's3://b/w' } }

    expect(isCatalogPropertyHidden(prop('aws-glue-catalog-id'), context)).toBe(false)
    expect(isCatalogPropertyHidden(prop('aws-glue-endpoint'), context)).toBe(false)
    expect(isCatalogPropertyHidden(prop('default-table-format'), context)).toBe(false)
    expect(isCatalogPropertyHidden(prop('table-format-filter'), context)).toBe(false)
  })

  it('hides optional properties the catalog does not report and that are not flagged alwaysVisible', () => {
    const context = { editCatalog: true, catalogProperties: { 'aws-region': 'us-east-1' } }

    expect(isCatalogPropertyHidden(prop('aws-access-key-id'), context)).toBe(true)
    expect(isCatalogPropertyHidden(prop('aws-secret-access-key'), context)).toBe(true)
  })

  it('keeps required, region and location properties visible on the edit dialog', () => {
    const context = {
      editCatalog: true,
      catalogProperties: { 'aws-region': 'us-east-1', 'jdbc-url': 'jdbc:mysql://h:3306', location: 's3://b/w' }
    }

    expect(isCatalogPropertyHidden(prop('aws-region'), context)).toBe(false)
    expect(isCatalogPropertyHidden({ key: 'location' }, context)).toBe(false)
    expect(isCatalogPropertyHidden({ key: 'jdbc-url', required: true }, context)).toBe(false)
    expect(isCatalogPropertyHidden({ key: 'jdbc-database' }, context)).toBe(true)

    // A location the catalog does not report stays hidden, required or not.
    expect(isCatalogPropertyHidden({ key: 'location' }, { editCatalog: true, catalogProperties: {} })).toBe(true)
  })

  it('hides alwaysVisible properties whose visibility depends on another field', () => {
    const jdbcDriver = {
      key: 'jdbc-driver',
      required: true,
      alwaysVisible: true,
      parentField: 'catalog-backend',
      hide: ['hive', 'rest']
    }

    expect(
      isCatalogPropertyHidden(jdbcDriver, { editCatalog: true, catalogProperties: {}, catalogBackend: 'hive' })
    ).toBe(true)
    expect(
      isCatalogPropertyHidden(jdbcDriver, { editCatalog: true, catalogProperties: {}, catalogBackend: 'jdbc' })
    ).toBe(false)
  })

  it('hides fields depending on the selected catalog backend', () => {
    const jdbcDriver = {
      key: 'jdbc-driver',
      required: true,
      parentField: 'catalog-backend',
      hide: ['hive', 'rest']
    }

    expect(
      isCatalogPropertyHidden(jdbcDriver, { editCatalog: false, catalogProperties: {}, catalogBackend: 'jdbc' })
    ).toBe(false)
    expect(
      isCatalogPropertyHidden(jdbcDriver, { editCatalog: false, catalogProperties: {}, catalogBackend: 'hive' })
    ).toBe(true)
  })

  it('hides Kerberos fields when the authentication type is simple', () => {
    const kerberosKeytab = {
      key: 'authentication.kerberos.keytab-uri',
      parentField: 'authentication.type',
      hide: ['simple']
    }

    expect(isCatalogPropertyHidden(kerberosKeytab, { editCatalog: false, authType: 'simple' })).toBe(true)
    expect(isCatalogPropertyHidden(kerberosKeytab, { editCatalog: false, authType: 'Kerberos' })).toBe(false)
    expect(isCatalogPropertyHidden(kerberosKeytab, { editCatalog: false, authType: undefined })).toBe(true)
  })
})

describe('buildCatalogProperties', () => {
  const visibleGlueProps = glueProps.filter(
    item => !isCatalogPropertyHidden(item, { editCatalog: true, catalogProperties: {} })
  )

  it('submits the declared default properties while creating a catalog', () => {
    const properties = buildCatalogProperties({
      values: { provider: 'glue' },
      defaultProps: visibleGlueProps,
      editCatalog: false
    })

    expect(properties).toMatchObject({ 'default-table-format': 'hive', 'table-format-filter': 'all' })
  })

  it('keeps alwaysVisible properties out of the request when the catalog omits them and they are untouched', () => {
    const properties = buildCatalogProperties({
      values: { 'aws-region': 'us-east-1', warehouse: 's3://b/w' },
      defaultProps: visibleGlueProps,
      editCatalog: true,
      catalogProperties: { 'aws-region': 'us-east-1', warehouse: 's3://b/w' }
    })

    expect(properties).not.toHaveProperty('aws-glue-catalog-id')
    expect(properties).not.toHaveProperty('aws-glue-endpoint')
    expect(properties).not.toHaveProperty('default-table-format')
    expect(properties).not.toHaveProperty('table-format-filter')
  })

  it('submits alwaysVisible properties the user sets on the edit dialog', () => {
    const properties = buildCatalogProperties({
      values: {
        'aws-region': 'us-east-1',
        warehouse: 's3://b/w',
        'aws-glue-endpoint': 'http://localhost:4566',
        'default-table-format': 'iceberg'
      },
      defaultProps: visibleGlueProps,
      editCatalog: true,
      catalogProperties: { 'aws-region': 'us-east-1', warehouse: 's3://b/w' }
    })

    expect(properties).toMatchObject({
      'aws-glue-endpoint': 'http://localhost:4566',
      'default-table-format': 'iceberg'
    })

    // Untouched fields still holding their declared default are left out of the request.
    expect(properties).not.toHaveProperty('table-format-filter')
  })

  it('round-trips properties the catalog already reports', () => {
    const catalogProperties = { 'aws-glue-endpoint': 'http://localhost:4566', 'default-table-format': 'iceberg' }

    const values = providerBase.glue.defaultProps.reduce((acc, item) => {
      if (!isCatalogPropertyHidden(item, { editCatalog: true, catalogProperties })) {
        acc[item.key] = catalogProperties[item.key] ?? item.value
      }

      return acc
    }, {})

    const properties = buildCatalogProperties({
      values,
      defaultProps: visibleGlueProps,
      editCatalog: true,
      catalogProperties
    })

    expect(properties).toMatchObject(catalogProperties)
  })

  it('keeps the free-form property rows and the location handling', () => {
    const properties = buildCatalogProperties({
      values: { properties: [{ key: 'custom-key', value: 'custom-value' }] },
      defaultProps: [{ key: 'location', value: 'warehouse/schema', prefix: 's3://bucket/' }],
      editCatalog: true,
      catalogProperties: {}
    })

    expect(properties).toEqual({ 'custom-key': 'custom-value', location: 's3://bucket/warehouse/schema' })
  })
})
