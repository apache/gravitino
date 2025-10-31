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

import { Alert, Spin, Table } from 'antd'

// import { useTablePreview } from '@/hooks'

export default function DataPreview({ ...props }) {
  const { currentMetalake, catalog, schema, table } = props.namespaces

  // const { data: previewData, isInitialLoading: isLoading } = useTablePreview(
  //   {
  //     keepPreviousData: true,
  //   },
  //   { metalake: currentMetalake, catalog, schema, table } as metadataParams,
  //   currentMetalake !== '',
  // )

  const getPreviewData = (type, value) => {
    if (typeof type === 'string') {
      if (value === null) return 'NULL'
      if (value === 0) return '0'
      if (type === 'boolean' && value !== null) return value ? 'true' : 'false'

      return value || '-'
    } else {
      const tCom = type?.type
      switch (tCom) {
        case 'struct':
          return `{${Object.entries(value)
            .map(([k, v]) => `${k}=${getPreviewData(type?.fields.find(f => f.name === k)?.type, v)}`)
            .join(', ')}}`
        case 'map':
          return `{${Object.entries(value)
            .map(([k, v]) => `${getPreviewData(type?.keyType, k)}=${getPreviewData(type?.valueType, v)}`)
            .join(', ')}}`
        case 'list':
          return `[${value.map(v => getPreviewData(type?.elementType, v)).join(', ')}]`
        default:
          return '-'
      }
    }
  }

  const columns =
    previewData?.columns?.map(col => {
      return {
        title: col.name,
        dataIndex: col.name,
        key: col.name,
        width: 150,
        ellipsis: true,
        render: text => {
          if (text === null) {
            return 'NULL'
          } else if (!text && col.type !== 'boolean' && Number.isNaN(text)) {
            return '-'
          } else if (typeof col.type === 'string') {
            return getPreviewData(col.type, text)
          } else if (typeof col.type === 'object') {
            const t = col.type?.type
            switch (t) {
              case 'struct':
                return `{${Object.entries(text)
                  .map(
                    ([key, value]) =>
                      `${key}=${getPreviewData(col.type?.fields.find(f => f.name === key)?.type, value)}`
                  )
                  .join(', ')}}`
              case 'map':
                return `{${Object.entries(text)
                  .map(
                    ([key, value]) =>
                      `${getPreviewData(col.type?.keyType, key)}=${getPreviewData(col.type?.valueType, value)}`
                  )
                  .join(', ')}}`
              case 'list':
                return `[${text.map(v => getPreviewData(col.type?.elementType, v)).join(', ')}]`
              default:
                return '-'
            }
          }
        }
      }
    }) || []

  const tableData =
    previewData?.results?.map((col, index) => {
      return {
        ...col,
        key: index
      }
    }) || []

  return (
    <Spin spinning={isLoading}>
      {previewData?.statusCode === 0 && (
        <Table
          columns={columns}
          dataSource={tableData}
          style={{ maxHeight: 'calc(100vh - 28rem)' }}
          scroll={{ y: 'calc(100vh - 34rem)' }}
        />
      )}
      {previewData?.statusCode === 1 && (
        <Alert
          message={t('common.previewWarning', { table: `${catalog}.${schema}.${table}` })}
          type='warning'
          showIcon
        />
      )}
      {previewData?.statusCode === 2 && (
        <Alert message={t('common.previewWarning2', { catalog: catalog })} type='warning' showIcon />
      )}
      {previewData?.statusCode === 3 && (
        <Alert message={t('common.previewWarning3', { catalog: catalog })} type='warning' showIcon />
      )}
    </Spin>
  )
}
