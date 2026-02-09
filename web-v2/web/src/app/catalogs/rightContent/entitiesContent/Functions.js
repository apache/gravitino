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

'use client'

import { useEffect, useMemo } from 'react'
import { Spin, Table } from 'antd'
import { useAntdColumnResize } from 'react-antd-column-resize'
import { useAppDispatch, useAppSelector } from '@/lib/hooks/useStore'
import { fetchFunctions } from '@/lib/store/metalakes'
import { formatToDateTime, isValidDate } from '@/lib/utils/date'
import Link from 'next/link'

export default function Functions({ metalake, catalog, schema }) {
  const dispatch = useAppDispatch()
  const store = useAppSelector(state => state.metalakes)

  useEffect(() => {
    if (!metalake || !catalog || !schema) return
    dispatch(fetchFunctions({ metalake, catalog, schema, init: true }))
  }, [dispatch, metalake, catalog, schema])

  const columns = useMemo(
    () => [
      {
        title: 'Name',
        dataIndex: 'name',
        key: 'name',
        width: 240,
        ellipsis: true,
        sorter: (a, b) =>
          (a?.name ?? '')
            .toString()
            .toLowerCase()
            .localeCompare((b?.name ?? '').toString().toLowerCase()),
        render: name => (
          <Link
            data-refer={`function-link-${name}`}
            href={`/catalogs?metalake=${encodeURIComponent(metalake)}&catalogType=relational&catalog=${encodeURIComponent(catalog)}&schema=${encodeURIComponent(schema)}&function=${encodeURIComponent(name)}`}
          >
            {name}
          </Link>
        )
      },
      {
        title: 'Creator',
        dataIndex: ['audit', 'creator'],
        key: 'creator',
        width: 180,
        render: (_, record) => record?.audit?.creator || '-'
      },
      {
        title: 'Created At',
        dataIndex: ['audit', 'createTime'],
        key: 'createdAt',
        width: 200,
        render: (_, record) => {
          const createTime = record?.audit?.createTime
          if (!createTime || !isValidDate(createTime)) return '-'

          return formatToDateTime(createTime)
        }
      }
    ],
    []
  )

  const { resizableColumns, components, tableWidth } = useAntdColumnResize(() => {
    return { columns, minWidth: 100 }
  }, [columns])

  return (
    <Spin spinning={store.tableLoading}>
      <Table
        data-refer='functions-table'
        size='small'
        style={{ maxHeight: 'calc(100vh - 30rem)' }}
        scroll={{ x: tableWidth, y: 'calc(100vh - 37rem)' }}
        dataSource={store.functions}
        pagination={{ position: ['bottomCenter'], showSizeChanger: true }}
        columns={resizableColumns}
        components={components}
        rowKey={record => record?.name}
      />
    </Spin>
  )
}
