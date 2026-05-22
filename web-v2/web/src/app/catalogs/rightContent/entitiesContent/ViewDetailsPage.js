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

import { useEffect, useMemo, useState } from 'react'
import {
  Descriptions,
  Divider,
  Flex,
  Input,
  Popover,
  Space,
  Segmented,
  Spin,
  Table,
  Tabs,
  Tag,
  Tooltip,
  Typography,
  message
} from 'antd'
import { useAntdColumnResize } from 'react-antd-column-resize'
import useResizeObserver from 'use-resize-observer'
import { format } from 'sql-formatter'
import { highlight } from 'sql-highlight'
import Icons from '@/components/Icons'
import { ColumnTypeColorEnum, sqlFormatterDialectMap } from '@/config'
import { useAppSelector } from '@/lib/hooks/useStore'
import { useSearchParams } from 'next/navigation'
import { formatToDateTime, isValidDate } from '@/lib/utils/date'
import PropertiesContent from '@/components/PropertiesContent'

const { Title, Paragraph } = Typography
const { Search } = Input

export default function ViewDetailsPage() {
  const searchParams = useSearchParams()
  const viewName = searchParams.get('view')
  const store = useAppSelector(state => state.metalakes)
  const viewData = store.activatedDetails
  const [search, setSearch] = useState('')
  const [tabKey, setTabKey] = useState('Columns')
  const [activeSqlKey, setActiveSqlKey] = useState(null)
  const [messageApi, contextHolder] = message.useMessage()
  const { ref, width } = useResizeObserver()

  const tableData = viewData?.columns
    ?.filter(c => {
      if (search === '') return true

      return c.name.includes(search)
    })
    .map(col => ({
      ...col,
      key: col.name,
      type: col.type
    }))

  const columnTypeFilters = viewData?.columns
    ?.map(column => (typeof column.type === 'string' ? column.type : column.type.type))
    .filter((value, index, self) => self.indexOf(value) === index)
    .map(t => ({
      text: t,
      value: t
    }))

  const columnTypeColor = type => {
    const formatType = typeof type === 'string' ? type.replace(/\(.*\)/, '') : 'objectType'

    return ColumnTypeColorEnum[formatType]
  }

  const representations = viewData?.representations || []
  const properties = viewData?.properties
  const defaultCatalog = viewData?.defaultCatalog
  const defaultSchema = viewData?.defaultSchema
  const createdAt = viewData?.audit?.createTime
  const createdAtText = !createdAt || !isValidDate(createdAt) ? '-' : formatToDateTime(createdAt)

  const formatSql = (sql, dialect) => {
    if (!sql) {
      return '-'
    }

    const normalizedDialect = (dialect || 'sql').toLowerCase()
    const mappedDialect = sqlFormatterDialectMap[normalizedDialect]

    try {
      if (mappedDialect) {
        return format(sql, {
          language: mappedDialect,
          keywordCase: 'upper'
        })
      }

      return format(sql, { keywordCase: 'upper' })
    } catch {
      return sql
    }
  }

  const onCopySql = async sql => {
    try {
      await navigator.clipboard.writeText(sql)
      messageApi.success('SQL copied')
    } catch {
      messageApi.error('Failed to copy SQL')
    }
  }

  const highlightSql = sql => {
    try {
      return highlight(sql, { html: true })
    } catch {
      return sql
    }
  }

  const sqlSegmentItems = useMemo(
    () =>
      representations.map((rep, index) => {
        const formattedSql = formatSql(rep?.sql, rep?.dialect)

        return {
          key: `sql-${index}-${rep?.dialect || 'unknown'}`,
          label: rep?.dialect || `dialect-${index + 1}`,
          formattedSql
        }
      }),
    [representations]
  )

  useEffect(() => {
    if (sqlSegmentItems.length === 0) {
      setActiveSqlKey(null)

      return
    }

    if (!sqlSegmentItems.some(item => item.key === activeSqlKey)) {
      setActiveSqlKey(sqlSegmentItems[0].key)
    }
  }, [sqlSegmentItems, activeSqlKey])

  const activeSqlItem =
    sqlSegmentItems.find(item => item.key === activeSqlKey) || (sqlSegmentItems.length > 0 ? sqlSegmentItems[0] : null)

  const tabOptions = [
    { label: 'Columns', key: 'Columns' },
    { label: 'SQL', key: 'SQL' }
  ]

  const onChangeTab = key => {
    setTabKey(key)
  }

  const onSearchColumn = e => {
    const { value } = e.target
    setSearch(value)
  }

  const columns = useMemo(
    () => [
      {
        title: 'Column Name',
        dataIndex: 'name',
        key: 'name',
        width: 300,
        ellipsis: true,
        sorter: (a, b) => a.name.toLowerCase().localeCompare(b.name.toLowerCase()),
        render: name => <span>{name}</span>
      },
      {
        title: 'Data Type',
        dataIndex: 'type',
        key: 'type',
        width: 200,
        ellipsis: true,
        filters: columnTypeFilters,
        onFilter: (value, record) =>
          typeof record.type === 'string' ? record.type.indexOf(value) === 0 : record.type.type.indexOf(value) === 0,
        render: type => <Tag color={columnTypeColor(type)}>{typeof type === 'string' ? type : type.type}</Tag>
      },
      {
        title: 'Nullable',
        dataIndex: 'nullable',
        key: 'nullable',
        width: 100,
        render: nullable => <span>{nullable ? 'Yes' : 'No'}</span>
      },
      {
        title: 'Comment',
        dataIndex: 'comment',
        key: 'comment',
        ellipsis: true,
        render: comment => <span>{comment || '-'}</span>
      }
    ],
    [viewData]
  )

  const { resizableColumns, components, tableWidth } = useAntdColumnResize(() => {
    return { columns, minWidth: 100 }
  }, [columns])

  return (
    <>
      {contextHolder}
      <Spin spinning={store.activatedDetailsLoading}>
        <Flex className='mb-2' gap='small' align='flex-start' ref={ref}>
          <div className='size-8'>
            <Icons.iconify icon='ic:outline-table-view' className='my-icon-large' />
          </div>
          <div className='grow-1 relative bottom-1'>
            <Title level={3} style={{ marginBottom: '0.125rem' }}>
              <span
                title={viewName}
                className='min-w-10 truncate'
                style={{ maxWidth: `calc(${width}px - 56px)`, display: 'inherit' }}
              >
                {viewName}
              </span>
            </Title>
            <Paragraph
              type='secondary'
              className='truncate'
              title={viewData?.comment}
              style={{ marginBottom: 0, maxWidth: `calc(${width}px - 56px)` }}
            >
              {viewData?.comment}
            </Paragraph>
          </div>
        </Flex>
        <Space split={<Divider type='vertical' />} wrap={true} className='mb-2'>
          {viewData?.audit?.creator && (
            <Space size={4}>
              <Tooltip title='Creator' placement='top'>
                <Icons.User className='size-4' color='grey' />
              </Tooltip>
              <span>{viewData.audit.creator}</span>
            </Space>
          )}
          <Space size={4}>
            <Tooltip title='Created' placement='top'>
              <Icons.iconify icon='mdi:clock-outline' className='size-4' props={{ color: 'grey' }} />
            </Tooltip>
            <span>{createdAtText}</span>
          </Space>
          {properties && Object.keys(properties).length > 0 && (
            <Space size={4}>
              <Tooltip title='Properties' placement='top'>
                <Icons.TableProperties className='size-4' color='grey' />
              </Tooltip>
              <Popover
                placement='bottom'
                title={<span>Properties</span>}
                content={<PropertiesContent properties={properties} />}
              >
                <a className='text-defaultPrimary'>{Object.keys(properties).length}</a>
              </Popover>
            </Space>
          )}
        </Space>
      </Spin>
      <Tabs data-refer='details-tabs' defaultActiveKey={tabKey} onChange={onChangeTab} items={tabOptions} />
      {tabKey === 'Columns' && (
        <>
          <Flex justify='flex-end' className='mb-4'>
            <div className='flex w-1/3 gap-4'>
              <Search name='searchColumnInput' placeholder='Search...' value={search} onChange={onSearchColumn} />
            </div>
          </Flex>
          <Spin spinning={store.activatedDetailsLoading}>
            <Table
              data-refer='view-columns-grid'
              size='small'
              style={{ maxHeight: 'calc(100vh - 30rem)' }}
              scroll={{ x: tableWidth, y: 'calc(100vh - 37rem)' }}
              dataSource={tableData}
              pagination={{ position: ['bottomCenter'], showSizeChanger: true }}
              columns={resizableColumns}
              components={components}
            />
          </Spin>
        </>
      )}
      {tabKey === 'SQL' && (
        <div className='mt-2'>
          {(defaultCatalog || defaultSchema) && (
            <Descriptions
              layout='horizontal'
              column={1}
              size='small'
              bordered
              className='mb-4'
              labelStyle={{ width: '20%' }}
            >
              {defaultCatalog && <Descriptions.Item label='Default Catalog'>{defaultCatalog}</Descriptions.Item>}
              {defaultSchema && <Descriptions.Item label='Default Schema'>{defaultSchema}</Descriptions.Item>}
            </Descriptions>
          )}
          {representations.length > 0 ? (
            <>
              <Segmented
                className='mb-3'
                size='small'
                options={sqlSegmentItems.map(item => ({ label: item.label, value: item.key }))}
                value={activeSqlItem?.key}
                onChange={setActiveSqlKey}
              />
              {activeSqlItem && (
                <div className='relative rounded border border-borderColor p-3'>
                  <Tooltip title='Copy SQL' placement='top'>
                    <button
                      type='button'
                      className='absolute right-3 top-3 inline-flex cursor-pointer items-center rounded border border-borderColor bg-transparent p-1 text-textSecondary transition-colors hover:text-defaultPrimary'
                      onClick={() => onCopySql(activeSqlItem.formattedSql)}
                    >
                      <Icons.Copy className='size-4' />
                    </button>
                  </Tooltip>
                  <pre
                    className='m-0 whitespace-pre-wrap break-all font-mono text-sm leading-6'
                    dangerouslySetInnerHTML={{ __html: highlightSql(activeSqlItem.formattedSql) }}
                  />
                </div>
              )}
            </>
          ) : (
            <Paragraph type='secondary'>No SQL representations available.</Paragraph>
          )}
        </div>
      )}
    </>
  )
}
