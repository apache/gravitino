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

import { useMemo, useState } from 'react'
import { Divider, Flex, Popover, Space, Spin, Table, Tabs, Tag, Tooltip, Typography, message } from 'antd'
import { useAntdColumnResize } from 'react-antd-column-resize'
import useResizeObserver from 'use-resize-observer'
import { useSearchParams } from 'next/navigation'
import Tags from '@/components/CustomTags'
import GetOwner from '@/components/GetOwner'
import Icons from '@/components/Icons'
import PropertiesContent from '@/components/PropertiesContent'
import { useAppSelector } from '@/lib/hooks/useStore'
import { copyToClipboard } from '@/lib/utils'
import { formatToDateTime, isValidDate } from '@/lib/utils/date'
import { formatSemanticModelDefinition } from '@/lib/utils/semanticModel'

const { Title, Paragraph } = Typography

const sourceFullName = source => {
  if (!source) {
    return '-'
  }

  return [...(source.namespace || []), source.name].filter(Boolean).join('.')
}

const renderExpression = expression => {
  const dialects = expression?.dialects || []

  if (!dialects.length) {
    return <span>-</span>
  }

  return (
    <Space direction='vertical' size={2}>
      {dialects.map((dialect, index) => (
        <span key={`${dialect?.dialect}-${index}`}>
          <Tag>{dialect?.dialect}</Tag>
          <span className='font-mono text-xs'>{dialect?.expression}</span>
        </span>
      ))}
    </Space>
  )
}

const renderColumns = columns => <span className='font-mono text-xs'>{(columns || []).join(', ') || '-'}</span>

export default function SemanticModelDetailsPage() {
  const searchParams = useSearchParams()
  const metalake = searchParams.get('metalake')
  const catalog = searchParams.get('catalog')
  const schema = searchParams.get('schema')
  const semanticModelName = searchParams.get('semanticModel')
  const auth = useAppSelector(state => state.auth)
  const { anthEnable } = auth
  const store = useAppSelector(state => state.metalakes)
  const semanticModel = store.activatedDetails
  const [tabKey, setTabKey] = useState('Datasets')
  const { ref, width } = useResizeObserver()

  const definition = semanticModel?.definition
  const properties = semanticModel?.properties
  const createdAt = semanticModel?.audit?.createTime
  const createdAtText = !createdAt || !isValidDate(createdAt) ? '-' : formatToDateTime(createdAt)
  const metadataObjectFullName = `${catalog}.${schema}.${semanticModelName}`

  const datasets = useMemo(
    () => (definition?.datasets || []).map(dataset => ({ ...dataset, key: dataset.name })),
    [definition]
  )

  const relationships = useMemo(
    () => (definition?.relationships || []).map(relationship => ({ ...relationship, key: relationship.name })),
    [definition]
  )

  const metrics = useMemo(
    () => (definition?.metrics || []).map(metric => ({ ...metric, key: metric.name })),
    [definition]
  )

  const definitionText = useMemo(() => formatSemanticModelDefinition(definition), [definition])

  const tagContent = (
    <div>
      <Tags readOnly={true} metadataObjectType={'semantic_model'} metadataObjectFullName={metadataObjectFullName} />
    </div>
  )

  const propertyContent = <PropertiesContent properties={properties} />

  const onCopyDefinition = async () => {
    try {
      await copyToClipboard(definitionText)
      message.success('Definition copied!')
    } catch (err) {
      console.error('Failed to copy definition: ', err)
      message.error('Failed to copy definition')
    }
  }

  const datasetColumns = useMemo(
    () => [
      {
        title: 'Dataset Name',
        dataIndex: 'name',
        key: 'name',
        width: 200,
        ellipsis: true,
        sorter: (a, b) => a.name.toLowerCase().localeCompare(b.name.toLowerCase())
      },
      {
        title: 'Source',
        dataIndex: 'source',
        key: 'source',
        width: 260,
        ellipsis: true,
        render: source => <span className='font-mono text-xs'>{sourceFullName(source)}</span>
      },
      {
        title: 'Primary Key',
        dataIndex: 'primaryKey',
        key: 'primaryKey',
        width: 180,
        ellipsis: true,
        render: renderColumns
      },
      {
        title: 'Fields',
        dataIndex: 'fields',
        key: 'fields',
        width: 80,
        render: fields => <span>{(fields || []).length}</span>
      },
      {
        title: 'Description',
        dataIndex: 'description',
        key: 'description',
        ellipsis: true,
        render: description => <span>{description || '-'}</span>
      }
    ],
    []
  )

  const fieldColumns = useMemo(
    () => [
      { title: 'Field Name', dataIndex: 'name', key: 'name', width: 200, ellipsis: true },
      {
        title: 'Data Type',
        dataIndex: 'datatype',
        key: 'datatype',
        width: 140,
        render: datatype => (datatype ? <Tag>{datatype}</Tag> : <span>-</span>)
      },
      {
        title: 'Time Dimension',
        dataIndex: 'dimension',
        key: 'dimension',
        width: 140,
        render: dimension => <span>{dimension?.isTime ? 'Yes' : 'No'}</span>
      },
      {
        title: 'Expression',
        dataIndex: 'expression',
        key: 'expression',
        render: renderExpression
      },
      {
        title: 'Description',
        dataIndex: 'description',
        key: 'description',
        ellipsis: true,
        render: description => <span>{description || '-'}</span>
      }
    ],
    []
  )

  const relationshipColumns = useMemo(
    () => [
      {
        title: 'Relationship Name',
        dataIndex: 'name',
        key: 'name',
        width: 220,
        ellipsis: true,
        sorter: (a, b) => a.name.toLowerCase().localeCompare(b.name.toLowerCase())
      },
      { title: 'From', dataIndex: 'from', key: 'from', width: 160, ellipsis: true },
      {
        title: 'From Columns',
        dataIndex: 'fromColumns',
        key: 'fromColumns',
        width: 200,
        ellipsis: true,
        render: renderColumns
      },
      { title: 'To', dataIndex: 'to', key: 'to', width: 160, ellipsis: true },
      {
        title: 'To Columns',
        dataIndex: 'toColumns',
        key: 'toColumns',
        ellipsis: true,
        render: renderColumns
      }
    ],
    []
  )

  const metricColumns = useMemo(
    () => [
      {
        title: 'Metric Name',
        dataIndex: 'name',
        key: 'name',
        width: 200,
        ellipsis: true,
        sorter: (a, b) => a.name.toLowerCase().localeCompare(b.name.toLowerCase())
      },
      {
        title: 'Data Type',
        dataIndex: 'datatype',
        key: 'datatype',
        width: 140,
        render: datatype => (datatype ? <Tag>{datatype}</Tag> : <span>-</span>)
      },
      {
        title: 'Expression',
        dataIndex: 'expression',
        key: 'expression',
        render: renderExpression
      },
      {
        title: 'Description',
        dataIndex: 'description',
        key: 'description',
        ellipsis: true,
        render: description => <span>{description || '-'}</span>
      }
    ],
    []
  )

  const tabOptions = [
    { label: 'Datasets', key: 'Datasets' },
    { label: 'Relationships', key: 'Relationships' },
    { label: 'Metrics', key: 'Metrics' },
    { label: 'Definition', key: 'Definition' }
  ]

  const {
    resizableColumns: datasetResizableColumns,
    components: datasetComponents,
    tableWidth: datasetTableWidth
  } = useAntdColumnResize(() => {
    return { columns: datasetColumns, minWidth: 100 }
  }, [datasetColumns])

  const {
    resizableColumns: relationshipResizableColumns,
    components: relationshipComponents,
    tableWidth: relationshipTableWidth
  } = useAntdColumnResize(() => {
    return { columns: relationshipColumns, minWidth: 100 }
  }, [relationshipColumns])

  const {
    resizableColumns: metricResizableColumns,
    components: metricComponents,
    tableWidth: metricTableWidth
  } = useAntdColumnResize(() => {
    return { columns: metricColumns, minWidth: 100 }
  }, [metricColumns])

  const expandedFieldsRow = dataset => (
    <Table
      data-refer='semantic-model-fields-grid'
      size='small'
      rowKey={field => field.name}
      dataSource={dataset.fields || []}
      columns={fieldColumns}
      pagination={false}
    />
  )

  return (
    <>
      <Spin spinning={store.activatedDetailsLoading}>
        <Flex className='mb-2' gap='small' align='flex-start' ref={ref}>
          <div className='size-8'>
            <Icons.iconify icon='mdi:graph-outline' className='my-icon-large' />
          </div>
          <div className='grow-1 relative bottom-1'>
            <Title level={3} style={{ marginBottom: '0.125rem' }}>
              <span
                title={semanticModelName}
                className='min-w-10 truncate'
                style={{ maxWidth: `calc(${width}px - 56px)`, display: 'inherit' }}
              >
                {decodeURIComponent(semanticModelName || '')}
              </span>
            </Title>
            <Paragraph
              type='secondary'
              className='truncate'
              title={semanticModel?.comment}
              style={{ marginBottom: 0, maxWidth: `calc(${width}px - 56px)` }}
            >
              {semanticModel?.comment}
            </Paragraph>
          </div>
        </Flex>
        <Space split={<Divider type='vertical' />} wrap={true} className='mb-2'>
          {anthEnable && (
            <Space size={4}>
              <Tooltip title='Owned' placement='top'>
                <Icons.User className='size-4' color='grey' />
              </Tooltip>
              <GetOwner
                metalake={metalake}
                metadataObjectType={'semantic_model'}
                metadataObjectFullName={metadataObjectFullName}
              />
            </Space>
          )}
          {semanticModel?.audit?.creator && (
            <Space size={4}>
              <Tooltip title='Creator' placement='top'>
                <Icons.UserPen className='size-4' color='grey' />
              </Tooltip>
              <span>{semanticModel.audit.creator}</span>
            </Space>
          )}
          <Space size={4}>
            <Tooltip title='Created' placement='top'>
              <Icons.iconify icon='mdi:clock-outline' className='size-4' props={{ color: 'grey' }} />
            </Tooltip>
            <span>{createdAtText}</span>
          </Space>
          <Space size={4}>
            <Tooltip title='Tags' placement='top'>
              <Icons.Tags className='size-4' color='grey' />
            </Tooltip>
            {store.currentEntityTags && store.currentEntityTags?.length > 0 ? (
              <Popover placement='bottom' title={<span>Tags</span>} content={tagContent}>
                <a className='text-defaultPrimary'>{store.currentEntityTags?.length}</a>
              </Popover>
            ) : (
              <a className='text-defaultPrimary'>0</a>
            )}
          </Space>
          <Space size={4}>
            <Tooltip title='Properties' placement='top'>
              <Icons.TableProperties className='size-4' color='grey' />
            </Tooltip>
            {properties && Object.keys(properties).length > 0 ? (
              <Popover placement='bottom' title={<span>Properties</span>} content={propertyContent}>
                <a className='text-defaultPrimary'>{Object.keys(properties).length}</a>
              </Popover>
            ) : (
              <a className='text-defaultPrimary'>0</a>
            )}
          </Space>
        </Space>
      </Spin>
      <Tabs data-refer='details-tabs' activeKey={tabKey} onChange={setTabKey} items={tabOptions} />
      <Spin spinning={store.activatedDetailsLoading}>
        {tabKey === 'Datasets' && (
          <Table
            data-refer='semantic-model-datasets-grid'
            size='small'
            style={{ maxHeight: 'calc(100vh - 30rem)' }}
            scroll={{ x: datasetTableWidth, y: 'calc(100vh - 37rem)' }}
            dataSource={datasets}
            columns={datasetResizableColumns}
            components={datasetComponents}
            expandable={{
              expandedRowRender: expandedFieldsRow,
              rowExpandable: dataset => (dataset.fields || []).length > 0
            }}
            pagination={{ position: ['bottomCenter'], showSizeChanger: true }}
          />
        )}
        {tabKey === 'Relationships' && (
          <Table
            data-refer='semantic-model-relationships-grid'
            size='small'
            style={{ maxHeight: 'calc(100vh - 30rem)' }}
            scroll={{ x: relationshipTableWidth, y: 'calc(100vh - 37rem)' }}
            dataSource={relationships}
            columns={relationshipResizableColumns}
            components={relationshipComponents}
            pagination={{ position: ['bottomCenter'], showSizeChanger: true }}
          />
        )}
        {tabKey === 'Metrics' && (
          <Table
            data-refer='semantic-model-metrics-grid'
            size='small'
            style={{ maxHeight: 'calc(100vh - 30rem)' }}
            scroll={{ x: metricTableWidth, y: 'calc(100vh - 37rem)' }}
            dataSource={metrics}
            columns={metricResizableColumns}
            components={metricComponents}
            pagination={{ position: ['bottomCenter'], showSizeChanger: true }}
          />
        )}
        {tabKey === 'Definition' &&
          (definitionText ? (
            <div className='relative rounded border border-borderColor p-3'>
              <Tooltip title='Copy definition' placement='top'>
                <button
                  type='button'
                  className='absolute right-3 top-3 inline-flex cursor-pointer items-center rounded border border-borderColor bg-transparent p-1 text-textSecondary transition-colors hover:text-defaultPrimary'
                  onClick={onCopyDefinition}
                >
                  <Icons.Copy className='size-4' />
                </button>
              </Tooltip>
              <pre
                data-refer='semantic-model-definition'
                className='m-0 overflow-auto whitespace-pre-wrap break-all font-mono text-sm leading-6'
                style={{ maxHeight: 'calc(100vh - 34rem)' }}
              >
                {definitionText}
              </pre>
            </div>
          ) : (
            <Paragraph type='secondary'>No definition available.</Paragraph>
          ))}
      </Spin>
    </>
  )
}
