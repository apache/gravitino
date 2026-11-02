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

import { catalogEntityKey } from '@/lib/utils/catalogEntity'
import { useEffect, useMemo, useRef, useState } from 'react'
import {
  Button,
  Divider,
  Flex,
  Input,
  Modal,
  Popover,
  Space,
  Spin,
  Table,
  Tabs,
  Tag,
  Tooltip,
  Typography,
  message
} from 'antd'
import styles from './SemanticModelDetailsPage.module.css'
import { useAntdColumnResize } from 'react-antd-column-resize'
import { useRouter, useSearchParams } from 'next/navigation'
import Tags from '@/components/CustomTags'
import GetOwner from '@/components/GetOwner'
import Icons from '@/components/Icons'
import PropertiesContent from '@/components/PropertiesContent'
import { useAppDispatch, useAppSelector } from '@/lib/hooks/useStore'
import { getSemanticModelDetails } from '@/lib/store/metalakes'
import { copyToClipboard } from '@/lib/utils'
import { formatToDateTime, isValidDate } from '@/lib/utils/date'
import { formatSemanticModelDefinition } from '@/lib/utils/semanticModel'
import { ColumnTypeColorEnum } from '@/config'
import SemanticModelExpression from './SemanticModelExpression'
import SemanticModelAIContext from './SemanticModelAIContext'
import SemanticModelExport from './SemanticModelExport'
import SemanticModelExtensions from './SemanticModelExtensions'
import CreateSemanticModelDialog from '../CreateSemanticModelDialog'
import SemanticMetadataEditor from '../SemanticMetadataEditor'

const { Title, Paragraph } = Typography

const sourceFullName = source => {
  if (!source) {
    return '-'
  }

  return [...(source.namespace || []), source.name].filter(Boolean).join('.')
}

const renderDataType = datatype => {
  if (!datatype) return <span>-</span>
  const type = { DateTime: 'timestamp', DateTimeTz: 'timestamp_tz' }[datatype] || datatype.toLowerCase()

  return <Tag color={ColumnTypeColorEnum[type] || undefined}>{datatype}</Tag>
}

const topAlignedColumns = columns =>
  columns.map(column => ({ ...column, onCell: () => ({ style: { verticalAlign: 'top' } }) }))

const renderColumns = columns => <span className='font-mono text-xs'>{(columns || []).join(', ') || '-'}</span>

const hasMetadata = entity => !!entity.aiContext || entity.customExtensions?.length > 0

export default function SemanticModelDetailsPage() {
  const searchParams = useSearchParams()
  const router = useRouter()
  const dispatch = useAppDispatch()
  const metalake = searchParams.get('metalake')
  const catalog = searchParams.get('catalog')
  const schema = searchParams.get('schema')
  const semanticModelName = searchParams.get('semanticModel')
  const auth = useAppSelector(state => state.auth)
  const { anthEnable } = auth
  const store = useAppSelector(state => state.metalakes)
  const detailsKey = catalogEntityKey({ metalake, catalog, schema, semanticModel: semanticModelName })
  const semanticModel = store.activatedDetailsKey === detailsKey ? store.activatedDetails : null
  const [tabKey, setTabKey] = useState('Datasets')
  const [openEdit, setOpenEdit] = useState(false)
  const [metadataEdit, setMetadataEdit] = useState(null)
  const [fieldContext, setFieldContext] = useState(null)
  const [metadataDetails, setMetadataDetails] = useState(null)
  const [expandedDefinition, setExpandedDefinition] = useState(false)
  const [fieldSearch, setFieldSearch] = useState('')
  const [metricSearch, setMetricSearch] = useState('')
  const [expandedDatasets, setExpandedDatasets] = useState([])

  const mounted = useRef(true)
  useEffect(() => {
    mounted.current = true

    return () => {
      mounted.current = false
    }
  }, [])

  const definition = semanticModel?.definition
  useEffect(() => {
    if (definition) return
    setMetadataEdit(null)
    setFieldContext(null)
    setMetadataDetails(null)
    setOpenEdit(false)
    setExpandedDefinition(false)
  }, [definition])
  const properties = semanticModel?.properties
  const createdAt = semanticModel?.audit?.createTime
  const createdAtText = !createdAt || !isValidDate(createdAt) ? '-' : formatToDateTime(createdAt)
  const lastModifiedAt = semanticModel?.audit?.lastModifiedTime
  const lastModifiedText = !lastModifiedAt || !isValidDate(lastModifiedAt) ? '-' : formatToDateTime(lastModifiedAt)
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

  const fieldQuery = fieldSearch.trim().toLowerCase()

  const filteredDatasets = useMemo(
    () =>
      fieldQuery
        ? datasets
            .map(dataset => ({
              ...dataset,
              fields: (dataset.fields || []).filter(field =>
                [field.name, field.label].some(value => value?.toLowerCase().includes(fieldQuery))
              )
            }))
            .filter(dataset => dataset.fields.length)
        : datasets,
    [datasets, fieldQuery]
  )

  const filteredMetrics = useMemo(
    () => metrics.filter(metric => metric.name.toLowerCase().includes(metricSearch.trim().toLowerCase())),
    [metrics, metricSearch]
  )

  const renderMetadata = (entity, label, target) => (
    <>
      <SemanticModelAIContext
        context={entity?.aiContext}
        label={`${label} AI Context`}
        onEdit={
          target
            ? () => {
                setMetadataDetails(null)
                setMetadataEdit({ ...target, section: 'context' })
              }
            : undefined
        }
      />
      <SemanticModelExtensions extensions={entity?.customExtensions} label={`${label} Vendor Extensions`} />
    </>
  )

  const definitionText = useMemo(() => formatSemanticModelDefinition(definition), [definition])

  const tagContent = (
    <div>
      <Tags readOnly={true} metadataObjectType={'semantic_model'} metadataObjectFullName={metadataObjectFullName} />
    </div>
  )

  const propertyContent = <PropertiesContent properties={properties} />

  const onSaved = async updated => {
    if (!mounted.current) return
    if (updated.name !== semanticModelName) {
      const params = new URLSearchParams(searchParams.toString())
      params.set('semanticModel', updated.name)
      router.replace(`/catalogs?${params.toString()}`)
    } else {
      await dispatch(getSemanticModelDetails({ metalake, catalog, schema, semanticModel: updated.name, init: true }))
    }
  }

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
        fixed: 'left',
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
        title: 'Unique Keys',
        dataIndex: 'uniqueKeys',
        key: 'uniqueKeys',
        width: 220,
        render: keys =>
          keys?.length ? (
            <Space direction='vertical' size={4}>
              {keys.map((columns, index) => (
                <Tag key={index}>{renderColumns(columns)}</Tag>
              ))}
            </Space>
          ) : (
            <span>-</span>
          )
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
      },
      {
        title: 'Actions',
        key: 'actions',
        fixed: 'right',
        width: 90,
        render: (_, record) => (
          <Button
            size='small'
            aria-label={`Edit dataset ${record.name}`}
            onClick={() => setMetadataEdit({ kind: 'dataset', name: record.name })}
          >
            Edit
          </Button>
        )
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
        fixed: 'left',
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
      },
      {
        title: 'Actions',
        key: 'actions',
        fixed: 'right',
        width: 90,
        render: (_, record) => (
          <Button
            size='small'
            aria-label={`Edit relationship ${record.name}`}
            onClick={() => setMetadataEdit({ kind: 'relationship', name: record.name })}
          >
            Edit
          </Button>
        )
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
        fixed: 'left',
        width: 200,
        ellipsis: true,
        sorter: (a, b) => a.name.toLowerCase().localeCompare(b.name.toLowerCase())
      },
      {
        title: 'Data Type',
        dataIndex: 'datatype',
        key: 'datatype',
        width: 140,
        render: renderDataType
      },
      {
        title: 'Expression',
        dataIndex: 'expression',
        key: 'expression',
        render: (expression, record) => <SemanticModelExpression expression={expression} name={record.name} />
      },
      {
        title: 'Description',
        dataIndex: 'description',
        key: 'description',
        ellipsis: true,
        render: description => <span>{description || '-'}</span>
      },
      {
        title: 'Actions',
        key: 'actions',
        fixed: 'right',
        width: 90,
        render: (_, record) => (
          <Button
            size='small'
            aria-label={`Edit metric ${record.name}`}
            onClick={() => setMetadataEdit({ kind: 'metric', name: record.name })}
          >
            Edit
          </Button>
        )
      }
    ],
    []
  )

  const tabOptions = [
    { label: 'Overview', key: 'Overview' },
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
    return { columns: topAlignedColumns(datasetColumns), minWidth: 100 }
  }, [datasetColumns])

  const {
    resizableColumns: relationshipResizableColumns,
    components: relationshipComponents,
    tableWidth: relationshipTableWidth
  } = useAntdColumnResize(() => {
    return { columns: topAlignedColumns(relationshipColumns), minWidth: 100 }
  }, [relationshipColumns])

  const {
    resizableColumns: metricResizableColumns,
    components: metricComponents,
    tableWidth: metricTableWidth
  } = useAntdColumnResize(() => {
    return { columns: topAlignedColumns(metricColumns), minWidth: 100 }
  }, [metricColumns])

  const expandedFieldsRow = dataset => (
    <section aria-label={`${dataset.name} fields`} className='py-2'>
      <div className='mb-3 flex items-center justify-between gap-3'>
        <Typography.Text strong>Fields ({(dataset.fields || []).length})</Typography.Text>
        {hasMetadata(dataset) && (
          <Button
            size='small'
            onClick={() =>
              setMetadataDetails({
                entity: dataset,
                label: dataset.name,
                target: { kind: 'dataset', name: dataset.name }
              })
            }
          >
            View dataset details
          </Button>
        )}
      </div>
      <div data-refer='semantic-model-fields-grid' className='divide-y divide-borderColor'>
        {(dataset.fields || []).map(field => (
          <section key={field.name} aria-label={`Field ${field.name}`} className='py-3'>
            <div className='mb-2 flex items-start justify-between gap-3'>
              <div className='min-w-0 break-words'>
                <Typography.Text strong>{field.name}</Typography.Text>
                {field.label && (
                  <Typography.Text type='secondary' className='ml-2'>
                    {field.label}
                  </Typography.Text>
                )}
              </div>
              <Button
                size='small'
                aria-label={`Edit field ${field.name}`}
                onClick={() => setMetadataEdit({ kind: 'field', name: field.name, datasetName: dataset.name })}
              >
                Edit
              </Button>
            </div>
            <div className='grid grid-cols-1 gap-3 sm:grid-cols-2'>
              <div className='min-w-0'>
                <div className='mb-2'>
                  {renderDataType(field.datatype)}
                  <Typography.Text type='secondary'>
                    Time dimension: {field.dimension?.isTime ? 'Yes' : 'No'}
                  </Typography.Text>
                </div>
                {field.description && <Paragraph className='break-words'>{field.description}</Paragraph>}
                <Space wrap>
                  {field.aiContext &&
                    (typeof field.aiContext === 'string' || Object.keys(field.aiContext).length > 0) && (
                      <Button
                        size='small'
                        aria-label={`${field.name} AI Context`}
                        onClick={() => setFieldContext({ ...field, datasetName: dataset.name })}
                      >
                        AI Context
                      </Button>
                    )}
                  {field.customExtensions?.length > 0 && (
                    <Button
                      size='small'
                      aria-label={`${field.name} Vendor Extensions`}
                      onClick={() =>
                        setMetadataDetails({
                          entity: field,
                          label: field.name,
                          target: { kind: 'field', name: field.name, datasetName: dataset.name }
                        })
                      }
                    >
                      Vendor Extensions
                    </Button>
                  )}
                </Space>
              </div>
              <SemanticModelExpression expression={field.expression} name={field.name} />
            </div>
          </section>
        ))}
      </div>
    </section>
  )

  return (
    <div className={styles.page} data-refer='semantic-model-details'>
      <Spin spinning={store.activatedDetailsLoading}>
        <Flex className='mb-2' gap='small' align='flex-start'>
          <div className='size-8'>
            <Icons.iconify icon='mdi:graph-outline' className='my-icon-large' />
          </div>
          <div className='relative bottom-1 min-w-0 flex-1'>
            <Title level={3} style={{ marginBottom: '0.125rem' }}>
              <div className='flex min-w-0 items-center gap-2'>
                <span title={semanticModelName} className='min-w-0 truncate'>
                  {semanticModelName || ''}
                </span>
                <Tooltip title='Edit'>
                  <Button
                    type='text'
                    size='small'
                    aria-label='Edit Semantic Model'
                    icon={<Icons.Pencil className='size-4' />}
                    disabled={store.activatedDetailsLoading || !definition}
                    onClick={() => setOpenEdit(true)}
                  />
                </Tooltip>
              </div>
            </Title>
            <Paragraph type='secondary' className='truncate' title={semanticModel?.comment} style={{ marginBottom: 0 }}>
              {semanticModel?.comment}
            </Paragraph>
          </div>
          <div className='ml-auto shrink-0'>
            <SemanticModelExport
              key={metadataObjectFullName}
              metalake={metalake}
              catalog={catalog}
              schema={schema}
              semanticModel={semanticModelName}
              disabled={store.activatedDetailsLoading || !definition}
            />
          </div>
        </Flex>
      </Spin>
      <Tabs data-refer='details-tabs' activeKey={tabKey} onChange={setTabKey} items={tabOptions} />
      <Spin wrapperClassName={styles.body} spinning={store.activatedDetailsLoading}>
        {tabKey === 'Overview' && (
          <div className='min-h-0 overflow-auto'>
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
                  <span className='inline-flex'>
                    <Icons.iconify icon='mdi:clock-outline' className='size-4' props={{ color: 'grey' }} />
                  </span>
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
              <Space size={4} data-refer='semantic-model-policies'>
                <span>Policies</span>
                {store.currentEntityPolicies?.length > 0 ? (
                  <Popover
                    placement='bottom'
                    title='Policies'
                    content={
                      <Space direction='vertical' size={4} className='max-w-sm'>
                        {store.currentEntityPolicies.map(policy => (
                          <Typography.Text key={policy.name} className='break-all'>
                            {policy.name}
                          </Typography.Text>
                        ))}
                      </Space>
                    }
                  >
                    <a className='text-defaultPrimary' aria-label='View Semantic Model policies'>
                      {store.currentEntityPolicies.length}
                    </a>
                  </Popover>
                ) : (
                  <span>0</span>
                )}
              </Space>
              {semanticModel?.audit?.lastModifier && (
                <Space size={4} data-refer='semantic-model-last-modifier'>
                  <Tooltip title='Last modified by'>
                    <Icons.UserPen className='size-4' color='grey' />
                  </Tooltip>
                  <span>{semanticModel.audit.lastModifier}</span>
                </Space>
              )}
              {lastModifiedAt && (
                <Space size={4} data-refer='semantic-model-last-modified-time'>
                  <Tooltip title='Last modified'>
                    <Icons.History className='size-4' color='grey' />
                  </Tooltip>
                  <span>{lastModifiedText}</span>
                </Space>
              )}
            </Space>
            <Button
              className='mb-3 block'
              disabled={!definition}
              onClick={() => setMetadataEdit({ kind: 'model', section: 'context' })}
            >
              Edit Model AI Context
            </Button>
            {renderMetadata(definition, 'Model')}
          </div>
        )}
        {tabKey === 'Datasets' && (
          <>
            <Space className='mb-3 shrink-0'>
              <Input
                className='w-64'
                aria-label='Search fields'
                placeholder='Search field name or label'
                value={fieldSearch}
                onChange={event => setFieldSearch(event.target.value)}
              />
              {fieldSearch && <Button onClick={() => setFieldSearch('')}>Clear field search</Button>}
            </Space>
            <Table
              data-refer='semantic-model-datasets-grid'
              className={styles.table}
              size='small'
              tableLayout='fixed'
              scroll={{ x: datasetTableWidth, y: '100%' }}
              dataSource={filteredDatasets}
              columns={datasetResizableColumns}
              components={datasetComponents}
              expandable={{
                expandedRowRender: expandedFieldsRow,
                expandedRowKeys: fieldQuery ? filteredDatasets.map(dataset => dataset.key) : expandedDatasets,
                onExpandedRowsChange: keys => {
                  if (!fieldQuery) setExpandedDatasets(keys)
                },
                rowExpandable: dataset => (dataset.fields || []).length > 0 || hasMetadata(dataset)
              }}
              pagination={{ position: ['bottomCenter'], showSizeChanger: true }}
            />
          </>
        )}
        {tabKey === 'Relationships' && (
          <Table
            data-refer='semantic-model-relationships-grid'
            className={styles.table}
            size='small'
            tableLayout='fixed'
            scroll={{ x: relationshipTableWidth, y: '100%' }}
            dataSource={relationships}
            columns={relationshipResizableColumns}
            components={relationshipComponents}
            expandable={{
              expandedRowRender: relationship =>
                renderMetadata(relationship, relationship.name, { kind: 'relationship', name: relationship.name }),
              rowExpandable: hasMetadata
            }}
            pagination={{ position: ['bottomCenter'], showSizeChanger: true }}
          />
        )}
        {tabKey === 'Metrics' && (
          <>
            <Space className='mb-3 shrink-0'>
              <Input
                aria-label='Search metrics'
                placeholder='Search metric name'
                value={metricSearch}
                onChange={event => setMetricSearch(event.target.value)}
              />
              {metricSearch && <Button onClick={() => setMetricSearch('')}>Clear metric search</Button>}
            </Space>
            <Table
              data-refer='semantic-model-metrics-grid'
              className={styles.table}
              size='small'
              tableLayout='fixed'
              scroll={{ x: metricTableWidth, y: '100%' }}
              dataSource={filteredMetrics}
              columns={metricResizableColumns}
              components={metricComponents}
              expandable={{
                expandedRowRender: metric => renderMetadata(metric, metric.name, { kind: 'metric', name: metric.name }),
                rowExpandable: hasMetadata
              }}
              pagination={{ position: ['bottomCenter'], showSizeChanger: true }}
            />
          </>
        )}
        {tabKey === 'Definition' &&
          (definitionText ? (
            <div className='flex min-h-0 flex-1 flex-col rounded border border-borderColor p-3'>
              <Space className='mb-3 shrink-0'>
                <Button onClick={() => setOpenEdit(true)}>Edit JSON definition</Button>
                <Tooltip title='Copy definition' placement='top'>
                  <button
                    type='button'
                    aria-label='Copy definition'
                    className='inline-flex cursor-pointer items-center rounded border border-borderColor bg-transparent p-1 text-textSecondary transition-colors hover:text-defaultPrimary'
                    onClick={onCopyDefinition}
                  >
                    <Icons.Copy className='size-4' />
                  </button>
                </Tooltip>
                <Button onClick={() => setExpandedDefinition(true)}>Expand definition</Button>
              </Space>
              <pre
                data-refer='semantic-model-definition'
                className='m-0 min-h-0 flex-1 overflow-auto whitespace-pre-wrap break-all font-mono text-sm leading-6'
              >
                {definitionText}
              </pre>
            </div>
          ) : (
            <Paragraph type='secondary'>No definition available.</Paragraph>
          ))}
      </Spin>
      {expandedDefinition && (
        <Modal
          open
          title={`${semanticModelName} definition`}
          width='100vw'
          style={{ top: 0, maxWidth: '100vw', margin: 0, paddingBottom: 0 }}
          styles={{
            content: { height: '100vh', display: 'flex', flexDirection: 'column', borderRadius: 0 },
            body: { flex: 1, minHeight: 0, display: 'flex', flexDirection: 'column' }
          }}
          footer={null}
          onCancel={() => setExpandedDefinition(false)}
        >
          <Button className='mb-3 self-start' onClick={onCopyDefinition}>
            Copy definition
          </Button>
          <pre className='m-0 min-h-0 flex-1 overflow-auto whitespace-pre-wrap break-all font-mono text-sm leading-6'>
            {definitionText}
          </pre>
        </Modal>
      )}
      {definition && metadataDetails && (
        <Modal
          open
          title={`${metadataDetails.label} details`}
          footer={null}
          width={800}
          style={{ top: 24 }}
          styles={{ body: { maxHeight: 'calc(100vh - 140px)', overflowY: 'auto' } }}
          onCancel={() => setMetadataDetails(null)}
        >
          {renderMetadata(metadataDetails.entity, metadataDetails.label, metadataDetails.target)}
        </Modal>
      )}
      {definition && fieldContext && (
        <Modal
          open
          title={`${fieldContext.name} AI Context`}
          footer={null}
          style={{ top: 24 }}
          styles={{ body: { maxHeight: 'calc(100vh - 140px)', overflowY: 'auto' } }}
          onCancel={() => setFieldContext(null)}
        >
          <SemanticModelAIContext
            context={fieldContext.aiContext}
            inline
            onEdit={() => {
              setMetadataEdit({
                kind: 'field',
                name: fieldContext.name,
                datasetName: fieldContext.datasetName,
                section: 'context'
              })
              setFieldContext(null)
            }}
          />
        </Modal>
      )}
      {definition && metadataEdit && (
        <SemanticMetadataEditor
          definition={definition}
          target={metadataEdit}
          metalake={metalake}
          catalog={catalog}
          schema={schema}
          semanticModel={semanticModelName}
          onClose={() => setMetadataEdit(null)}
          onSaved={onSaved}
        />
      )}
      {openEdit && (
        <CreateSemanticModelDialog
          open={openEdit}
          setOpen={setOpenEdit}
          metalake={metalake}
          catalog={catalog}
          schema={schema}
          editSemanticModel={semanticModelName}
          onSaved={onSaved}
        />
      )}
    </div>
  )
}
