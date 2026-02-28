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

import { useEffect, useState } from 'react'
import { Collapse, Descriptions, Space, Spin, Tag, Typography, Flex, Tooltip, Divider, Tabs } from 'antd'
import { useSearchParams } from 'next/navigation'
import { formatToDateTime, isValidDate } from '@/lib/utils/date'
import Icons from '@/components/Icons'
import { useAppSelector } from '@/lib/hooks/useStore'

const { Title, Paragraph } = Typography

const formatType = type => {
  if (!type) return '-'
  if (typeof type === 'string') return type
  if (type?.type === 'unparsed') return type.unparsedType

  return JSON.stringify(type)
}

const formatDefaultValue = defaultValue => {
  if (defaultValue == null) return ''

  if (typeof defaultValue === 'string' || typeof defaultValue === 'number' || typeof defaultValue === 'boolean') {
    return `${defaultValue}`
  }

  if (typeof defaultValue === 'object') {
    if (Object.prototype.hasOwnProperty.call(defaultValue, 'value')) {
      return `${defaultValue.value}`
    }

    return JSON.stringify(defaultValue)
  }

  return ''
}

const renderParameters = parameters => {
  if (!parameters?.length) return null

  return parameters.map((param, index) => {
    const comment = typeof param?.comment === 'string' ? param.comment.trim() : ''
    const defaultText = formatDefaultValue(param?.defaultValue)

    return (
      <span key={`${param.name || 'param'}-${index}`}>
        <span>{param.name || '-'}</span>
        {comment && (
          <Tooltip title={comment}>
            <span className='mx-1 inline-flex cursor-help text-slate-400 align-middle'>
              <Icons.iconify icon='material-symbols:info-outline' />
            </span>
          </Tooltip>
        )}
        <span>{`: ${formatType(param.dataType)}${defaultText ? ` = ${defaultText}` : ''}`}</span>
        {index < parameters.length - 1 && <span>, </span>}
      </span>
    )
  })
}

const formatReturnColumns = returnColumns => {
  if (!returnColumns?.length) return ''

  return returnColumns.map(col => `${col.name}: ${formatType(col.dataType)}`).join(', ')
}

const buildSignature = (name, definition) => {
  const params = renderParameters(definition?.parameters)
  const returnType = formatType(definition?.returnType)
  const returnColumns = formatReturnColumns(definition?.returnColumns)
  const returnText = returnColumns || returnType || '-'

  return (
    <span className='font-mono'>
      <span>{`${name || '-'}(`}</span>
      {params}
      <span>{`) => ${returnText}`}</span>
    </span>
  )
}

const buildImplDetails = impl => {
  const details = [
    { label: 'Language', value: impl?.language || '-' },
    { label: 'Runtime', value: impl?.runtime || '-' }
  ]

  if (impl?.sql) {
    details.push({ label: 'SQL', value: impl.sql })
  }
  if (impl?.className) {
    details.push({ label: 'Class Name', value: impl.className })
  }
  if (impl?.handler) {
    details.push({ label: 'Handler', value: impl.handler })
  }
  if (impl?.codeBlock) {
    details.push({ label: 'Code Block', value: impl.codeBlock })
  }
  if (impl?.resources) {
    const { jars = [], files = [], archives = [] } = impl.resources || {}
    details.push({ label: 'Resources (Jars)', value: jars.length ? jars.join(', ') : '-' })
    details.push({ label: 'Resources (Files)', value: files.length ? files.join(', ') : '-' })
    details.push({ label: 'Resources (Archives)', value: archives.length ? archives.join(', ') : '-' })
  }
  if (impl?.properties) {
    details.push({
      label: 'Properties',
      value: Object.keys(impl.properties).length ? JSON.stringify(impl.properties) : '-'
    })
  }

  return details
}

const FunctionImplTabs = ({ impls }) => {
  const [activeKey, setActiveKey] = useState(impls[0]?.runtime || 'runtime-0')

  useEffect(() => {
    setActiveKey(impls[0]?.runtime || 'runtime-0')
  }, [impls])

  const tabItems = impls.map((impl, implIndex) => {
    const runtimeKey = impl?.runtime || `runtime-${implIndex}`
    const tabLabel = impl?.runtime || `Runtime ${implIndex + 1}`

    return {
      key: runtimeKey,
      label: tabLabel
    }
  })

  const activeImpl = impls.find((impl, index) => {
    const key = impl?.runtime || `runtime-${index}`

    return key === activeKey
  })
  const activeDetails = activeImpl ? buildImplDetails(activeImpl) : []

  return (
    <div className='flex flex-col gap-2 w-full'>
      <Tabs size='small' className='w-full' items={tabItems} activeKey={activeKey} onChange={setActiveKey} />
      <Descriptions
        layout='horizontal'
        column={1}
        size='small'
        bordered
        style={{ width: '100%' }}
        labelStyle={{ width: '40%' }}
      >
        {activeDetails.map(detail => (
          <Descriptions.Item key={`${activeKey}-${detail.label}`} label={detail.label}>
            {detail.value}
          </Descriptions.Item>
        ))}
      </Descriptions>
    </div>
  )
}

export default function FunctionDetailsPage() {
  const searchParams = useSearchParams()
  const functionName = searchParams.get('function')
  const store = useAppSelector(state => state.metalakes)
  const functionData = store.activatedDetails
  const [activeDefinitionKeys, setActiveDefinitionKeys] = useState([])

  useEffect(() => {
    setActiveDefinitionKeys([])
  }, [functionName])

  const definitions = functionData?.definitions || []
  const createdAt = functionData?.audit?.createTime
  const createdAtText = !createdAt || !isValidDate(createdAt) ? '-' : formatToDateTime(createdAt)

  const definitionItems = definitions.map((definition, index) => {
    const signature = buildSignature(functionData?.name, definition)
    const impls = definition?.impls || []

    return {
      key: `definition-${index}`,
      label: (
        <div className='flex flex-col gap-1'>
          <div className='font-mono truncate'>{signature}</div>
        </div>
      ),
      children: (
        <div className='flex flex-col gap-2 w-full'>
          {impls.length === 0 ? (
            <span className='text-slate-400'>No implementations</span>
          ) : (
            <FunctionImplTabs impls={impls} />
          )}
        </div>
      )
    }
  })

  return (
    <Spin spinning={store.activatedDetailsLoading}>
      <div className='bg-white min-h-[calc(100vh-24rem)]'>
        <Space direction='vertical' size='small' style={{ width: '100%' }}>
          <Flex className='mb-2' gap='small' align='flex-start'>
            <div className='size-8'>
              <Icons.iconify icon='material-symbols:function' className='my-icon-large' />
            </div>
            <div className='grow-1 relative bottom-1'>
              <Title level={3} style={{ marginBottom: '0.125rem' }}>
                {functionData?.name || '-'}
              </Title>
              <Paragraph type='secondary' style={{ marginBottom: 0 }}>
                {functionData?.comment || '-'}
              </Paragraph>
            </div>
          </Flex>
          <Space split={<Divider type='vertical' />} wrap={true} className='mb-2'>
            <Space size={4}>
              <Tooltip title='Type'>
                <Icons.Type className='size-4' color='grey' />
              </Tooltip>
              <span>{functionData?.functionType || '-'}</span>
            </Space>
            <Space size={4}>
              <Tooltip title='Deterministic'>
                <Icons.CircleCheckBig className='size-4' color='grey' />
              </Tooltip>
              {typeof functionData?.deterministic === 'boolean' ? (
                <Tag color={functionData?.deterministic ? 'green' : 'orange'}>
                  {functionData?.deterministic ? 'true' : 'false'}
                </Tag>
              ) : (
                <span>-</span>
              )}
            </Space>
            <Space size={4}>
              <Tooltip title='Creator'>
                <Icons.User className='size-4' color='grey' />
              </Tooltip>
              <span>{functionData?.audit?.creator || '-'}</span>
            </Space>
            <Space size={4}>
              <Tooltip title='Created At'>
                <Icons.Clock className='size-4' color='grey' />
              </Tooltip>
              <span>{createdAtText}</span>
            </Space>
          </Space>
          <div className='max-h-[60vh] overflow-auto'>
            <Tabs
              data-refer='details-tabs'
              defaultActiveKey={'Definitions'}
              items={[{ label: 'Definitions', key: 'Definitions' }]}
            />
            {definitions.length === 0 ? (
              <span className='text-slate-400'>No definitions</span>
            ) : (
              <Collapse
                items={definitionItems}
                activeKey={activeDefinitionKeys}
                onChange={keys => setActiveDefinitionKeys(Array.isArray(keys) ? keys : [keys])}
              />
            )}
          </div>
        </Space>
      </div>
    </Spin>
  )
}
