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

import React, { useEffect, useState } from 'react'
import { useRouter } from 'next/navigation'
import { LockOutlined } from '@ant-design/icons'
import { Button, Flex, Spin, Tag, Tooltip, theme } from 'antd'
import { useAppDispatch } from '@/lib/hooks/useStore'
import { getCurrentEntityPolicies } from '@/lib/store/metalakes'
import { useSearchParams } from 'next/navigation'
import { useMetadataObjectList } from './MetadataObjectListsProvider'

export default function PolicyTag({ ...props }) {
  const { metadataObjectType, metadataObjectFullName } = props
  const { token } = theme.useToken()
  const router = useRouter()
  const dispatch = useAppDispatch()
  const [loadedPolicies, setPoliciesForEntity] = useState([])
  const list = useMetadataObjectList('policies', metadataObjectType, metadataObjectFullName)
  const bulk = list !== null
  const policiesForEntity = list?.data?.policies || loadedPolicies
  const searchParams = useSearchParams()
  const currentMetalake = searchParams.get('metalake')

  const getPoliciesForEntity = async () => {
    const { payload } = await dispatch(
      getCurrentEntityPolicies({ metalake: currentMetalake, metadataObjectType, metadataObjectFullName, details: true })
    )

    return payload?.policies || []
  }

  useEffect(() => {
    const initLoad = async () => {
      const policies = await getPoliciesForEntity()
      setPoliciesForEntity(policies)
    }
    if (!bulk && currentMetalake && metadataObjectType && metadataObjectFullName) {
      initLoad()
    }
  }, [bulk, currentMetalake, metadataObjectType, metadataObjectFullName])

  const handleClick = name => () => {
    router.push(
      `/metadataObjectsForPolicy?policy=${encodeURIComponent(name)}&metalake=${encodeURIComponent(currentMetalake)}`
    )
  }

  if (bulk && !list.data) {
    return list.error ? (
      <span>
        Unable to load policies.{' '}
        <Button type='link' size='small' onClick={list.retry}>
          Retry
        </Button>
      </span>
    ) : (
      <Spin size='small' aria-label='Loading policies' />
    )
  }

  return (
    <Flex gap='4px 0' wrap>
      {policiesForEntity?.map((policy, index) => {
        const isLongPolicy = policy.name.length > 20
        let color = policy.enabled ? token.colorPrimary : token.colorTextDisabled

        const policyElem = (
          <Tag
            key={policy.name + index}
            color={color}
            icon={policy.inherited ? <LockOutlined /> : null}
            onClick={handleClick(policy.name)}
            className='cursor-pointer'
            style={{ userSelect: 'none' }}
          >
            <span>{isLongPolicy ? `${policy.name.slice(0, 20)}...` : policy.name}</span>
          </Tag>
        )

        return isLongPolicy ? (
          <Tooltip title={policy.name} key={policy.name}>
            {policyElem}
          </Tooltip>
        ) : (
          policyElem
        )
      })}
      {policiesForEntity?.length === 0 && (
        <Tooltip title='Policies are applied through tags.'>
          <span>No Policies</span>
        </Tooltip>
      )}
    </Flex>
  )
}
