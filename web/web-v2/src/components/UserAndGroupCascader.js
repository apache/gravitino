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
import { Cascader, Form } from 'antd'
import { fetchUsers } from '@/lib/store/users'
import { fetchUserGroups } from '@/lib/store/userGroups'
import { useAppDispatch } from '@/lib/hooks/useStore'

export default function UserAndGroupCascader({ ...props }) {
  const { cascaderOwnerRef, metalake, multiple, form, data } = props
  const dispatch = useAppDispatch()

  const [options, setOptions] = useState([
    {
      value: 'user',
      label: (
        <div title='User' className='inline-block w-full truncate'>
          User
        </div>
      ),
      key: 'user',
      type: 'user',
      isLeaf: false
    },
    {
      value: 'group',
      label: (
        <div title='Group' className='inline-block w-full truncate'>
          Group
        </div>
      ),
      key: 'group',
      type: 'group',
      isLeaf: false
    }
  ])

  useEffect(() => {
    if (data && cascaderOwnerRef.current) {
      form.setFieldValue('name', [data?.type, data?.name])
    }
  }, [data])

  const loadData = selectedOptions =>
    new Promise(async resolve => {
      const targetOption = selectedOptions[selectedOptions.length - 1]
      const type = targetOption?.value
      let optionData = []
      switch (type) {
        case 'user':
          const { payload } = await dispatch(fetchUsers({ metalake }))
          optionData = payload?.userRes?.names?.map(user => ({
            label: (
              <div title={user} className='inline-block w-full truncate'>
                {user}
              </div>
            ),
            value: user,
            key: user,
            type: 'user',
            isLeaf: true
          }))
          break
        case 'group':
          const { payload: groupPayload } = await dispatch(fetchUserGroups({ metalake }))
          optionData = groupPayload?.userGroupsRes?.names?.map(group => ({
            label: (
              <div title={group} className='inline-block w-full truncate'>
                {group}
              </div>
            ),
            value: group,
            key: group,
            type: 'group',
            isLeaf: true
          }))
          break
        default:
          break
      }
      targetOption.children = optionData || []
      setOptions([...options])

      resolve()
    })

  const displayRender = labels => {
    let displayLabels = labels.slice(-1).map((label, index) => (
      <div
        title={label?.props?.title}
        key={index}
        className='relative top-[3px] inline-block max-w-full overflow-hidden truncate'
      >
        {label}
      </div>
    ))

    return displayLabels
  }

  return (
    <Form.Item noStyle name={['name']} label='Owner'>
      <Cascader
        size='small'
        options={options}
        loadData={loadData}
        multiple={multiple}
        maxTagCount='responsive'
        displayRender={displayRender}
        getPopupContainer={() => cascaderOwnerRef.current}
        className='w-full'
      />
    </Form.Item>
  )
}
