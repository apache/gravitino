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

import { Dropdown, Modal, Tooltip } from 'antd'
import Icons from '@/components/Icons'

export default function TableActions({ ...props }) {
  const { catalogType, provider, anthEnable, handleEdit, showDeleteConfirm, handleSetOwner } = props
  const [modal, contextHolder] = Modal.useModal()

  return (
    <div className='flex gap-2'>
      {contextHolder}
      <a>
        <Icons.Pencil className='size-4' onClick={() => handleEdit()} />
      </a>
      {provider !== 'lakehouse-paimon' && (
        <a data-refer={`delete-entity-${props.name}`}>
          <Icons.Trash2Icon className='size-4' onClick={() => showDeleteConfirm(modal)} />
        </a>
      )}
      {catalogType !== 'model' && anthEnable && (
        <Dropdown
          menu={{
            items: [
              {
                label: 'Set Owner',
                key: 'setOwner'
              }
            ],
            onClick: ({ key }) => {
              switch (key) {
                case 'setOwner':
                  handleSetOwner()
                  break
              }
            }
          }}
          trigger={['hover']}
        >
          <Tooltip title='Settings'>
            <a onClick={e => e.preventDefault()}>
              <Icons.Settings className='size-4' />
            </a>
          </Tooltip>
        </Dropdown>
      )}
    </div>
  )
}
