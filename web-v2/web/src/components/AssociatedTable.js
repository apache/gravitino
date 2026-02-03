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

import { useEffect, useState, useMemo } from 'react'
import { Spin, Table, Tag } from 'antd'
import { listRolesForObject, getRoleDetails } from '@/lib/store/roles'
import { useAppDispatch, useAppSelector } from '@/lib/hooks/useStore'
import { useAntdColumnResize } from 'react-antd-column-resize'

export default function AssociatedTable({ ...props }) {
  const { metalake, metadataObjectType, metadataObjectFullName } = props
  const [tableData, setTableData] = useState([])
  const dispatch = useAppDispatch()
  const store = useAppSelector(state => state.roles)

  useEffect(() => {
    dispatch(listRolesForObject({ metalake, metadataObjectType, metadataObjectFullName }))
  }, [metalake, metadataObjectType, metadataObjectFullName])

  useEffect(() => {
    const loadPrivileges = async () => {
      const data = []

      const roleDetails = await Promise.all(
        (store.rolesForObject || []).map(async role => {
          const { payload } = await dispatch(getRoleDetails({ metalake, role }))

          return payload
        })
      )
      roleDetails.forEach(roleData => {
        const privileges = roleData?.securableObjects?.filter(o => o.fullName === metadataObjectFullName)[0]?.privileges
        const allowPrivileges = privileges?.filter(p => p.condition === 'allow').map(p => p.name)
        const denyPrivileges = privileges?.filter(p => p.condition === 'deny').map(p => p.name)
        if (allowPrivileges?.length) {
          data.push({
            key: roleData.name + '-allow',
            role: roleData.name,
            condition: 'allow',
            privileges: allowPrivileges
          })
        }
        if (denyPrivileges?.length) {
          data.push({
            key: roleData.name + '-deny',
            role: roleData.name,
            condition: 'deny',
            privileges: denyPrivileges
          })
        }
      })
      setTableData(data)
    }
    loadPrivileges()
  }, [store.rolesForObject])

  const columns = useMemo(
    () => [
      {
        title: 'Role Name',
        dataIndex: 'role',
        key: 'role',
        width: 200,
        onCell: (_, index) => {
          if (!tableData || index === undefined) return {}

          const currentRole = tableData[index].role

          // Find how many rows have the same role starting from this row
          const rowSpan = tableData
            .slice(index + 1)
            .filter((item, i) => item.role === currentRole && i + index === index).length

          // Check if the current row is the first occurrence of the role
          const isFirstOccurrence = index === 0 || tableData[index - 1].role !== currentRole

          return isFirstOccurrence ? { rowSpan: rowSpan == 1 ? 2 : 1 } : { rowSpan: 0 }
        }
      },
      {
        title: 'Privilege Type',
        dataIndex: 'condition',
        key: 'condition',
        width: 200
      },
      {
        title: 'Privileges',
        dataIndex: 'privileges',
        key: 'privileges',
        render: privileges => (
          <>
            {privileges.map(privilege => {
              return <Tag key={privilege}>{privilege}</Tag>
            })}
          </>
        )
      }
    ],
    [tableData]
  )

  const { resizableColumns, components, tableWidth } = useAntdColumnResize(() => {
    return { columns, minWidth: 100 }
  }, [columns])

  return (
    <Spin spinning={store.rolesForObjectLoading}>
      <Table
        dataSource={tableData}
        columns={resizableColumns}
        components={components}
        style={{ maxHeight: 'calc(100vh - 28rem)' }}
        scroll={{ x: tableWidth, y: 'calc(100vh - 34rem)' }}
      />
    </Spin>
  )
}
