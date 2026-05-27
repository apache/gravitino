import React from 'react'
import Link from 'next/link'
import { Tooltip, Dropdown } from 'antd'
import Tags from '@/components/CustomTags'
import Policies from '@/components/PolicyTag'
import Icons from '@/components/Icons'
import TableActions from '@/components/TableActions'

export function buildSchemaColumns({
  currentMetalake,
  catalog,
  catalogType,
  anthEnable,
  provider,
  handleEditSchema,
  showDeleteConfirm,
  handleSetOwner
}) {
  return [
    {
      title: 'Schema Name',
      dataIndex: 'name',
      key: 'name',
      ellipsis: true,
      sorter: (a, b) => a.name.toLowerCase().localeCompare(b.name.toLowerCase()),
      width: 200,
      render: name => (
        <Link
          data-refer={`schema-link-${name}`}
          href={`/catalogs?metalake=${encodeURIComponent(currentMetalake)}&catalogType=${catalogType}&catalog=${encodeURIComponent(catalog)}&schema=${encodeURIComponent(name)}`}
        >
          {name}
        </Link>
      )
    },
    {
      title: 'Tags',
      dataIndex: 'tags',
      key: 'tags',
      ellipsis: true,
      render: (_, record) =>
        record?.node === 'schema' ? (
          <Tags metadataObjectType={'schema'} metadataObjectFullName={`${record.namespace?.at(-1)}.${record.name}`} />
        ) : null
    },
    {
      title: 'Policies',
      dataIndex: 'policies',
      key: 'policies',
      ellipsis: true,
      render: (_, record) =>
        record?.node === 'schema' ? (
          <Policies
            metadataObjectType={'schema'}
            metadataObjectFullName={`${record.namespace?.at(-1)}.${record.name}`}
          />
        ) : null
    },
    ...(provider !== 'kafka'
      ? [
          {
            title: 'Actions',
            dataIndex: 'action',
            key: 'action',
            width: 120,
            render: (_, record) => {
              return (
                <div className='flex gap-2'>
                  <a>
                    <Tooltip title='Edit'>
                      <Icons.Pencil className='size-4' onClick={() => handleEditSchema(record.name)} />
                    </Tooltip>
                  </a>
                  <a>
                    <Tooltip title='Delete'>
                      <Icons.Trash2Icon className='size-4' onClick={() => showDeleteConfirm(record.name)} />
                    </Tooltip>
                  </a>
                  {anthEnable && (
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
                              handleSetOwner('schema', `${catalog}.${record.name}`)
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
          }
        ]
      : [])
  ]
}

export default buildSchemaColumns
