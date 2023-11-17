import mock from 'src/@fake-db/mock'

const data = {
  metalakes: [
    {
      id: 1,
      name: 'datastrato-test-1',
      properties: [
        {
          key: 'metalake_name',
          value: 'datastrato-test-1'
        },
        {
          key: 'key1',
          value: 'value1'
        }
      ],
      comment: 'Neque porro quisquam est qui dolorem ipsum quia dolor sit amet, consectetur, adipisci velit...',
      createdBy: 'admin',
      createdAt: new Date('2023-10-10 10:10:10'),
      lastModifiedBy: 'user',
      lastModifiedAt: new Date('2023-10-10 11:11:11')
    }
  ]
}

mock.onGet('/apis/metalakes').reply(config => {
  const { q = '' } = config.params ?? ''
  const queryLowered = q.toLowerCase()

  const filteredData = data.metalakes.filter(metalake => metalake.name.toLowerCase().includes(queryLowered))

  return [
    200,
    {
      allData: data.metalakes,
      metalakes: filteredData,
      params: config.params,
      total: filteredData.length
    }
  ]
})

mock.onPost('/apis/metalakes').reply(config => {
  const { metalake } = JSON.parse(config.data).data
  const { length } = data.metalakes
  let lastIndex = 0
  if (length) {
    lastIndex = data.metalakes[length - 1].id
  }
  metalake.id = lastIndex + 1

  const item = {
    ...metalake,
    lastModifiedAt: new Date(),
    lastModifiedBy: 'user',
    createdAt: new Date(),
    createdBy: 'admin'
  }
  data.metalakes.push(item)

  return [201, { metalake }]
})
