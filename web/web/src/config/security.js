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

export const privilegeOptionsMap = {
  metalake: [
    {
      label: 'Manage',
      options: [
        {
          label: 'Manage Users',
          value: 'manage_users'
        },
        {
          label: 'Manage Groups',
          value: 'manage_groups'
        },
        {
          label: 'Manage Grants',
          value: 'manage_grants'
        },
        {
          label: 'Create Role',
          value: 'create_role'
        }
      ]
    },
    {
      label: 'Write',
      options: [
        {
          label: 'Create Catalog',
          value: 'create_catalog'
        },
        {
          label: 'Create Schema',
          value: 'create_schema'
        },
        {
          label: 'Create Table',
          value: 'create_table'
        },
        {
          label: 'Modify Table',
          value: 'modify_table'
        },
        {
          label: 'Create Topic',
          value: 'create_topic'
        },
        {
          label: 'Produce Topic',
          value: 'produce_topic'
        },
        {
          label: 'Create Fileset',
          value: 'create_fileset'
        },
        {
          label: 'Write Fileset',
          value: 'write_fileset'
        },
        {
          label: 'Create Model',
          value: 'create_model'
        },
        {
          label: 'Create Model Version',
          value: 'create_model_version'
        }
      ]
    },
    {
      label: 'Read',
      options: [
        {
          label: 'Use Catalog',
          value: 'use_catalog'
        },
        {
          label: 'Use Schema',
          value: 'use_schema'
        },
        {
          label: 'Select Table',
          value: 'select_table'
        },
        {
          label: 'Consume Topic',
          value: 'consume_topic'
        },
        {
          label: 'Read Fileset',
          value: 'read_fileset'
        },
        {
          label: 'Use Model',
          value: 'use_model'
        }
      ]
    }
  ],
  'catalog-relational': [
    {
      label: 'Write',
      options: [
        {
          label: 'Create Schema',
          value: 'create_schema'
        },
        {
          label: 'Create Table',
          value: 'create_table'
        },
        {
          label: 'Modify Table',
          value: 'modify_table'
        }
      ]
    },
    {
      label: 'Read',
      options: [
        {
          label: 'Use Catalog',
          value: 'use_catalog'
        },
        {
          label: 'Use Schema',
          value: 'use_schema'
        },
        {
          label: 'Select Table',
          value: 'select_table'
        }
      ]
    }
  ],
  'catalog-messaging': [
    {
      label: 'Write',
      options: [
        {
          label: 'Create Topic',
          value: 'create_topic'
        },
        {
          label: 'Produce Topic',
          value: 'produce_topic'
        }
      ]
    },
    {
      label: 'Read',
      options: [
        {
          label: 'Use Catalog',
          value: 'use_catalog'
        },
        {
          label: 'Use Schema',
          value: 'use_schema'
        },
        {
          label: 'Consume Topic',
          value: 'consume_topic'
        }
      ]
    }
  ],
  'catalog-fileset': [
    {
      label: 'Write',
      options: [
        {
          label: 'Create Schema',
          value: 'create_schema'
        },
        {
          label: 'Create Fileset',
          value: 'create_fileset'
        },
        {
          label: 'Write Fileset',
          value: 'write_fileset'
        }
      ]
    },
    {
      label: 'Read',
      options: [
        {
          label: 'Use Catalog',
          value: 'use_catalog'
        },
        {
          label: 'Use Schema',
          value: 'use_schema'
        },
        {
          label: 'Read Fileset',
          value: 'read_fileset'
        }
      ]
    }
  ],
  'catalog-model': [
    {
      label: 'Write',
      options: [
        {
          label: 'Create Schema',
          value: 'create_schema'
        },
        {
          label: 'Create Model',
          value: 'create_model'
        },
        {
          label: 'Create Model Version',
          value: 'create_model_version'
        }
      ]
    },
    {
      label: 'Read',
      options: [
        {
          label: 'Use Catalog',
          value: 'use_catalog'
        },
        {
          label: 'Use Schema',
          value: 'use_schema'
        },
        {
          label: 'Use Model',
          value: 'use_model'
        }
      ]
    }
  ],
  'schema-relational': [
    {
      label: 'Write',
      options: [
        {
          label: 'Create Table',
          value: 'create_table'
        },
        {
          label: 'Modify Table',
          value: 'modify_table'
        }
      ]
    },
    {
      label: 'Read',
      options: [
        {
          label: 'Use Schema',
          value: 'use_schema'
        },
        {
          label: 'Select Table',
          value: 'select_table'
        }
      ]
    }
  ],
  'schema-messaging': [
    {
      label: 'Write',
      options: [
        {
          label: 'Create Topic',
          value: 'create_topic'
        },
        {
          label: 'Produce Topic',
          value: 'produce_topic'
        }
      ]
    },
    {
      label: 'Read',
      options: [
        {
          label: 'Use Schema',
          value: 'use_schema'
        },
        {
          label: 'Consume Topic',
          value: 'consume_topic'
        }
      ]
    }
  ],
  'schema-fileset': [
    {
      label: 'Write',
      options: [
        {
          label: 'Create Fileset',
          value: 'create_fileset'
        },
        {
          label: 'Write Fileset',
          value: 'write_fileset'
        }
      ]
    },
    {
      label: 'Read',
      options: [
        {
          label: 'Use Schema',
          value: 'use_schema'
        },
        {
          label: 'Read Fileset',
          value: 'read_fileset'
        }
      ]
    }
  ],
  'schema-model': [
    {
      label: 'Write',
      options: [
        {
          label: 'Create Model',
          value: 'create_model'
        },
        {
          label: 'Create Model Version',
          value: 'create_model_version'
        }
      ]
    },
    {
      label: 'Read',
      options: [
        {
          label: 'Use Schema',
          value: 'use_schema'
        },
        {
          label: 'Use Model',
          value: 'use_model'
        }
      ]
    }
  ],
  table: [
    {
      label: 'Write',
      options: [
        {
          label: 'Modify Table',
          value: 'modify_table'
        }
      ]
    },
    {
      label: 'Read',
      options: [
        {
          label: 'Select Table',
          value: 'select_table'
        }
      ]
    }
  ],
  topic: [
    {
      label: 'Write',
      options: [
        {
          label: 'Produce Topic',
          value: 'produce_topic'
        }
      ]
    },
    {
      label: 'Read',
      options: [
        {
          label: 'Consume Topic',
          value: 'consume_topic'
        }
      ]
    }
  ],
  fileset: [
    {
      label: 'Write',
      options: [
        {
          label: 'Write Fileset',
          value: 'write_fileset'
        }
      ]
    },
    {
      label: 'Read',
      options: [
        {
          label: 'Read Fileset',
          value: 'read_fileset'
        }
      ]
    }
  ],
  model: [
    {
      label: 'Write',
      options: [
        {
          label: 'Create Model Version',
          value: 'create_model_version'
        }
      ]
    },
    {
      label: 'Read',
      options: [
        {
          label: 'Use Model',
          value: 'use_model'
        }
      ]
    }
  ]
}
