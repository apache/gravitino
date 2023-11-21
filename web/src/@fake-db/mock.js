/*
 * Copyright 2023 Datastrato.
 * This software is licensed under the Apache License version 2.
 */

import axios from 'axios'
import MockAdapter from 'axios-mock-adapter'

const mock = new MockAdapter(axios)

export default mock
