<!--

  Licensed to the Apache Software Foundation (ASF) under one or more
  contributor license agreements.  See the NOTICE file distributed with
  this work for additional information regarding copyright ownership.
  The ASF licenses this file to You under the Apache License, Version 2.0
  (the "License"); you may not use this file except in compliance with
  the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.

-->

# Apache ZooKeeper

This directory contains a brief introduction into Apache ZooKeeper.


## Building the presentation

By running the following command, you can generate the presentation:

    mvn clean package
   
## Running the presentation

In order to start a local web server serving the presentation, execute the following command:

    mvn jetty:run-exploded
    
As soon as that's done, just point your browser to:

    http://localhost:8080/

## Generating PDF versions

In order to generate a PDF version of the presentation just add `?print-pdf` to the url. (Keep in mind, that you have to add it before any `#blahblah`)

The following link should do the trick:

    http://localhost:8080/?print-pdf
    
As soon as that's loaded, just use the normal `print` functionality of the browser and `print as PDF`.
