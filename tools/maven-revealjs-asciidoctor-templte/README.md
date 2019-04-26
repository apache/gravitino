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

# Presentation with Reveal.JS and AsciiDoctor

Remarks:
- Currently it seems as if the system can't detect the 'docinfo' files, so we have to replace the `document.html.slim` file from `asciidoctor-reveal.js` with an updated one, that adds some additional js and css references. This template is located in `libs/docinfo-hack`.
- In order to use the preview of the IntelliJ asciidoctor plugin, you need to set an attribute in the plugin settings: `imagesdir` = `../resources/images`
- Even if it is possible to run the presentation directly from the `generated-slides` directory, some JavaScript extensions don't work in this case. Therefore it is required to run the presentation from a local webserver. 
- In order to generate the diagrams, GraphVIS needs to be installed locally. Get it from: http://www.graphviz.org/
- The template is adjusted to use the codecentric font `Panton`, so be sure to have that installed on your presentation machine.
- Any css adjustments can go to `src/main/theme/cc.css` as this is automatically embedded into the themes directory.

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

## Installing third party software:

### Mermaid

    npm install mermaid.cli
    
This will install mermaid under `node_modules/.bin/mmdc`.

### PhantomJS

https://bitbucket.org/ariya/phantomjs/downloads/phantomjs-2.1.1-macosx.zip
