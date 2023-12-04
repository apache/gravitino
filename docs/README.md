---
title: "Gravitino Documentation"
date: 2023-10-03T09:03:20-08:00
license: "Copyright 2023 Datastrato.
This software is licensed under the Apache License version 2."
---
## Gravitino documentation

## Launch Gravitino document website

Execute `./launch-docs-website.sh` scripts. The script will create the Gravitino document website locally following these steps:

1. Create a `build` directory within the `docs` directory.
2. Automatically download the [Hugo](https://github.com/gohugoio/hugo) binary executable file into the `build` directory.
3. Use the `hugo new site web` command to create the website project within the `build` directory.
4. Copy all markdown files into `web/content/docs` to generate the HTML for the document website using Hugo.
5. Copy the entire assets directory into `web/static` to include the images referenced in the document website HTML.
6. Automatically replace the Markdown embedded image addresses with Hugo website absolute paths.
   > This is necessary because Markdown embedded images use relative paths, whereas Hugo website images require absolute paths.
   > Replace `![](assets/` with `![](/assets/` in all markdown files.
7. Execute `hugo server` to launch the website.
8. Open `http://localhost:1313` in your browser to view the Gravitino document website.

## Add or modify a document

To add a new document to the Gravitino website, follow these steps:

1. Create a new markdown file in the `docs` directory.
2. For the markdown file, include the following header at the beginning as `Hugo` uses it to create the website links list.

    ```text
    ---
    title: "Article Title"
    date: Writing date
    license: "Copyright 2023 Datastrato.
    This software is licensed under the Apache License version 2."
    ---
    ```

3. To insert an image into a markdown file, you must save all the referenced images in the `docs/assets` directory.
   Additionally, you must use the following format: `![](assets/...`
4. Execute `./launch-docs-website.sh update` scripts to update the Gravitino document website.
