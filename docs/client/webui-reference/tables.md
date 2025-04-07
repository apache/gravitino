---
title: Web UI for Tables
slug: /webui-tables
keywords:
  - webui 
  - table
license: 'This software is licensed under the Apache License version 2.'
---

## Table

<!--TODO(Qiming): This is not generic-->
Click the hive schema tree node on the left sidebar or the schema name link in the table cell.

Displays the list tables of the schema.

![list-tables](../../assets/webui/list-tables.png)

### Create a table

Click on the `CREATE TABLE` button displays the dialog to create a table.

![create-table](../../assets/webui/create-table.png)

Creating a table needs these fields:

1. **Name** (_required_): the name of the table.
1. **Columns** (_required_):

   1. The name and type of each column are required.
   1. Only suppport simple types, cannot support complex types by UI.
      You can create complex types by calling the APIs.

1. **Comment** (_optional_): the comment of the table.
1. **Properties** (_optional_): Click on the `ADD PROPERTY` button to add custom properties.

#### Show table details

Click on the action icon <Icon icon='bx:show-alt' fontSize='24' />
in the table cell.

You can see the detailed information of this table in the drawer component on the right.

![table-details](../../assets/webui/table-details.png)

Click the table tree node on the left sidebar or the table name link in the table cell.

You can see the columns and detailed information on the right page.

![list-columns](../../assets/webui/list-columns.png)
![table-selected-details](../../assets/webui/table-selected-details.png)

#### Edit a table

Click on the action icon <Icon icon='mdi:square-edit-outline' fontSize='24' />
in the table cell.

Displays the dialog for modifying fields of the selected table.

![update-table-dialog](../../assets/webui/update-table-dialog.png)

#### Drop a table

Click on the action icon <Icon icon='mdi:delete-outline' fontSize='24' color='red' />
in the table cell.

Displays a confirmation dialog, clicking on the `DROP` button drops this table.

![delete-table](../../assets/webui/delete-table.png)



