# Creating App Reporting Pack in DataStudio

Once data for App Reporting Pack data are generated and stored in BigQuery you can
proceed with replication which consists of two steps:

* Replication of datasources
* Replication of dashboard

## Replicate datasources

Before replicating the dashboard you need to make copies of datasources that power up the dashboard.
Replication of the datasources is important since they contains multiple calculated metrics which could be problematic to create from scratch.

Make a copy of each of the following datasources used in the template dashboard.
* [asset_performance](https://datastudio.google.com/c/u/0/datasources/cb655b63-49c0-48d6-babf-aa956c369b15)
* [approval_statuses](https://datastudio.google.com/c/u/0/datasources/cdbfb99c-203c-4eeb-9a57-3d99f34546ee)
* [creative_excellence](https://datastudio.google.com/c/u/0/datasources/636bab56-3bff-4143-92a2-106206c4ad03)
* [change_history](https://datastudio.google.com/c/u/0/datasources/e82e7458-f386-419f-9556-31b932d68463)
* [performance_grouping](https://datastudio.google.com/c/u/0/datasources/e211b30c-0209-4940-98b5-61517fdb8f13)
* [ad_group_network_split](https://datastudio.google.com/c/u/0/datasources/36341813-dee7-4aef-8b9b-bf015e4657d6)


In order to replicate a datasource, please do the following:
* Click on the datasource link above.
* Click on *Make a copy of this datasource*

	![make_copy_datasource](src/make_copy_datasource.png)

* Confirm copying by clicking *Copy Data Source*

	![confirm](src/copy_confirm.png)

* Select *MY PROJECTS* and either pick a project or enter project id manually (this should be the project where App Reporting Pack tables are located)
* In Dataset select a BQ dataset where App Reporting Pack tables are located

	![setup project](src/setup_project.png)
* Select a table from the dataset which the name similar to Data Source name (i.e., if Data Source is called *Assets* look for the table which is called *assets*)

	![select table](src/select_table.png)

* Confirm copying by clicking *RECONNECT* button.

	![reconnect](src/reconnect.png)


> Don’t forget to rename the datasource so you can find it easily. I.e. such name as *Copy of BQ Template ARP Assets* is a bit mouthful, you can name it simply *ARP Assets* or *YOUR-COMPANY-NAME ARP Assets*.

* Repeat the steps above for all the datasources.

Now that you’ve copied each of the datasources, make a copy of the dashboard and replace each of the template’s datasources with the corresponding datasource you copied.

## Replication of the dashboard

> Please ensure that ALL datasources are created before proceeding to replication of the dashboard.

You can access the template version of the dashboard [here](https://datastudio.google.com/c/u/0/reporting/187f1f41-16bc-434d-8437-7988bed6e8b9/page/0hcO).

In order to replicate dashboard please do the following:

* make a [copy of the dashboard](https://datastudio.google.com/c/u/0/reporting/187f1f41-16bc-434d-8437-7988bed6e8b9/page/0hcO) by clicking on *More options - Make a copy*.

	![copy dashboard](src/copy_dashboard.png)

* In *Copy this report* window map original datasources to the ones you created in the previous step.

	![datasource association](src/datasource_association.png)

Once all template datasources are replaced with new ones, click *Copy Report* and enjoy your new shiny App Reporting Pack!


