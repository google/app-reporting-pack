# Creating App Reporting Pack in DataStudio

Once data for App Reporting Pack data are generated and stored in BigQuery you can
proceed with replication which consists of two steps:

* Replication of datasources
* Replication of dashboard

## Replicate datasources

Before replicating the dashboard you need to make copies of datasources that power up the dashboard.
Replication of the datasources is important since they contains multiple calculated metrics which could be problematic to create from scratch.

Make a copy of each of the following datasources used in the template dashboard.
* [Asset Performance](https://datastudio.google.com/c/u/0/datasources/c0cbd3e6-b0ba-4ee2-9a3e-bb645e9dc88://datastudio.google.com/c/u/0/datasources/c0cbd3e6-b0ba-4ee2-9a3e-bb645e9dc88e)
* [Disapprovals](https://datastudio.google.com/c/u/0/datasources/2f25f95c-3307-4a13-97a3-3b5ce3d60340)
* [Creative Excellence](https://datastudio.google.com/c/u/0/datasources/124eb3dd-d299-4d57-946b-ab4a98fdc3d6)
* [Change History](https://datastudio.google.com/c/u/0/datasources/9fc9197c-1eaa-4343-a673-0af374d32149)
* [Performance Grouping Changes](https://datastudio.google.com/c/u/0/datasources/7100d995-e5c4-4390-be7c-d7b1d4fd6c44)
* [Ad Group Network Split ](https://datastudio.google.com/c/u/0/datasources/c37d8e08-2fbb-4d33-911c-d81130e90786)
* [Cannibalization](https://datastudio.google.com/c/u/0/datasources/79b2e275-3427-4eaf-a61a-5f4b550787fe)


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

You can access the template version of the dashboard [here](https://datastudio.google.com/c/u/0/reporting/6b7ef4da-2337-4178-bf9b-2c3097e6eae8/page/0hcO).

In order to replicate dashboard please do the following:

* make a [copy of the dashboard](https://support.google.com/datastudio/answer/7175478?hl=en#zippy=%2Cin-this-article) by clicking on *More options - Make a copy*.

	![copy dashboard](src/copy_dashboard.png)

* In *Copy this report* window map original datasources to the ones you created in the previous step.

	![datasource association](src/datasource_association.png)

Once all template datasources are replaced with new ones, click *Copy Report* and enjoy your new shiny App Reporting Pack!


