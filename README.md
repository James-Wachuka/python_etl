###### Python ETL
Using python to extract data from a source database:mysql,  perfrom transformations on the data and then load the data  to a target analytical database :postgresql. 
The etl job is the run using windows task scheduler every 5 minutes.

###### data
The source database contains a classicmodels db which is a database for a car retail company.
From the database can report on the following.
* customers who made the most orders
* products that have the highest number of purchases
* customers who have spent more

this data is then extracted and transformed

```
# performing some transformations on the dataframes- joining columns for first name and last name
df2['customername'] = df2['contactFirstName'].str.cat(df2['contactLastName'],sep=" ")
df2=df2.drop(['contactFirstName','contactLastName'], axis=1)
df3['customername'] = df3['contactFirstName'].str.cat(df3['contactLastName'],sep=" ")
df3=df3.drop(['contactFirstName','contactLastName'], axis=1)
# converting the datatype of quantity ordered to integer for df-product demand
data_types={'quantity_ordered':int}
df1 = df1.astype(data_types)
```


###### loading data into the target database-classicmodels_analysis
creating the analytical database 
```CREATE DATABASE classic_model_analysis```
Then the individual tables to which the data will be loaded are created.

###### Automating the job with windows scheduler
This etl job is scheduled to run every 5 minutes for one day, using the windows task scheduler.  ```schedule_python_etl.bat``` activates the environment and runs the python script.
Creating a task in windows task scheduler.
```start->task scheduler->create a folder (mytask)->create task (python_etl)->trigger(repeat after 5 mins)->action(start program-schedule_python_etl.bat)```

###### Usage
* install mysql and postgresql databases
* populate the source database using the ```mysqlsampledatabase.sql``` file
* clone the repository
-. install the requirements, activate venv and run the script
