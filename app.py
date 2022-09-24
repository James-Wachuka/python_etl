import db_connect
import pandas as pd
import psycopg2
import psycopg2.extras as extras

# ignore user warnings
import warnings
warnings.simplefilter("ignore", UserWarning)

cur1, conn1 = db_connect.get_conn_mysql()
cur2, conn2 = db_connect.get_conn_postgresql()

# creating and updating tables for the target database
commands = (
        """
        CREATE TABLE IF NOT EXISTS toporders(
            customername VARCHAR(255),
            number_of_orders INTEGER
        )
        """,
        """ CREATE TABLE IF NOT EXISTS product_demand(
                productName VARCHAR(255),
                quantity_ordered INTEGER
                )
        """,
        """
        CREATE TABLE IF NOT EXISTS customer_spending(
            customername VARCHAR(255),
            total_amount_spent float8
        )
        """,
        """
        TRUNCATE TABLE toporders, product_demand, customer_spending;
        """
        )
# executing the queries against the target database
for command in commands:
    cur2.execute(command)
print("--------- tables updated ----------")
# commit schema changes
conn2.commit()

# showing tables present in the target database
cur2.execute("""SELECT table_name FROM information_schema.tables
       WHERE table_schema = 'public'""")
for table in cur2.fetchall():
    print(table)


# extracting data from the source database-views
#product demand-products with highest purchases
query1="SELECT productName , SUM(quantityOrdered) AS quantity_ordered\
       FROM  products, orderdetails\
       WHERE products.productCode = orderdetails.productCode\
       GROUP BY productName\
       ORDER BY quantity_ordered DESC\
       LIMIT 20;"

# toporders- customers that have the most orders
query2="SELECT contactFirstName, contactLastName , COUNT(*) AS number_of_orders\
       FROM  customers, orders\
       WHERE customers.customerNumber = orders.customerNumber\
       GROUP BY customerName\
       ORDER BY number_of_orders DESC\
       LIMIT 20;"
# customer spending- ustomers that have spent more
query3="SELECT contactFirstName , contactLastName, SUM(quantityOrdered*priceEach) AS total_amount_spent\
       FROM  customers, orders, orderdetails\
       WHERE customers.customerNumber = orders.customerNumber AND orderdetails.orderNumber= orders.orderNumber\
       GROUP BY customerName\
       ORDER BY total_amount_spent DESC\
       LIMIT 10;"

# creating dataframes from the queries
df1= pd.read_sql(query1,con=conn1)
df2= pd.read_sql(query2,con=conn1)
df3= pd.read_sql(query3,con=conn1)

# performing some transformations on the dataframes- joining columns for first name and last name
df2['customername'] = df2['contactFirstName'].str.cat(df2['contactLastName'],sep=" ")
df2=df2.drop(['contactFirstName','contactLastName'], axis=1)
df3['customername'] = df3['contactFirstName'].str.cat(df3['contactLastName'],sep=" ")
df3=df3.drop(['contactFirstName','contactLastName'], axis=1)
# converting the datatype of quantity ordered to integer for df-product demand
data_types={'quantity_ordered':int}
df1 = df1.astype(data_types)

# loading the df into the target database tables
def execute_values(conn, df, table):
  
    tuples = [tuple(x) for x in df.to_numpy()]
  
    cols = ','.join(list(df.columns))
    # SQL query to execute
    query = "INSERT INTO %s(%s) VALUES %%s" % (table, cols)
    try:
        extras.execute_values(cur2, query, tuples)
        conn2.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error: %s" % error)
        conn.rollback()
        return 1
    print("-------data updated/inserted into table----",table)

execute_values(conn2, df1,'product_demand')
execute_values(conn2, df2,'toporders')
execute_values(conn2, df3,'customer_spending')

# close connections for mysql and postgresql
conn1.close()
conn2.close()