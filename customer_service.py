import urllib.request
from bs4 import BeautifulSoup
import pandas as pd
import psycopg2
import sys
from sqlalchemy import create_engine

# URL to the required web page
ttc = "http://www.ttc.ca/Customer_Service/Daily_Customer_Service_Report/index.jsp"

# Query the website and return the HTML to the variable 'page'
page = urllib.request.urlopen(ttc)

# Parse the HTML in the page variable and store it in BeautifulSoup format
soup = BeautifulSoup(page, "html.parser")

# Pick the table that has to be parsed using the class name of the table
right_table = soup.find('table', class_='ttc-customer-service-table')

# Create and update a list called 'header' to save the column names of the table
header = []
for head in right_table.findAll("thead"):
    cells = head.findAll('th')
    for c_head in cells:
        header.append(c_head.find(text=True))
del header[0]

# Create and update a list called 't_val' to save the column values of the table
t_val = []
for rows in right_table.findAll('tbody'):
    values = rows.findAll('td')
    for c_vals in values:
        if c_vals.find('img'):
            result = c_vals.findAll(text=False)
            result = str(result).split('"')[1]
            if (result == "pass") or (result == "fail"):
                t_val.append(result)
        else:
            t_val.append(c_vals.findAll(text=True))

# Sort each row values and put them in a tuple so as to put them in a DataFrame
row_vals = []
start = 0
end = 5
for loop in range(8):
    del t_val[(start+1)][1]
    tup = tuple(t_val[start:end])
    row_vals.append(tup)
    start = end
    end = end + 5

# Save the table data in DataFrame 'df'
df = pd.DataFrame.from_records(row_vals, columns=header)

# Connect to a Database name 'interview_test'
try:
    con = psycopg2.connect("dbname=interview_test host=localhost user=postgres password=Dhanyatha")
except Exception:
    pass

# Create a cursor for the connection for data manipulation
cur = con.cursor()

# Create a table "TTC_CSR"
try:
    cur.execute('CREATE TABLE TTC_CSR ()')
except Exception:
    pass

# Commit the changes made to the table
con.commit()

# Create and engine to enable the DataFrame to be pushed to DataBase
url = 'postgresql://postgres:Dhanyatha@localhost:5432/interview_test'
engine = create_engine(url, pool_pre_ping=True)

# Push the table data into PostgreSQL Database
try:
    df.to_sql("TTC_CSR", con=engine, if_exists='append')
except Exception:
    pass

# Read back the table from DataBase into DataFrame and save it in "check" object
check = pd.read_sql_table("TTC_CSR", con=engine)

# Print the data from Database for verification
# print(check)

# Close the connection
cur.close()
con.close()
