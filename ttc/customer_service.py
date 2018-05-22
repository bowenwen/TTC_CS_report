"""
Py script name : customer_service.py
Author : Dhanyatha Mylanahalli Harish
Modules used : urllib, BeautifulSoup, pyycopg2, sqlalchemy
Purpose : Fetch the Customer Service Report from TTC website and add these data daily into DataBAse
"""
import urllib.request
from bs4 import BeautifulSoup
import pandas as pd
import psycopg2
import sys
from sqlalchemy import create_engine

# URL to the required web page
ttc = "http://www.ttc.ca/Customer_Service/Daily_Customer_Service_Report/index.jsp"

# Query the website and return the HTML to the variable 'page'
try:
    page = urllib.request.urlopen(ttc)
except Exception as page_error:
    print("An exception occurred while trying to open a web page and the exception is ", page_error)

# Parse the HTML in the page variable and store it in BeautifulSoup format
soup = BeautifulSoup(page, "html.parser")

# Pick the table that has to be parsed using the class name of the table
try:
    right_table = soup.find('table', class_='ttc-customer-service-table')
except Exception as table_error:
    print("An exception occurred while finding the right table to fetch using class name and the exception is ", table_error)

# Create and update a list called 'header' to save the column names of the table
try:
    header = []
    for head in right_table.findAll("thead"):
        cells = head.findAll('th')
        for c_head in cells:
            header.append(c_head.find(text=True))
    del header[0]
except Exception as header_error:
    print("An exception occurred while fetching the column names and the exception is ", header_error)

# Create and update a list called 'row_vals' to save the row values of the table
# by iterating through every row and every cell of the table
try:
    t_val = []
    row_vals = []
    for rows in right_table.findAll('tbody'):
        row=rows.findAll('tr')
        for cells in row:
            values = cells.findAll('td')
            for c_vals in values:
                if c_vals.find('img'):
                    result = c_vals.findAll(text=False)
                    result = str(result).split('"')[1]
                    if (result == "pass") or (result == "fail"):
                        t_val.append(result)
                else:
                    t_val.append(c_vals.findAll(text=True)[:1])
            row_vals.append(tuple(t_val))
            t_val.clear()
except Exception as value_error:
    print("An exception occurred while fetching all the values from a table and the exception is ", value_error)

# Save the table data in DataFrame 'df'
try:
    df = pd.DataFrame.from_records(row_vals, columns=header)
except Exception as dataframe_error:
    print("An exception occurred while creating a DataFrame and the exception is ", dataframe_error)

# Connect to a Database name 'interview_test'
try:
    con = psycopg2.connect("dbname=interview_test host=localhost user=postgres password=Dhanyatha")
except psycopg2.ProgrammingError:
    pass

# Create a cursor for the connection for data manipulation
cur = con.cursor()

# Create a table "TTC_CSR"
try:
    cur.execute('CREATE TABLE TTC_CSR ()')
except psycopg2.ProgrammingError:
    # If the table is already there, ignore this exception
    pass

# Commit the changes made to the table
con.commit()

# Create and engine to enable the DataFrame to be pushed to DataBase
try:
    url = 'postgresql://postgres:Dhanyatha@localhost:5432/interview_test'
    engine = create_engine(url, pool_pre_ping=True)
except Exception as engine_error:
    print("An exception occurred while creating an engine and the exception is ", engine_error)

# Push the table data into PostgreSQL Database
try:
    df.to_sql("TTC_CSR", con=engine, if_exists='append')
except Exception as df_db_error:
    print("An exception occurred while pushing the dataframe to database and the exception is ", df_db_error)

# Read back the table from DataBase into DataFrame and save it in "check" object
try:
    check = pd.read_sql_table("TTC_CSR", con=engine)
except Exception as db_df_error:
    print("An exception occurred while retrieving the table from database to dataframe and the exception is ",
          db_df_error)

# Print the data from Database for verification
print(check)

# Close the connection
cur.close()
con.close()
