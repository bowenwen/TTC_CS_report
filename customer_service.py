import urllib.request
from bs4 import BeautifulSoup
import pandas as pd
import psycopg2
import sys
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from sqlalchemy import create_engine

# URL to the required web page
ttc = "http://www.ttc.ca/Customer_Service/Daily_Customer_Service_Report/index.jsp"

# Query the website and return the HTML to the variable page
page = urllib.request.urlopen(ttc)

# Parse the HTML in the page variable and store it in BeautifulSoup format
soup = BeautifulSoup(page, "html.parser")

# Print the HTML file that is parsed
# print(soup.prettify())

all_tables = soup.find_all('table')

# for table in all_tables:
#     print(table)

right_table = soup.find('table', class_='ttc-customer-service-table')
# print(right_table)

# Generate lists
header=[]
for head in right_table.findAll("thead"):
    cells = head.findAll('th')
    for i in cells:
        header.append(i.find(text=True))
del header[0]
# print(header)

t_val = []
for rows in right_table.findAll('tbody'):
    values = rows.findAll('td')
    for i in values:
        if i.find('img'):
            a = i.findAll(text=False)
            a = str(a).split('"')[1]
            if (a == "pass") or (a == "fail"):
                t_val.append(a)
        else:
            t_val.append(i.findAll(text=True))

# print(t_val)
row_vals = []
start = 0
end = 5
n = 0
for out in range(8):
    del t_val[(start+1)][1]
    tup = tuple(t_val[start:end])
    # print(tup)
    row_vals.append(tup)
    start = end
    end = end + 5
    n = n + 1


df = pd.DataFrame.from_records(row_vals, columns=header)
# print(df)

try:
    con = psycopg2.connect("dbname=interview host=localhost user=postgres password=Dhanyatha")
except Exception as e:
    print(e)
# con.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
cur = con.cursor()

try:
    cur.execute('CREATE TABLE ttc ()')
except Exception as e:
    print(e)

# cur.close()
con.commit()

url = 'postgresql://postgres:Dhanyatha@localhost:5432/interview'
engine = create_engine(url, pool_pre_ping=True)
try:
    df.to_sql("ttc", con=engine, if_exists='append')
except Exception as e:
    print(e)

check = pd.read_sql_table("ttc", con=engine)
print(check)


cur.close()
con.close()
