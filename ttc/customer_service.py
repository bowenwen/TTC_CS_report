import urllib.request
from bs4 import BeautifulSoup
import pandas as pd
import collections as co

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

t_val = []
# for v in range(8):
#     t_val.append([])
for rows in right_table.findAll('tbody'):
    values = rows.findAll('td')
    for j in values:
        t_val.append(j.find(text=True))

print(t_val)



# df = pd.DataFrame(columns=header)
# print(df)
