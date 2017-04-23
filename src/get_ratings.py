import warnings
warnings.filterwarnings('ignore')
from pymongo import MongoClient
import copy
import pandas as pd
import requests
from bs4 import BeautifulSoup

# Get html 
simpsons_ratings_url = 'http://www.imdb.com/title/tt0096697/epdate'
r = requests.get(simpsons_ratings_url)

# Parse
soup = BeautifulSoup(r.content, "lxml")

# Generate table
div = soup.find("div", {"id": "tn15content"})
table = div.find("table")
rows = table.find_all("tr")
all_rows = []
empty_row = {
    "number": None, "title": None, "user_rating": None, "user_votes": None}
# skip header row
for row in rows[1:]:
    new_row = copy.copy(empty_row)
    # A list of all the entries in the row.
    columns = row.find_all("td")
    new_row['number'] = float(columns[0].text.strip())
    new_row['title'] = columns[1].text.strip()
    new_row['user_rating'] = float(columns[2].text.strip())
    new_row['user_votes'] = int(columns[3].text.strip().replace(',',''))
    all_rows.append(new_row)

# Load rows into mongo db
client = MongoClient('mongodb://localhost:27017/')
db = client.simpsons
ratings = db.simpsons
for row in all_rows:
    ratings.insert_one(row)

# Load into pandas dataframe
rows = ratings.find()
simpsons_ratings = pd.DataFrame(list(rows))
simpsons_ratings.to_csv('simpsons_ratings.csv')
