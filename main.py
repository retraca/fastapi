from sqlalchemy import create_engine
from fastapi import Depends, FastAPI
from pydantic import BaseModel
import pandas as pd

app = FastAPI()


    
# making dataframe
data = pd.read_csv('ETFs.csv')
state = pd.read_csv('state.csv')
# Create the db engine
engine = create_engine('sqlite:///:memory:')
# Store the dataframe as a table
data.to_sql('data_table', engine)
state.to_sql('state', engine)


class Fund(BaseModel):
    fund_symbol: str
    exchange_name: str
    total_net_assets: str
    fund_family: str
    top10_holdings: str

class Preferences(BaseModel):
    number_of_results: int
    order_by_total_net: str
    
@app.get("/")
async def root():
    return {"message": "Hello World"}

@app.get("/search/{nor}/{order}/{tna}")
async def search(nor: str, order: str, tna: str):
    """ querystate = f"SELECT * FROM state LIMIT 1"
    result = pd.read_sql_query(querystate, engine)
    number_of_results= result.iloc[0]['nor']
    order_by_total_net = result.iloc[0]['order'] """
    piv=tna
    query = f"SELECT fund_symbol, exchange_name, total_net_assets, fund_family FROM data_table WHERE total_net_assets>{piv} ORDER BY total_net_assets {order} LIMIT {nor}"
    try :
        piv = int(tna)
    except:
        query = f"SELECT fund_symbol, exchange_name, total_net_assets, fund_family FROM data_table WHERE fund_family={piv} ORDER BY total_net_assets {order} LIMIT {nor}"
    result = pd.read_sql_query(query, engine)
    print(result)
    allfunds = []
    for i in range(len(result)):
        x={
            'fund_symbol': result.iloc[i]['fund_symbol'],
            'exchange_name': result.iloc[i]['exchange_name'],
            'total_net_assets': result.iloc[i]['total_net_assets'],
            'fund_family': result.iloc[i]['fund_family']
        }
        allfunds.append(x)
    return allfunds

""" @app.post("/preferences/{nor}/{order}")
async def prefs(nor: str, order: str):
    newPref= Preferences(number_of_results=nor, order_by_total_net=order)
    return {"message": "Prefs saved"} """
