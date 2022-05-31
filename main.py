from sqlalchemy import create_engine
from fastapi import Depends, FastAPI
from pydantic import BaseModel
import pandas as pd

app = FastAPI()


    
data = pd.read_csv('ETFs.csv')

engine = create_engine('sqlite:///:memory:')
data.to_sql('data_table', engine)



""" class Fund(BaseModel):
    fund_symbol: str
    exchange_name: str
    total_net_assets: str
    fund_category: str
    top10_holdings: str

class Preferences(BaseModel):
    number_of_results: int
    order_by_total_net: str """
    
@app.get("/")
async def root():
    return {"message": "Hello World"}

#http://127.0.0.1:8000/size/10/DESC/100000
@app.get("/size/{nor}/{order}/{tna}")
async def search(nor: str, order: str, tna: str):
    query = f"SELECT fund_symbol, exchange_name, total_net_assets, fund_category FROM data_table WHERE total_net_assets>{tna} ORDER BY total_net_assets {order} LIMIT {nor}"
    result = pd.read_sql_query(query, engine)
    print(result)
    allfunds = []
    for i in range(len(result)):
        x={
            'fund_symbol': result.iloc[i]['fund_symbol'],
            'exchange_name': result.iloc[i]['exchange_name'],
            'total_net_assets': result.iloc[i]['total_net_assets'],
            'fund_category': result.iloc[i]['fund_category']
        }
        allfunds.append(x)
    return allfunds

#http://127.0.0.1:8000/dominant/5/DESC/Large%20Blend
@app.get("/dominant/{nor}/{order}/{tna}")
async def search(nor: str, order: str, tna: str):
    query = f"SELECT fund_symbol, exchange_name, total_net_assets, fund_category FROM data_table WHERE fund_category='{tna}' ORDER BY total_net_assets {order} LIMIT {nor}"
    result = pd.read_sql_query(query, engine)
    print(result)
    allfunds = []
    for i in range(len(result)):
        x={
            'fund_symbol': result.iloc[i]['fund_symbol'],
            'exchange_name': result.iloc[i]['exchange_name'],
            'total_net_assets': result.iloc[i]['total_net_assets'],
            'fund_category': result.iloc[i]['fund_category']
        }
        allfunds.append(x)
    return allfunds

""" @app.post("/preferences/{nor}/{order}")
async def prefs(nor: str, order: str):
    newPref= Preferences(number_of_results=nor, order_by_total_net=order)
    //insert database
    return {"message": "Prefs saved"} """
