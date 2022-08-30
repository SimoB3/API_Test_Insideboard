from fastapi import Body, FastAPI
import pandas as pd
import json
from datetime import datetime, time, timedelta
from typing import Union
from uuid import UUID


items = pd.read_csv("api/data/items.csv")
ventes = pd.read_csv("transactions.csv")
customers = pd.read_csv("api/data/clients.csv")

customers.rename(columns={"id": "client_id"}, inplace=True)

app = FastAPI()

@app.get("/top_ventes")
async def NTop_Sales(top: int = 1):
    items.rename(columns={"id": "item_id"}, inplace=True)
    most_sell = ventes.groupby(['item_id'])['quantity'].sum().reset_index(name='total_sold').sort_values(by='total_sold', ascending=False)
    total_ventes = items.merge(most_sell, how='inner', on='item_id')
    total_ventes.sort_values('total_sold', ascending =False, inplace = True)
    total_ventes = total_ventes.head(top)
    posts = json.loads('{"items":' + total_ventes.to_json(orient='records', date_format='iso') + '}')

    return posts

@app.get("/top_customers/")
async def NTop_Customers(top: int = 1):
    ventes_clients = ventes.copy()
    list_client = customers.copy()
    ventes_clients['time'] = ventes_clients['time'].apply(pd.to_datetime)
    ventes_clients['time'] = pd.to_datetime(ventes_clients['time']).dt.date
    ventes_clients = ventes_clients.drop_duplicates(subset=["client_id", "time"], keep=False)
    ventes_clients = ventes_clients.groupby(['client_id'])['client_id'].size().reset_index(name='total orders')
    top_clients = list_client.merge(ventes_clients, how = "left", on ="client_id").sort_values(by='total orders', ascending=False)
    top_clients = top_clients.head(top)
    posts = json.loads('{"items":' + top_clients.to_json(orient='records', date_format='iso') + '}')

    return posts

@app.get("/daily_sales/")
async def dailysales(day : str, time : int):
    daily_sales =  ventes.copy()
    daily_sales['time'] = daily_sales['time'].apply(pd.to_datetime)
    daily_sales['Dates'] = pd.to_datetime(daily_sales['time']).dt.date.apply(pd.to_datetime).dt.day_name()
    daily_sales['Time'] = pd.to_datetime(daily_sales['time']).dt.time
    daily_sales['hour'] = daily_sales['time'].dt.hour
    daily_sales.drop(columns=['item_id', 'client_id', 'time', 'Time'], inplace = True)
    daily_sales = daily_sales.groupby(['Dates', 'hour']).size().reset_index(name='ventes')
    daily_sales = daily_sales.query("Dates==@day & hour==@time")
    posts = json.loads('{"items":' + daily_sales.to_json(orient='records', date_format='iso') + '}')

    return posts

# @app.get("/daily_sales/")
# async def frequentation(age = int,
#     start_datetime: Union[datetime, None] = Body(default=None),
#     end_datetime: Union[datetime, None] = Body(default=None)):
#     ventes_clients = ventes.copy()
#     list_client = customers.copy()
#     ventes_clients['time'] = ventes_clients['time'].apply(pd.to_datetime)
#     list_client['birthdate'] = list_client['birthdate'].apply(pd.to_datetime)
#     ventes_clients = ventes_clients.drop_duplicates(subset=["client_id", "time"], keep=False)
#     freq = ventes_clients[['client_id', 'time', 'quantity']].merge(list_client[['birthdate', 'client_id', 'sex']], how = "left", on = "client_id")
#     now = pd.to_datetime('now')
#     freq['age'] = (now - freq['birthdate']).astype('<m8[Y]')
