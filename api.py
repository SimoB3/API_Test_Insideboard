from calendar import WEDNESDAY
from fastapi import Body, FastAPI, Query
import numpy as np
from enum import Enum
import pandas as pd
import json
from datetime import datetime, time, timedelta, date
from typing import Union
from uuid import UUID


items = pd.read_csv("api/data/items.csv")
ventes = pd.read_csv("transactions.csv")
customers = pd.read_csv("api/data/clients.csv")

customers.rename(columns={"id": "client_id"}, inplace=True)
ventes['time'] = ventes['time'].apply(pd.to_datetime)
customers['birthdate'] = customers['birthdate'].apply(pd.to_datetime)
items.rename(columns={"id": "item_id"}, inplace=True)

class DayNames(str, Enum):
    Monday = "Monday"
    Tuesday = "Tuesday"
    Wednesday = "Wednesday"
    Thursday = "Thursday"
    Friday = "Friday"
    Saturday = "Saturday"
    Sunday = "Sunday"

app = FastAPI()

@app.get("/top_ventes")
async def NTop_Sales(top: int = Query (..., ge = 0, le = 1000, description="Top N des objets les plus vendus")):
    most_sell = ventes.groupby(['item_id'])['quantity'].sum().reset_index(name='total_sold').sort_values(by='total_sold', ascending=False)
    total_ventes = items.merge(most_sell, how='inner', on='item_id')
    total_ventes.sort_values('total_sold', ascending =False, inplace = True)
    total_ventes = total_ventes.head(top)
    posts = json.loads('{"items":' + total_ventes.to_json(orient='records', date_format='iso') + '}')

    return posts

@app.get("/top_customers/")
async def NTop_Customers(top: int = Query (..., ge = 0, le = 1000, description="Top N des clients fréquentant le plus le magasin")):
    ventes_clients = ventes.copy()
    list_client = customers.copy()
    ventes_clients['time'] = pd.to_datetime(ventes_clients['time']).dt.date
    ventes_clients = ventes_clients.drop_duplicates(subset=["client_id", "time"], keep=False)
    ventes_clients = ventes_clients.groupby(['client_id'])['client_id'].size().reset_index(name='total orders')
    top_clients = list_client.merge(ventes_clients, how = "left", on ="client_id").sort_values(by='total orders', ascending=False)
    top_clients = top_clients.head(top)
    posts = json.loads('{"items":' + top_clients.to_json(orient='records', date_format='iso') + '}')

    return posts

@app.get("/daily_sales/")
async def dailysales(
    *,
    day : DayNames,
    time : int = Query (..., ge = 0, le = 24, description="Entrez une heure")):
    daily_sales =  ventes.copy()
    daily_sales['Dates'] = pd.to_datetime(daily_sales['time']).dt.date.apply(pd.to_datetime).dt.day_name()
    daily_sales['Time'] = pd.to_datetime(daily_sales['time']).dt.time
    daily_sales['hour'] = daily_sales['time'].dt.hour
    daily_sales.drop(columns=['item_id', 'client_id', 'time', 'Time'], inplace = True)
    daily_sales = daily_sales.groupby(['Dates', 'hour']).size().reset_index(name='ventes')
    daily_sales = daily_sales.query("Dates==@day & hour==@time")
    posts = json.loads('{"items":' + daily_sales.to_json(orient='records', date_format='iso') + '}')

    return posts

@app.get("/customers_age/")
async def frequentation(
    *,
    date_start : str = Query (..., description = "Date du début Min = 2020-01-01 / Max = 2020-12-31"),
    date_end : str = Query (..., description = "Date de fin Min = 2020-01-01 / Max = 2020-12-31")):

    ventes_clients = ventes.copy()
    list_client = customers.copy()
    ventes_clients['time'] = ventes_clients['time'].apply(pd.to_datetime)
    list_client['birthdate'] = list_client['birthdate'].apply(pd.to_datetime)
    ventes_clients = ventes_clients.drop_duplicates(subset=["client_id", "time"], keep=False)
    freq = ventes_clients[['client_id', 'time', 'quantity']].merge(list_client[['birthdate', 'client_id', 'sex']], how = "left", on = "client_id")
    now = pd.to_datetime('now')
    freq['age'] = (now - freq['birthdate']).astype('<m8[Y]')
    mask = (freq['time'] > date_start) & (freq['time'] <= date_end)
    freq['date'] = pd.to_datetime(freq['time']).dt.date
    freq = freq.drop_duplicates(subset=["client_id", "date"], keep=False)
    freq = freq.loc[mask]

    conditions = [
    (freq['age'] < 18),
    (freq['age'] >= 18) & (freq['age'] <= 29),
    (freq['age'] >= 30) & (freq['age'] <= 55),
    (freq['age'] > 55)
    ]
    values = ['<18', '18 - 29', '30 - 55', '> 55']

    freq['tranche'] = np.select(conditions, values)
    freq = freq.groupby(["tranche"])["tranche"].count().reset_index(name='count').sort_values(by='count', ascending=False)

    posts = json.loads('{"items":' + freq.to_json(orient='records', date_format='iso') + '}')

    return posts

@app.get("/total_spent/")
async def total_spent(
    *,
    genre : str = Query(..., description="Sélectionnez un genre entre F (Feminin) et M (Masculin)"),
    age_start : int = Query(..., description = "Âge minime de la tranche"),
    age_end : int = Query(..., description = "Âge max de la tranche"),
    date_start : str = Query (..., description = "Date du début Min = 2020-01-01 / Max = 2020-12-31"),
    date_end : str = Query (..., description = "Date de fin Min = 2020-01-01 / Max = 2020-12-31")):

    ventes_total = ventes.copy()
    prix_items = items.copy()
    total = ventes_total.merge(prix_items, how="left", on = "item_id")
    total["total_spent"] = total['price'] * total['quantity']
    total.drop(columns=['item_id', 'quantity', 'name', 'price'], inplace = True)
    total_spent = total.merge(customers, how = "left", on ='client_id')
    now = pd.to_datetime('now')
    total_spent['age'] = (now - total_spent['birthdate']).astype('<m8[Y]')
    mask = (total_spent['time'] > date_start) & (total_spent['time'] <= date_end)

    total_spent = total_spent.loc[mask].query('age>=@age_start & age<=@age_end & sex==@genre')

    column_map = {col: "first" for col in total_spent.columns}
    column_map["total_spent"] = "sum"

    total_spent = total_spent.groupby(["client_id"], as_index=False).agg(column_map).sort_values(by='total_spent', ascending=False)
    total = total_spent.groupby(["sex"], as_index=False)['total_spent'].sum()

    posts_total = json.loads('{"items":' + total.to_json(orient='records', date_format='iso') + '}')
    posts_total_spent = json.loads('{"items":' + total_spent.to_json(orient='records', date_format='iso') + '}')

    return posts_total, posts_total_spent
