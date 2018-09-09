from __future__ import print_function
import csv
import json
import pymongo
from pymongo import MongoClient
from collections import defaultdict
from tqdm import tqdm

def load_orders(data, fn):
    print("loading orders...")
    with open(fn, "r") as f:
        reader = csv.reader(f)
        header = next(reader)
        for row in tqdm(reader):
            order_id = row[0]
            user_id = row[1]
            order_number = row[3]
            days_since_prior_order = -1
            try:
                days_since_prior_order = float(row[-1])
            except ValueError:
                pass
            data[order_id] = {
                "order_number": order_number,
                "days_since_prior_order": days_since_prior_order,
                "products": []
            }
    return data

def attach_products(data, fn):
    print("attaching products...")
    with open(fn, "r") as f:
        reader = csv.reader(f)
        header = next(reader)
        for row in tqdm(reader):
            order_id = row[0]
            product_id = row[1]
            if order_id not in data:
                continue
            data[order_id]["products"].append(product_id)         
    return data

def transpose(data, fn):
    print("tanspose...")
    data_new = defaultdict(list)
    with open(fn, "r") as f:
        reader = csv.reader(f)
        header = next(reader)
        for row in tqdm(reader):
            order_id = row[0]
            user_id = row[1]
            data_new[user_id].append(data[order_id]) 
    return data_new

def dump2mongo(data):
    print("dumping...")
    client = MongoClient()
    db = client.instacart
    db.orders.drop()
    for k, v in tqdm(data.items()):
        doc = {"user_id": k,
             "orders": v}
        db.orders.insert_one(doc)
    db.orders.create_index([("user_id", pymongo.ASCENDING)],
                            unique=True)
    print(db.orders.index_information())
    return 0

if __name__=="__main__":

    fn_orders = "data/orders.csv"
    fn_order_products_1 = "data/order_products__prior.csv"
    fn_order_products_2 = "data/order_products__train.csv"

    data = {}
    data = load_orders(data, fn_orders)
    data = attach_products(data, fn_order_products_1)
    data = attach_products(data, fn_order_products_2)
    data = transpose(data, fn_orders)
    dump2mongo(data)
    


