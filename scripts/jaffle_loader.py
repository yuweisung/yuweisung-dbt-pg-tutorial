import psycopg2
import os
from configparser import ConfigParser

def config(filename='../postgres.ini', section='postgresql'):
    parser = ConfigParser()
    parser.read(filename)

    db = {}
    if parser.has_section(section):
        params = parser.items(section)
        for param in params:
            db[param[0]] = param[1]
    else:
        raise Exception('Section {0} not found in {1} file.'.format(section, filename))
    
    return db


SQL_COPY = """
    COPY %s FROM STDIN
    WITH CSV
    HEADER
    DELIMITER AS ','
"""


params = config()
conn = psycopg2.connect(**params)
cur = conn.cursor()

cur.execute("""
        DROP TABLE IF EXISTS raw_customers;
        CREATE TABLE raw_customers(id text, name text);
""")
f = open(r'../../jaffle/raw/raw_customers.csv', 'r')
cur.copy_expert(sql=SQL_COPY % 'raw_customers', file=f)

cur.execute("""
        DROP TABLE IF EXISTS raw_orders;
        CREATE TABLE raw_orders(id text, customer text, ordered_at text, store_id text, subtotal text, tax_paid int, order_total int);
""")
f = open(r'../../jaffle/raw/raw_orders.csv', 'r')
cur.copy_expert(sql=SQL_COPY % 'raw_orders', file=f)

cur.execute("""
        DROP TABLE IF EXISTS raw_order_items;
        CREATE TABLE raw_order_items(id text, order_id text, sku text);
""")
f = open(r'../../jaffle/raw/raw_order_items.csv', 'r')
cur.copy_expert(sql=SQL_COPY % 'raw_order_items', file=f)

cur.execute("""
        DROP TABLE IF EXISTS raw_products;
        CREATE TABLE raw_products(sku text, name text, type text, price int, description text);
""")
f = open(r'../../jaffle/raw/raw_products.csv', 'r')
cur.copy_expert(sql=SQL_COPY % 'raw_products', file=f)

cur.execute("""
        DROP TABLE IF EXISTS raw_supplies;
        CREATE TABLE raw_supplies(id text, name text, cost int, perishable boolean, sku text);
""")
f = open(r'../../jaffle/raw/raw_supplies.csv', 'r')
cur.copy_expert(sql=SQL_COPY % 'raw_supplies', file=f)

cur.execute("""
        DROP TABLE IF EXISTS raw_stores;
        CREATE TABLE raw_stores(id text, name text, opened_at text, tax_rate float);
""")
f = open(r'../../jaffle/raw/raw_stores.csv', 'r')
cur.copy_expert(sql=SQL_COPY % 'raw_stores', file=f)

conn.commit()
conn.close()