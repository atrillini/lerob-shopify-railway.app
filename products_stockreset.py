#type: ignore
import pandas as pd
from sh import Sh
import datetime
import math
from time import sleep
from constants import bcolors
import mysql.connector

def dbConnect(mysql_cfg):
    return mysql.connector.connect(
        user=mysql_cfg['db_user'],
        password=mysql_cfg['db_password'],
        host=mysql_cfg['host'],
        database=mysql_cfg['db_name']
    )

def dbAddProduct(conn, cur, values):
    query = "INSERT INTO products (shid, sku, sync_at, status) VALUES (%s, %s, %s, %s)"
    cur.execute(query, values)
    conn.commit()

def dbDeleteStock(conn, cur, sku):
    query = "DELETE from stocks where sku LIKE '%"+sku+"%'"
    cur.execute(query)
    conn.commit()

def dbDeleteProduct(conn, cur, sku):
    query = "DELETE from products where sku LIKE '%"+sku+"%'"
    cur.execute(query)
    conn.commit()

def dbCheckProductExists(cursor, sku):
    query = "SELECT * FROM products WHERE sku = '" + sku + "' LIMIT 0,1"
    cursor.execute(query)
    res = cursor.fetchone()
    return res

def dbAddProductStock(conn, cur, values):
    query = "INSERT INTO stocks (shid, invid, qty, sku, sync_at) VALUES (%s, %s, %s, %s, %s)"
    cur.execute(query, values)
    conn.commit()

def mapData(fpath):
    prodsdf = pd.read_csv(fpath, sep=',')
    pdict = {}
    variantskus = []
    for i, pdata in prodsdf.iterrows():
        if pdata['Ptype'] == 'configurable':
            sku = pdata['Variant SKU'].upper()
            pdict[sku] = {}
            pdict[sku]['title'] = pdata['Title']
            pdict[sku]['description'] = '<p>' + pdata['Body HTML'].replace(' -', "<br>-") + '</p>' if type(pdata['Body HTML']) == str else ''
            pdict[sku]['type'] = pdata['Type'] if type(pdata['Type']) == str else ''
            pdict[sku]['tags'] = pdata['Type'].lower() if type(pdata['Type']) == str else ''
            if 'Donna' in pdata['Title']:
                pdict[sku]['tags'] += ',donna'
            if 'Uomo' in pdata['Title']:
                pdict[sku]['tags'] += ',uomo'
            if 'Unisex' in pdata['Title']:
                pdict[sku]['tags'] += ',unisex'
            if 'Bambino' in pdata['Title']:
                pdict[sku]['tags'] += ',bambino'
            pdict[sku]['vendor'] = pdata['Vendor']
            pdict[sku]['status'] = int(pdata['Status'])
            pdict[sku]['images'] = pdata['Image Src'].split(';')
            pdict[sku]['color'] = pdata['Color'] if type(pdata['Color']) == str else ''
            if sku[2] == 'S':
                pdict[sku]['tags'] += ',scarpe'
            if sku[2] == 'B':
                pdict[sku]['tags'] += ',borse'
            if sku[2] == 'C':
                pdict[sku]['tags'] += ',abbigliamento'
            if sku[2] == 'A':
                pdict[sku]['tags'] += ',accessori'
        else:
            sku_parent = '-'.join(pdata['Variant SKU'].split('-')[:-1]).upper()
            if not sku_parent in pdict:
                print('parent not present -> ' + sku_parent)
                continue 
            if not 'sizes' in pdict[sku_parent]:
                pdict[sku_parent]['sizes'] = []
            if pdata['Variant SKU'] in variantskus:
                continue
            variantskus.append(pdata['Variant SKU'])
            pdict[sku_parent]['sizes'].append({
                'sku': pdata['Variant SKU'].upper(),
                'value': pdata['Variant SKU'].split('-')[-1],
                'price': pdata['Variant Price'],
                'compare': pdata['Variant Compare At Price'] if not math.isnan(pdata['Variant Compare At Price']) else 0.0,
                'qty': int(pdata['Variant Inventory Qty']),
                'barcode': pdata['Variant Barcode'] if type(pdata['Variant Barcode']) == str else ''
            })
    return pdict

cfg = {
    'mysql': {
        'host': '35.205.119.178',
        'db_user': 'root',
        'db_password': '8iNL4BM7ij7HsFPE',
        'db_name': 'lerob_orders',
        'orders_table': 'orders'
    }
}
prods = mapData('https://oldstore.lerobshop.com/import/csv/export_man.csv')
dbconn = dbConnect(cfg['mysql'])
cur = dbconn.cursor()
shopify = Sh()


def stockReset(sku):

    #sku = ("POBMNCW8491PEC0199","ccc")

        sku = sku.split("-")[0]
        ex = dbCheckProductExists(cur,sku)

        if(ex):
            shid = ex[1]
        else:
            print(sku + ' non esiste')
            
        
        dbDeleteStock(dbconn,cur,sku)
        shp = shopify.getProd(shid)
            
        for v in shp.variants:
                viinvitemd = v.inventory_item_id
                vid = v.id
                qty = v.inventory_quantity
                sku = v.sku
                dbAddProductStock(dbconn, cur, (vid, viinvitemd, qty,sku, datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')))
                print(bcolors.OKGREEN + 'Variants product ' + str(vid) +  ' -> ' + 'updated' + bcolors.ENDC)

        dbconn.commit()