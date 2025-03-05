#type: ignore
import pandas as pd
from sh import Sh
import datetime
import json
import requests
import math
import time
import mysql.connector
from constants import bcolors
from products_stockreset import stockReset


def dbConnect(mysql_cfg):
    return mysql.connector.connect(
        user=mysql_cfg['db_user'],
        password=mysql_cfg['db_password'],
        host=mysql_cfg['host'],
        port=mysql_cfg['port'],
        database=mysql_cfg['db_name']
    )

def dbCheckVariantExists(cursor, sku):
    query = "SELECT * FROM stocks WHERE sku = '" + sku + "'"
    cursor.execute(query)
    res = cursor.fetchone()
    return res

def dbgetIdbySku(cursor, sku):
    query = "SELECT * FROM products WHERE sku = '" + sku + "'"
    cursor.execute(query)
    res = cursor.fetchone()
    return res

def dbUpdateQtytocks(conn, cur, shid, qty):
    query = "UPDATE stocks SET qty = '" + str(qty) + "' WHERE shid = '" + shid + "'"

    cur.execute(query)
    conn.commit()

def dbAddProduct(conn, cur, values):
    query = "INSERT INTO stocks (shid, invid, qty, sku, sync_at) VALUES (%s, %s, %s, %s, %s)"
    cur.execute(query, values)
    conn.commit()

def mapStocksData(data):
    stocksd = {}
    for row in data:
        
        sku = row['ART'].strip()
        arrayindex = sku+'#'+str(row['ID'])
        stocksd[arrayindex] = []
        for m in range(1, 31):
            stocksd[arrayindex].append({
                'size': row['M' + "{0:0=2d}".format(m)].replace(' 1/2', '.5').strip(),
                'qty': int(row['Q' + "{0:0=2d}".format(m)])
            })
    
   
    return stocksd

def getUpdatedStocks():
    url = "http://93.147.145.116:11111/SRWebApi/api/SALDI"
    r = requests.get(url)
    if not r.content:
        return None
    res = mapStocksData(json.loads(r.content))
    return res

def mapData(fpath):
    dtype = {"Variant Barcode": str}
    prodsdf = pd.read_csv(fpath, sep=',',dtype=dtype) 
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
        'host': 'crossover.proxy.rlwy.net',
        'db_user': 'root',
        'db_password': 'MktJYSAiWiGSTrRiyxZgTuFvVEHVbQiR',
        'port':'22694',
        'db_name': 'railway',
        'orders_table': 'orders',
       
    }
}

'''prods = mapData('export-1809.csv')
for sku, data in prods.items():
    for var in data['sizes']:
        sku = var['sku']
        qty = var['qty']
        size = var['value']'''
        


dbconn = dbConnect(cfg['mysql'])
cur = dbconn.cursor(buffered=True)
shopify = Sh()
stocks = getUpdatedStocks()

for key in stocks:
     
     splittata = key.split('#')
     sku = splittata[0]
     idgestionale = splittata[1]
     print('sto traddando sku '+ str(sku))
     #if (sku != 'LMSSNSW15192EC0079'):
         #continue
     
     
     find = dbgetIdbySku(cur,sku)
     if not find:
         print('non ho trovato la referenza '+str(sku))
         continue
     
     for sizes in stocks[key]:
            splittata = key.split('#')
            sku = splittata[0]
            #if(sku == 'B7SSNSW01251PE0068'):
                #continue
            #idgestionale = splittata[1]
            skuvariant = sku + '-' + sizes['size']
           
            resvariant = dbCheckVariantExists(cur,skuvariant)
            
            if not resvariant:
                if(int(sizes['qty'])>0):
                    #print('non ho trovato la variante '+str(skuvariant)+' che ha quantità' +str(sizes['qty']))
                    with open("output.log", "a", encoding="utf-8") as file:
                        file.write(str(skuvariant)+"|"+str(sizes['qty'])+"\n")
                continue
                '''
                stockReset(skuvariant)
                time.sleep(2)
                # 🔄 Forza la chiusura e la riapertura della connessione al DB
                cur.close()
                dbconn.close()
                time.sleep(1)  # Aspetta un secondo per sicurezza
                # Riapri la connessione
                dbconn = dbConnect(cfg['mysql'])
                cur = dbconn.cursor(buffered=True)
                '''
            #resvariant = dbCheckVariantExists(cur,skuvariant)
            '''
            if not resvariant:
               
            '''
            data = {}
            data['inv_id'] = resvariant[2]
            data['qty'] = sizes['qty'] 
           
            print(bcolors.OKGREEN + 'aggiorno giacenza prodotto ' + str(resvariant[5]) +  ' -> ' + 'nuova giacenza ' + str(data['qty']) +  bcolors.ENDC)
            shopify.update_stock(data['inv_id'],data['qty'])
            dbUpdateQtytocks(dbconn,cur,resvariant[1],data['qty'])
            
     requests.delete('http://93.147.145.116:11111/SRWebApi/api/SALDI/'+str(idgestionale))
     print(bcolors.OKGREEN + 'cancello da gestionale la referenza id  ' + str(idgestionale) + bcolors.ENDC)
     

         

'''for p in prods:
        shid = dbgetIdbySku(cur,p)[1]
        shp = shopify.getProd(shid)
        
        for v in shp.variants:
            viinvitemd = v.inventory_item_id
            vid = v.id
            qty = v.inventory_quantity
            sku = v.sku
            
            dbAddProduct(dbconn, cur, (vid, viinvitemd, qty,sku, datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')))
            print(bcolors.OKGREEN + 'Variants product ' + str(vid) +  ' -> ' + 'updated' + bcolors.ENDC)'''
                
        

