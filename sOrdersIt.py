# from sh import Sh
from datetime import datetime, timedelta
import xmltodict
import requests
from sh import Sh
import mysql.connector
import time
from products_stockreset import stockReset


# URL_ITA = 'https://sws.spartoo.it/mp/xml_export_orders.php'
URL_COM = 'https://sws.spartoo.com/mp/xml_export_orders.php'
URL_SPARTOO_DE = 'https://sws.spartoo.de/mp/xml_export_orders.php'

LEROB_ID = 'AE6F6B21EC88DFB0'
URL_SPARTOO_CONFIRM = 'https://sws.spartoo.it/mp/xml_maj_orders.php'
URL_SPARTOO_CONFIRM_FR = 'https://sws.spartoo.com/mp/xml_maj_orders.php'
URL_SPARTOO_CONFIRM_DE = 'https://sws.spartoo.de/mp/xml_maj_orders.php'




def dbConnect(mysql_cfg):
    return mysql.connector.connect(
        user=mysql_cfg['db_user'],
        password=mysql_cfg['db_password'],
        host=mysql_cfg['host'],
        port=mysql_cfg['port'],
        database=mysql_cfg['db_name']
    )

def getSOrdersIta():
    target_date = datetime.now() - timedelta(days=10)
    URL_ITA = 'https://sws.spartoo.it/mp/xml_export_orders.php'
    data = {
        'partner': LEROB_ID, 
        'date': target_date.strftime("%Y-%m-%d:00:00:00"),
        'status': '11'
    }
    res = requests.post(URL_ITA, params=data)
  
    return xmltodict.parse(res.content)['root']['orders']

def getSOrdersDe():
    target_date = datetime.now() - timedelta(days=10)
    URL_SPARTOO_DE = 'https://sws.spartoo.de/mp/xml_export_orders.php'
    data = {
        'partner': LEROB_ID, 
        'date': target_date.strftime("%Y-%m-%d:00:00:00"),
        'status': '11'
    }
    res = requests.post(URL_SPARTOO_DE, params=data)
    return xmltodict.parse(res.content)['root']['orders']

def getSOrdersFr():
    target_date = datetime.now() - timedelta(days=10)
    URL_FR = 'https://sws.spartoo.com/mp/xml_export_orders.php'
    data = {
        'partner': LEROB_ID, 
        'date': target_date.strftime("%Y-%m-%d:00:00:00"),
        'status': '11'
    }
    res = requests.post(URL_FR, params=data)
    return xmltodict.parse(res.content)['root']['orders']

def updateOrderSpartooIt(id):
    
    data = {
        'partenaire': LEROB_ID, 
        'oID': id,
        'statut': '2'
    }
    res = requests.post(URL_SPARTOO_CONFIRM, params=data)
    return xmltodict.parse(res.content)

def updateOrderSpartooFr(id):
    
    data = {
        'partenaire': LEROB_ID, 
        'oID': id,
        'statut': '2'
    }
    res = requests.post(URL_SPARTOO_CONFIRM_FR, params=data)
    return xmltodict.parse(res.content)

def updateOrderSpartooDe(id):
    
    data = {
        'partenaire': LEROB_ID, 
        'oID': id,
        'statut': '2'
    }
    res = requests.post(URL_SPARTOO_CONFIRM_DE, params=data)
    return xmltodict.parse(res.content)



def dbCheckVariantExists(cursor, shid):
    query = "SELECT * FROM stocks WHERE shid = '" + shid + "'"
    cursor.execute(query)
    res = cursor.fetchone()
    return res != None

def dbgetIdbySku(cursor, sku):
    query = "SELECT * FROM stocks WHERE sku LIKE '%" + sku + "%'"
    cursor.execute(query)
    res = cursor.fetchone()
    return res



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

dbconn = dbConnect(cfg['mysql'])
cur = dbconn.cursor(buffered=True)
ordsIt = getSOrdersIta()
ordsFr = getSOrdersFr()
shopify = Sh()
#shopify.createOrder(ords)



if(ordsIt) :
            
           
            orders = ordsIt.get("order")

            if isinstance(orders, dict):  # Caso di un solo ordine (dizionario)
                orders = [orders]  # Convertiamo in lista per uniformità
            
            for order in orders:
                        
                
                       
                products = order.get("products", {}).get("product", [])
                if isinstance(products, dict):
                    products = [products]
                
                for product in products:
                        reference = product.get("products_size_reference", "N/A")
                        product_name = product.get("products_name", "N/A")
                        product_qty = product.get("products_qty", "N/A")
                        product_size = product.get("products_size", "N/A")
                        product_price = product.get("products_final_price", "N/A")
                        shid = dbgetIdbySku(cur,reference)
                        if shid and len(shid) > 1:
                            product["product_variant_id"] = shid[1]
                        else:
                            stockReset(reference)
                            time.sleep(2)
                            # 🔄 Forza la chiusura e la riapertura della connessione al DB
                            cur.close()
                            dbconn.close()
                            time.sleep(1)  # Aspetta un secondo per sicurezza

                            # Riapri la connessione
                            dbconn = dbConnect(cfg['mysql'])  # Usa la tua funzione per creare la connessione
                            cur = dbconn.cursor()
                            # 🔄 Loop per verificare se `shid` è stato aggiornato
                            retry_count = 5  # Numero massimo di tentativi
                            while retry_count > 0:
                                shid = dbgetIdbySku(cur, reference)
                                if shid and len(shid) > 1:
                                    break  # Esci dal loop se trovi il valore
                                print("⚠️ Attesa aggiornamento database...")
                                time.sleep(1)
                                retry_count -= 1
                            if not shid:
                                raise ValueError(f"❌ Errore: SKU {reference} non trovato dopo il reset!")
                            shid = dbgetIdbySku(cur,reference)
                        product["product_variant_id"] = shid[1]
                      
                        
               
                shopify.createOrder(order)
                updateOrderSpartooIt(order['orders_id'])

else:
    print('nessun ordine da processare')

