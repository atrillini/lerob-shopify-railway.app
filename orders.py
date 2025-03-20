#type: ignore
import sh
import os
import mysql.connector
import csv
import io
from time import sleep
import pandas as pd
from datetime import date, timedelta
from ftplib import FTP
from minio import Minio
from minio.error import S3Error
from sh import Sh


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


def dbConnect(mysql_cfg):
    return mysql.connector.connect(
         user=mysql_cfg['db_user'],
        password=mysql_cfg['db_password'],
        host=mysql_cfg['host'],
        port=mysql_cfg['port'],
        database=mysql_cfg['db_name']
        
    )
   

def dbCheckOrderExists(cursor, soid):
    q = "SELECT * FROM `orders` WHERE soid = '" + str(soid) + "'"

    cursor.execute(q)
    res = cursor.fetchone()
    
    return res != None

def connectFTP(host,user,password):
   
    f = ftpretty(host, user, password)
    return f

def dbAddOrder(conn, cur, values):
    query = "INSERT INTO orders (soid, soname, cemail,total, status ,sync_at) VALUES (%s, %s, %s, %s, %s, %s)"
    cur.execute(query, values)
    conn.commit()

def getPaymentMethod(gateway):
   
    if gateway == "shopify_payments":
        return "CC"
    elif gateway == "paypal":
        return "PP"
    elif gateway == "Cash on Delivery (COD)":
        return "CO"
    elif gateway == "AM":
        return "AM"
    elif gateway == "amazon":
        return "AM"
    elif gateway == "bank_transfer":
        return "BO"
    elif gateway == "Scalapay":
        return "SP"

def createFile(data):
    
    # create the csv writer
    f = open('orders_to_adhoc/'+data[18].replace('#','')+'.csv', 'w')
    writer = csv.writer(f, delimiter=';')

    # write a row to the csv file
    writer.writerow(data)

    # close the file
    f.close()


def format_string_file(s):
    string_formatted = ''
    for r in s.split('\n'):
        if(not r or r == 'b'): continue
        string_formatted += r + '\n'
    return string_formatted


def write_to_file(orders_data, blob):
    #df = pd.DataFrame.from_dict(orders_data)
    #csv_str = df.to_csv(index=False, header=True, sep=';')
    output = io.StringIO()
    writer = csv.writer(output, delimiter =';', quoting=csv.QUOTE_NONE)
    writer.writerow(orders_data)


def getMappedData(orderInfo,p):

    countryCode = orderInfo['shippingAddress']['countryCodeV2']
    df = pd.read_csv("countrylist.csv", sep = ";")
    indexofresult = df[df['ccode'].str.contains(countryCode)].index[0]
    rate = df[df['ccode'].str.contains(countryCode)].rate[indexofresult]*100
    data_ordine = orderInfo['createdAt'].replace('T',' ')
    data_ordine = data_ordine.replace('Z','')
    gateway = orderInfo['transactions'][0]['gateway'] if orderInfo['transactions'] else 'SP'
    gatewaycode = getPaymentMethod(gateway)
    variantvalue = p['variant']['selectedOptions'][0]['value'].replace('.5',' 1/2')
    ordername = orderInfo['name'].replace('#','')
    if(gatewaycode == 'AM'):
        ordername = '999'+orderInfo['name'].replace('#','')
    if 'spartoo' in orderInfo['tags']:
        ordername = '888'+orderInfo['name'].replace('#','')
    mdata = []
    mdata.append(orderInfo['billingAddress']['name'])
    mdata.append("")
    mdata.append("F")
    mdata.append("")
    mdata.append(orderInfo['email'])
    mdata.append(orderInfo['billingAddress']['phone'])
    mdata.append(orderInfo['billingAddress']['address1'])
    mdata.append(orderInfo['billingAddress']['city'])
    mdata.append(orderInfo['billingAddress']['zip'])
    mdata.append(orderInfo['billingAddress']['province'])
    mdata.append(orderInfo['billingAddress']['countryCodeV2'])
    mdata.append(orderInfo['shippingAddress']['name'])
    mdata.append(orderInfo['shippingAddress']['phone'])
    mdata.append(orderInfo['shippingAddress']['address1'])
    mdata.append(orderInfo['shippingAddress']['city'])
    mdata.append(orderInfo['shippingAddress']['zip'])
    mdata.append(orderInfo['shippingAddress']['province'])
    mdata.append(orderInfo['shippingAddress']['countryCodeV2'])
    mdata.append(ordername)
    mdata.append(data_ordine)
    mdata.append(gatewaycode)
    mdata.append(orderInfo['financial_status'])
    mdata.append(orderInfo['totalShippingPriceSet']['presentmentMoney']['amount'])
    mdata.append(orderInfo['totalPriceSet']['presentmentMoney']['amount'])
    mdata.append(p['sku'].replace('-'+ p['variant']['selectedOptions'][0]['value'],''))
    mdata.append('')# mdata.append(next(iter(itm for itm in p['selectedOptions'] if item['Name'] == 'Colore'), None))
    mdata.append(variantvalue)
    mdata.append(p['quantity'])
    if(gatewaycode == 'AM') or 'spartoo' in orderInfo['tags'] or countryCode != 'IT':
        mdata.append(p['originalTotalSet']['presentmentMoney']['amount'])
    else :
        mdata.append(p['variant']['price'])
    mdata.append(rate)
    mdata.append('0.00') # discount code
    mdata.append('3.5' if gateway == 'COD' else '0')
    if(gatewaycode == 'AM'):
        mdata.append("AM")
    if('spartoo' in orderInfo['tags']):
        mdata.append("SP")
    if(gatewaycode != 'AM' and 'spartoo' not in orderInfo['tags']):
        mdata.append("SH")

    return mdata

#def sendFileToServer(fobj):


def process_orders():

    host = "93.147.145.116"
    user = "magento"
    password = "qlND5Vn@"
    shopify = sh.Sh()
    log_string = ''
    fdate = date.today() - timedelta(days=3)
    fdate = fdate.strftime("%Y-%m-%d")

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

    shopify = Sh()
     # Configura il client MinIO
    minio_client = Minio(
              endpoint="bucket-production-51bd.up.railway.app:443",  # Solo il dominio, senza https://
              access_key=os.getenv("MINIO_ROOT_USER"),       # Credenziali da variabili d'ambiente
              secret_key=os.getenv("MINIO_ROOT_PASSWORD"),
              secure=True                                    # True per HTTPS (Railway usa 443)
          )

     # Nome del bucket da usare
    bucket_name = "lerob-orders"

   
    conn = dbConnect(cfg['mysql'])
    cur = conn.cursor()

    ords = shopify.listOrders(fdate)
    
    for o in ords:

        if o.financial_status not in ['paid', 'authorized']:
            continue
        
        #if o.name != '#1603': 
            #continue
        
        
        if  dbCheckOrderExists(cur, o.id):
            continue
        
        orderInfo = shopify.getOrderInfo(o.id)
        orderInfo['financial_status'] = o.financial_status
       
        
        if not orderInfo:
            continue
        
        # create the csv writer
        #f = open('orders_to_adhoc/'+orderInfo['name'].replace('#','')+'.csv', 'w')
        #writer = csv.writer(f, delimiter=';')
       
        local_file = "/tmp/orderInfo['name'].replace('#', '').csv"
        f = open(local_file, 'w')
        writer = csv.writer(f, delimiter=';')
        
        

        for p in orderInfo['lineItems']['nodes']:
            if p['sku'] == None:
                continue
            mappedData = getMappedData(orderInfo,p)
            writer.writerow(mappedData)
            # write a row to the csv file
            #writer.writerow(mappedData)
            #write_to_file(mappedData, blob_order)
            # set public access
        f.flush()  # Forza la scrittura sul disco
        f.close()  # Chiude il file
        # Carica il file su MinIO
        object_name = orderInfo['name'].replace('#','')+'.csv'
        minio_client.fput_object(
            bucket_name=bucket_name,
            object_name=object_name,
            file_path=local_file
        )
        print(f"File '{object_name}' caricato con successo in '{bucket_name}'.")
        
        ftp = FTP(host)
        ftp.login(user,password)
        ftp.cwd('magento/ORDINI')
       
        filename = orderInfo['name'].replace('#','')+'.csv'
        with open(local_file, "rb") as file:  # Apri il file in modalità binaria
            ftp.storbinary(f"STOR {filename}", file)
        ftp.quit()
       
        print('file ordine ' + orderInfo['name'] +  ' -> ' + 'inviato nel server del cliente')

        os.remove(local_file)
        print(f"File locale '{local_file}' rimosso.")
       

        dbAddOrder(conn, cur, (o.id, orderInfo['name'], orderInfo['email'],orderInfo['totalPriceSet']['presentmentMoney']['amount'], orderInfo['createdAt'], '1'))
        sleep(1)
    return "{ordini importati correttamente}"
              
       
process_orders()


