#type: ignore
from sh import Sh
import mysql.connector
from bs4 import BeautifulSoup
from google.cloud import storage
from time import sleep



def dbConnect(mysql_cfg):
    return mysql.connector.connect(
        user=mysql_cfg['db_user'],
        password=mysql_cfg['db_password'],
        host=mysql_cfg['host'],
        port=mysql_cfg['port'],
        database=mysql_cfg['db_name']
    )

def generateFeed(prods):
    storage_client = storage.Client()
    bucket = storage_client.get_bucket('lerob-shopify')

    blob = bucket.blob('feed.xml')
    
    cfg = {
    'mysql': {
        'host': '10.65.16.3',
        'db_user': 'root',
        'db_password': '8iNL4BM7ij7HsFPE',
        'db_name': 'lerob_orders',
        'orders_table': 'orders'
    }
    }

    dbconn = dbConnect(cfg['mysql'])
    cur = dbconn.cursor()
    feed = '<?xml version="1.0"?>'
    feed += '<root><products>'
    for prod in prods:
       

        if prod.status != 'active' or 'spartoo' not in prod.tags:
            continue
        sku = prod.title
        priceData = getPrice(prod.variants)
       
        model = sku[3:5]
        codeSpartoo = getModelSpartoo(cur,model)
        if codeSpartoo == '':
            print('non ho trovato il code spartoo per ' + str(prod.variants))
        else: 
            codeSpartoo = codeSpartoo[7]
        namePr = prod.product_type + ' ' + prod.vendor + ' ' + getGenderName(sku) 
        descr = prod.body_html
        soup = BeautifulSoup(descr,features="html.parser").get_text()
        feed += '<product>'
        
        feed += '<reference_partenaire>' + sku + '</reference_partenaire>'
        feed += '<product_name>' + namePr + '</product_name>'
        feed += '<manufacturers_name>' + prod.vendor + '</manufacturers_name>'
        feed += '<product_sex>' + getGender(sku) + '</product_sex>'
        if priceData['compare'] and float(priceData['compare']) > 0:
            feed += '<product_price>' + priceData['compare'] + '</product_price>'
            feed += '<discount>'
            feed += '<startdate>1719370800</startdate>'
            feed += '<stopdate>1721771999</stopdate>'
            feed += '<price_discount>' + priceData['price'] + '</price_discount>'
            feed += '<sales>1</sales>'
            feed += '</discount>'
        else:
            feed += '<product_price>' + priceData['price'] + '</product_price>'
        feed += '<product_quantity>' + getQuantity(prod.variants) + '</product_quantity>'
        feed += '<color_id></color_id>'
        feed += '<product_style>' +str(codeSpartoo)+  '</product_style>'
        feed += '<languages>'
       
        for lang in ['IT','FR','ES','DE']:
            feed += '<language>'
            feed += '<code>' + lang + '</code>'
            feed += '<product_name>'+namePr+'</product_name>'
            feed += '<product_description>'+soup+'</product_description>'
            feed += '<product_color></product_color>'
            if priceData['compare'] and float(priceData['compare']) > 0:
                feed += '<product_price>' + priceData['compare'] + '</product_price>'
                feed += '<discount>'
                feed += '<price_discount>' + priceData['price'] + '</price_discount>'
                feed += '<sales>1</sales>'
                feed += '</discount>'
            else:
                feed += '<product_price>' + priceData['price'] + '</product_price>'
            feed += '</language>'
        feed += '</languages>'
        feed += '<product_description>'+soup+'</product_description>'
        feed += '<product_color>' + getColor(prod.metafields()) + '</product_color>'
        feed += '<size_list>'
        for v in prod.variants:
            feed += '<size>'
            if v.option1 == 'TU':
                feed += '<size_name>TU</size_name>'
            else:
                feed += '<size_name>' + v.option1 + '</size_name>'
            feed += '<size_quantity>' + str(v.inventory_quantity) + '</size_quantity>'
            feed += '<size_reference>' + v.sku + '</size_reference>'
            if v.barcode:
                feed += '<ean>' + v.barcode + '</ean>'
            else:
                feed += '<ean></ean>'
            feed += '</size>'
        feed += '</size_list>'
        feed += '<photos>'
        i = 0
        for img in prod.images:
            i += 1
            feed += '<url' + str(i) + '>' + img.src + '</url' + str(i) + '>'
        feed += '</photos>'
        feed += '</product>'
        sleep(1)
    feed += '</products></root>'
    blob.upload_from_string(feed)
    blob.acl.reload()
    acl = blob.acl
    acl.all().grant_read()
    acl.save()
    

def getProductSku(variants):
    if not variants:
        return ''
    return variants[0].sku.split('-')[0]

def getModelSpartoo(cursor,model):
    query = "SELECT * FROM modelli WHERE code = '" + model + "'"
    cursor.execute(query)
    res = cursor.fetchone()

    if not res:
        res = ''
        return res
    else :
        return res
    
    

def getGender(sku):
    gcode = sku[6:7]
    if gcode == 'M':
        return 'H'
    if gcode == 'W':
        return 'F'
    if gcode == 'U':
        return 'F'
    return gcode

def getGenderName(sku):
    gcode = sku[6:7]
    if gcode == 'M':
        return 'Uomo'
    if gcode == 'W':
        return 'Donna'
    if gcode == 'U':
        return 'Unisex'
    return gcode

def getPrice(variants):
    if not variants:
        return {'price': '', 'compare': ''}
    price = float(variants[0].price)
    cprice = float(variants[0].compare_at_price) if variants[0].compare_at_price else 0
    if cprice == '0.0':
        cprice = 0
    
    return {
        'price': str(round(price + ((price * 10) / 100),2)),
        'compare': str(round(cprice + ((cprice * 10) / 100),2))
    }

def getQuantity(variants):
    if not variants:
        return 0
    sumqty = 0
    for v in variants:
        sumqty += v.inventory_quantity
    return str(sumqty)

def getColor(metafields):
    for m in metafields:
        if m.key == 'color':
            return m.value
    return ''

def mainFeed():
    
    shopify = Sh()
    prods = shopify.getAllProducts()
    feed = generateFeed(prods)
    return 'feed generato correttamente'
