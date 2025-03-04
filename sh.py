# type: ignore
import shopify
import json
import os
from time import sleep
from datetime import date
from pathlib import Path
from constants import bcolors
from shopify import ApiVersion, Release
import mysql.connector

API_KEY = os.getenv("shopify_api_key")
API_SECRET_KEY = os.getenv("shopify_api_secret")
API_PASSWORD = os.getenv("shopify_api_password")
API_VERSION = '2023-04'
SHOP_URL = os.getenv("shopify_url")
LOCATION_ID = os.getenv("shopify_location_id")
ENDPOINT = 'https://' + API_KEY + ':' + API_PASSWORD + '@' + SHOP_URL + '/admin'
    
class Sh:

        
    def __init__(self):
        # set shopify site
        ApiVersion.define_version(Release("2023-04"))
        shopify.ShopifyResource.set_site(ENDPOINT)
    
    def getProd(self, shid):
        return shopify.Product.find(shid)
    
    def getCollection(self, id):
        return shopify.SmartCollection.find(id)
    
    def set_product_position(self, collection_id, product_id, position):

        document = Path("./queries.graphql").read_text()

        query_data = {
            "id": "gid://shopify/Collection/" + collection_id,
            "moves": {
                "id": "gid://shopify/Product/" + product_id,
                "newPosition": position,
            },
        }
        
        session = shopify.Session(SHOP_URL, API_VERSION, API_PASSWORD)
        
        shopify.ShopifyResource.activate_session(session)

        result = json.loads(
            shopify.GraphQL().execute(
                query=document,
                variables=query_data,
                operation_name="collectionReorderProducts",
            )
        )
        return
    
    def get_order_id_by_name(self, order_name):
        document = Path("./queries.graphql").read_text()

        query_data = {
            "name": order_name,
        }

        session = shopify.Session(SHOP_URL, API_VERSION, API_PASSWORD)

        shopify.ShopifyResource.activate_session(session)

        result = json.loads(
            shopify.GraphQL().execute(
                query=document,
                variables=query_data,
                operation_name="getOrderByName",
            )
        )

        try:
           
            order_id = result["data"]["orders"]["edges"][0]["node"]["id"].replace('gid://shopify/Order/','')
            return order_id
        except (KeyError, IndexError):
            return None
    
    def get_all_products(self, limit=100):
        get_next_page = True
        since_id = 0
        while get_next_page:
            products = shopify.Product.find(since_id=since_id, limit=limit)
            """status='active"""
            for product in products:
                yield product
                since_id = product.id

            if len(products) < limit:
                get_next_page = False

    # retrieve all orders
    def getAllOrders(self):
        orders = shopify.Order.find(status='any')
        return orders

    # retrieve all orders
    def listOrders(self, dfrom=date.today().strftime("%Y-%m-%d")):
       
        orders = shopify.Order.find(status='any', processed_at_min=dfrom)
        return orders
    
    # retrieve all orders
    def getOrdersSingle(self,id):
       
        orders = shopify.Order.find(id)
        return orders

    def getOrderInfo(self, oid):
        document = Path("queries.graphql").read_text()

        query_data = {
            "orderId": "gid://shopify/Order/" + str(oid)
        }

        session = shopify.Session(SHOP_URL, API_VERSION, API_PASSWORD)
        shopify.ShopifyResource.activate_session(session)
        result = json.loads(shopify.GraphQL().execute(
            query=document,
            variables=query_data,
            operation_name="orderInfo"
        ))
        if 'data' in result:
            return result['data']['order']
        return None

    # retrieve all products
    def getAllProducts(self, limit=200):
        get_next_page = True
        since_id = 0
        while get_next_page:
            products = shopify.Product.find(since_id=since_id, limit=limit)

            for product in products:
                yield product
                since_id = product.id

            if len(products) < limit:
                get_next_page = False

    def createProduct(self, sku, pdata):

       
        print(bcolors.OKBLUE + '[#] Creating product ' + sku + bcolors.ENDC)
        p = shopify.Product()
        p.title = sku
        p.product_type = pdata['type']
        p.body_html = pdata['description']
        p.tags = pdata['tags']
        p.vendor = pdata['vendor'] if type(pdata['vendor']) == str else ''
        p.published_scope = 'global'
        p.status = 'draft' if pdata['status'] != 1 else 'active'

        try:
            success = p.save()
            if(not success):
                print(bcolors.FAIL +
                    '[-] Product error -> ' + str(p.errors.full_messages()) + bcolors.ENDC)
            else:
                prod = shopify.Product.find(p.id)
                prod.add_metafield(
                    shopify.Metafield({
                        'key': 'colore',
                        'type': 'single_line_text_field',
                        'namespace': 'custom',
                        'value': pdata['color']
                    })
                )
                prod.save()

                # set images
                self.setImages(p.id, pdata['images'])
                if 'sizes' in pdata:
                    self.setVariants(p.id, pdata['sizes'])
                print(bcolors.OKCYAN +
                '[+] Shopify product added' + bcolors.ENDC)
                return p.id
        except Exception as e:
            print(bcolors.FAIL + '[-] Error -> ' + str(e) + bcolors.ENDC)
            print(pdata)
            exit()
        return None
    
    def createOrder(self,odata):

        
        print(bcolors.OKBLUE + '[#] Creating order ' + bcolors.ENDC)
        o = shopify.Order()
        o.email = odata['delivery']['delivery_email_address']
        o.created_at = odata['date_purchased']
        o.line_items = []
       
       # Verifica che 'products' esista e contenga 'product'
        if 'products' in odata and 'product' in odata['products']:
            products = odata['products']['product']

        # Se c'è solo un prodotto, trattiamolo come lista per uniformità
        if isinstance(products, dict):  # Caso raro in cui 'product' fosse un dizionario invece di una lista
            products = [products]

        # Iteriamo sempre sui prodotti (anche se ce n'è solo uno)
        for prodotto in products:
            line_item = {
            "variant_id": prodotto['product_variant_id'],
            "quantity": prodotto['products_qty'],
            "price": prodotto['products_price_unit']
            }
            o.line_items.append(line_item)
       
        o.financial_status = 'paid'
        o.gateway = 'spartoo'
        o.billing_address = {
                "address1": odata['customers']['customers_street_address'],
                "address2": odata['customers']['customers_suburb'],
                "city": odata['customers']['customers_city'],
                "country": odata['customers']['customers_country'],
                "first_name": odata['customers']['customers_firstname'],
                "last_name": odata['customers']['customers_lastname'],
                "phone": odata['customers']['customers_telephone'],
                "province": odata['customers']['customers_state'],
                "zip": odata['customers']['customers_postcode'],
                "name": odata['customers']['customers_firstname'] + ' ' + odata['customers']['customers_lastname'],
                "province_code": "",
                "country_code": odata['customers']['customers_country_iso'],
        }
        o.shipping_address =  {
            "address1": odata['delivery']['delivery_street_address'],
                "address2": odata['delivery']['delivery_suburb'],
                "city": odata['delivery']['delivery_city'],
                "country": odata['delivery']['delivery_country'],
                "first_name": odata['delivery']['delivery_firstname'],
                "last_name": odata['delivery']['delivery_lastname'],
                "phone": odata['delivery']['delivery_telephone'],
                "province": odata['delivery']['delivery_state'],
                "zip": odata['delivery']['delivery_postcode'],
                "name": odata['delivery']['delivery_firstname'] + ' ' + odata['delivery']['delivery_lastname'],
                "province_code": "",
                "country_code": odata['delivery']['delivery_country_iso'],
        }
        o.shipping_lines  = [{
                        "custom": 'true',
                        "price": odata['shipping_price'],
                        "title": "Spartoo shipping"
        }]

        o.tags = 'spartoo,imported'
        #o.source_name = "spartoo",
        o.source_identifier = odata['orders_id']
        o.note = 'spartoo order ' + str(odata['orders_id'])
       
        try:
            success = o.save()
            if(not success):
                print(bcolors.FAIL +
                '[-] Order error -> ' + str(o.errors.full_messages()) + bcolors.ENDC)
            else:
                print(bcolors.OKCYAN +
                '[+] Shopify order added ' + str(o.id) + bcolors.ENDC)
                return o.id
        except Exception as e:
            print(bcolors.FAIL + '[-] Error -> ' + str(e) + bcolors.ENDC)
            return None
        
    def setFirstImage(self, pid, path):
        prod = shopify.Product.find(pid)
        image = shopify.Image()
        image.product_id = pid
        image.position = 1

        with open(path, 'rb') as f:
            encoded = f.read()
            image.attach_image(encoded, path.split('/')[-1])
        
        prod.images[0] = image
        prod.save()

    def setImages(self, pid, imgs):
        prod = shopify.Product.find(pid)
        for img in imgs:
            filename = img.split('/')[-1]
            imagename = filename.split('.jpg')[0]
            image = shopify.Image()
            image.product_id = pid
            image.position = imagename.split('_')[-1]
            image.src = img

            
            #if not os.path.exists('disabled/' + filename):
                #continue

            #with open('disabled/' + filename, 'rb') as f:
                #encoded = f.read()
                #image.attach_image(encoded, filename)
            #with open(img, "rb") as f:
                #filename = filename
                #encoded = f.read()
                #image.attach_image(encoded, filename=filename)

            prod.images.append(image)
            prod.save()
        print(bcolors.OKGREEN + '[+] Images imported' + bcolors.ENDC)

    def setVariants(self, pid, sizes):
        prod = shopify.Product.find(pid)
        prod.options = [{"name": "Taglia"}]
        prod.save()

        
        variants = []
        for size in sizes:
            var = shopify.Variant()
            var.option1 = size['value']
            var.price = size['price']
            var.compare_at_price = size['compare']
            var.fullfilment_service = "manual"
            var.inventory_management = "shopify"
            var.sku = size['sku']
            var.barcode = size['barcode']
            variants.append(var)
        
        prod.variants = variants

        prod.save()

        
        sleep(1)

        variantsQuantity = []
        for v in prod.variants:
            qty = [x['qty'] for x in sizes if x['value'] == v.option1][0]
            variantsQuantity.append({
                'qty': qty,
                'inv_id': v.inventory_item_id
            })
      
        self.update_stock(v.inventory_item_id,qty)
        sleep(1)
    
    def update_stock(self, inv_id, qty):
        # time.sleep(0.5)
      
        try:
            inv_l = shopify.InventoryLevel.set(LOCATION_ID, inv_id, qty)
            return inv_l.available == qty
        except Exception as e:
            print(e)
            return False
    
    
    def updateStockS(self, data):

        document = Path("./queries.graphql").read_text()
        
        query_data = {
            
            "locationId": "gid://shopify/Location/" + LOCATION_ID,
            "inventoryItemId": "gid://shopify/InventoryItem/" + str(data['inv_id']),
            "available": data['qty']

        }
        
        
        session = shopify.Session(SHOP_URL, API_VERSION, API_PASSWORD)
        
        shopify.ShopifyResource.activate_session(session)

        
        result = json.loads(shopify.GraphQL().execute(
            query=document,
            variables=query_data,
            operation_name="ActivateInventoryItem"
        ))
        #if(not result['data']['inventoryActivate']['inventoryLevel']['id']):
            #print(bcolors.OKGREEN + '[+] Variants updated' + bcolors.ENDC)
        #else:
            #print(bcolors.FAIL + 'Error while updating stock' + bcolors.ENDC)


    def trysession(self):
         
        session = shopify.Session(SHOP_URL, API_VERSION, API_PASSWORD)
        
        shopify.ShopifyResource.activate_session(session)
        
    
    def deleteProduct(self, pid):

        document = Path("queries.graphql").read_text()

        query_data = {
            "productId": "gid://shopify/Product/" + str(pid)
        }

        session = shopify.Session(SHOP_URL, API_VERSION, API_PASSWORD)
        shopify.ShopifyResource.activate_session(session)

        result = json.loads(shopify.GraphQL().execute(
            query=document,
            variables=query_data,
            operation_name="productDeleteAsync"
        ))

        if 'data' in result:
            return True
       

    def setMetafield(self, p, key, mtype, domain, value):
        p.add_metafield(
            shopify.Metafield({
                'key': key,
                'type': mtype,
                'namespace': domain,
                'value': value
            })
        )
        p.save()

    def getVariant(self,vid):
        p = shopify.Variant.find(vid)
        return p
    
    def getProductS(self,vid):
        p = shopify.Product.find(vid)
        return p
    
    def getCountry(self):
        p = shopify.Country.find(since_id=0, limit=100)
        return p

    def updateStatus(self, p, status):
        prod = [
"BEL05991",
"BEL04904",
"ANDB9335",
"BEL05074",
"BEL04903",
"BET05352",
"BUFD6116",
"M3798MNA",
"M6111A",
"M6111F",
"M6117MC",
"M6118E",
"M6193G",
"M6308B",
"M6309B",
"M6315B",
"M6315C",
"M6321A",
"M6324B",
"M6338D",
"M6338A",
"M6347A",
"M6348A",
"M6349A",
"M6351A",
"M6381B",
"MAS05881",
"MIQD6024",
"MIQD6073",
"RUS06061",
"SHAD0273",
"RUS06063",
"31543N",
"31922A",
"31925",
"32014",
"32015",
"32018A",
"32019A",
"LAN04948",
"LAN04946",
"LUC01008",
"31938Z"
 ]
        
        p.status = status
        p.save()
        print("[+] prodotto elaborato " + str(p.id))

    def updateTag(self, prod, tag):

            p = self.getProd(prod)
            # for m in p.metafields():
                #if m.key == "prodottofinito" and m.value in prod:
                   # if 'saldi2023' not in p.tags :
                      #  p.tags += "," + tag
                        #p.save()
                      #  print("[+] prodotto elaborato " + m.value)
            p.tags += "," + tag
            p.save()
            print("[+] prodotto elaborato " + str(p.id))
            


            '''tags = p.tags.split(sep=',', maxsplit=-1)
            
            if ' outlet' in tags and ' notoutlet' in tags:
                tags.remove(' notoutlet')
                newtags = ','.join(tags)
                p.tags = newtags
                p.save()
                print("[+] prodotto da rimuovere elaborato " + str(p.id))
                
            else:
                p.tags += "," + tag
                p.save()
                print("[+] prodotto elaborato " + str(p.id))
                
            
            #for m in p.tags:
                
                exit()
                if m.key == "prodottofinito" and m.value in prod:
                   # if 'saldi2023' not in p.tags :
                        p.tags += "," + tag
                        p.save()
                        print("[+] prodotto elaborato " + m.value)'''
