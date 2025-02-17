import pandas as pd
import requests
import time
import random
from tqdm import tqdm

cookies = {
    'TOKENS': '%22access_token%22:%22vV6ptogO7JAMPx2kbwTf0aYsD5j3lrqh%22', 
    '_trackity': '311e1d90-39a2-e3dd-f0e6-d5fc1778f0bc', 
    '_ga': 'GA1.1.1133572030.1704718402', 
    '_gcl_au': '1.1.1222323761.1704718407', 
    '__RC': '31', 
    '__iid': '749', 
    '__iid':'749', 
    '__su':'0', 
    '__su':'0', 
    '__R':'2', 
    '__tb':'0', 
    '_fbp':'fb.1.1704718614673.1291562083', 
    'TIKI_RECOMMENDATION':'d22ad0614d70ba56b69221f28695bc23', 
    'TKSESSID': 'e249a4ee829e6cd8a1cb34d235892bec', 
    '_hjSessionUser_522327': 'eyJpZCI6IjFmYmM1YmY3LTY2YzYtNTAyOC05YWVjLTI1ZTBjZmJiMzYwNyIsImNyZWF0ZWQiOjE3MDQ3MTg0MDc3MDcsImV4aXN0aW5nIjp0cnVlfQ==', 
    'dtdz': '1400d2c9-f4da-4081-a82d-e5411ca790bf', 
    '__IP': '1953398500', 
    'delivery_zone': 'Vk4wMjUwMDMwMDM=', 
    'tiki_client_id': '1133572030.1704718402', 
    '_hjIncludedInSessionSample_522327': '0', 
    '_hjSession_522327': 'eyJpZCI6IjYzYmY4N2YxLTA0NDQtNDhkMS1iMTA2LTVlYzg1ZjkxZTUwYiIsImMiOjE3MDQ4MDc2MjU5NzgsInMiOjAsInIiOjAsInNiIjowfQ==',
    '_hjAbsoluteSessionInProgress': '1', 
    '_ga_S9GLR1RQFJ': 'GS1.1.1704807621.3.1.1704807936.24.0.0', 
    'amp_99d374': '5Tr1pO7ScgsiiJRDIYrq-V...1hjn77l91.1hjn7hfid.8d.91.he', 
    '__uif': '__uid%3A7840491781953387943%7C__ui%3A1%252C5%7C__create%3A1694049178', 
    'cto_bundle': 'C8_ZzV9tZ29aU2tSRTY1SXFVaXJ1Mll5RXlEVHhKUEszQ3Q1T2tIR085cTRONDRoNjBjSnVMR28lMkJPR2NwZkJ0ak1meldzbzdMeXhCTHdFUDIwRndFWWlLQSUyRlZaMkhCRmZNcDJ4dnZwRVJ3cGUlMkZnRWxFSkh6YzNEUnFsOWt6VkNvM1piMktMblZIZ1NYTEJCZFltSjgxVHoyTlJsalF6YlRXYWFkOXRSTXNxJTJCMnZGcUl2dDRtRHUxOHZtS2NsM3NNdzV3U09PdWRyJTJGckVDek1mQ2kwWGlpNmwyZyUzRCUzRA'
    }

headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Accept': 'application/json, text/plain, */*',
    'Accept-Language': 'vi,en-US;q=0.9,en;q=0.8,vi-VN;q=0.7,fr-FR;q=0.6,fr;q=0.5',
    'Referer': 'https://tiki.vn/may-doc-sach-all-new-kindle-paperwhite-5-11th-gen-hang-nhap-khau-p125182567.html?itm_campaign=CTP_YPD_TKA_PLA_UNK_ALL_UNK_UNK_UNK_UNK_X.263120_Y.1845440_Z.3817081_CN.Kindle&itm_medium=CPC&itm_source=tiki-ads&spid=225920030',
    'x-guest-token': 'vV6ptogO7JAMPx2kbwTf0aYsD5j3lrqh',
    'Connection': 'keep-alive',
    'TE': 'Trailers',
}
    
params = (
    ('platform', 'web'),
    ('spid', 273549657),
    ('version', 3)
    #('include', 'tag,images,gallery,promotions,badges,stock_item,variants,product_links,discount_tag,ranks,breadcrumbs,top_features,cta_desktop'),
)

def parser_product(json):
    id = (json.get('id') if json else "")
    sku = (json.get('sku') if json else "")
    short_url = (json.get('short_url') if json else "")
    price = (json.get('price') if json else "")
    list_price = (json.get('list_price') if json else "")
    discount = (json.get('discount') if json else "")
    discount_rate = (json.get('discount_rate') if json else "")
    review_count = (json.get('review_count') if json else "")
    order_count = (json.get('order_count') if json else "")
    inventory_status = (json.get('inventory_status') if json else "")
    stock_item_qty = (json.get('stock_item').get('qty') if json.get('stock_item') else "") 
    stock_item_max_sale_qty = (json.get('stock_item').get('max_sale_qty') if json.get('stock_item') else "") 
    product_name = (json.get('meta_title') if json else "")
    brand_id = (json.get('brand').get('id') if json.get('brand') else "")
    brand_name = (json.get('brand').get('name') if json.get('brand') else "")
    
    return {'id': id, 'sku': sku, 'short_url': short_url
            , 'price': price, 'list_price': list_price
            , 'discount': discount, 'discount_rate': discount_rate,'review_count': review_count, 'order_count': order_count
            , 'inventory_status': inventory_status, 'stock_item_qty': stock_item_qty, 'stock_item_max_sale_qty': stock_item_max_sale_qty, 'product_name': product_name
            , 'brand_id': brand_id, 'brand name': brand_name
            }

df_id = pd.read_csv('product_id_ncds.csv')
p_ids = df_id.id.to_list()
print(p_ids)

result = []
for pid in tqdm(p_ids, total=len(p_ids)):
    try:
        response = requests.get('https://tiki.vn/api/v2/products/{}'.format(pid), headers=headers, params=params, cookies=cookies)
        response.raise_for_status()
        if response.status_code == 200:
            json_content = response.json()
        else:
            print(f'Failed to request url for {pid}: {response.status_code}')
            continue
    except requests.exceptions.RequestException as req_err:
        print(f'Request Exception error in {pid}: {req_err}')
        continue
    except Exception as err:
        print(f'Error in {pid}: {err}')
        continue
    try:
        product_content = parser_product(json_content)
        print('Crawl data {} success !!!'.format(pid))
    except Exception as e:
        print(f'Error {e} occurred in {pid}')
        continue
    result.append(product_content)
    time.sleep(random.randrange(10, 20))
df_product = pd.DataFrame(result)
df_product.to_csv('crawled_data_ncds.csv', index=False, encoding = 'utf-8-sig')
