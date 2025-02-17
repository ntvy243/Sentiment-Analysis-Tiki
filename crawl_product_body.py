import pandas as pd
import requests
import time
import random
from tqdm import tqdm

cookies = {
    'TIKI_GUEST_TOKEN': 'ajFhGKNXUClxEH5YtfeSzO1T68JZ0ri3',
    'TOKENS': '{%22access_token%22:%22ajFhGKNXUClxEH5YtfeSzO1T68JZ0ri3%22}',
    'amp_99d374': 'mO_vQM2W5kPeHvGZ5hYtdB...1i5pdesme.1i5pdg16k.de.hj.v1',
    'amp_99d374_tiki.vn': 'eSc-_0HT1um7cb57E7dwA0...1enloc6a2.1enlocds8.0.1.1',
    '_gcl_au': '1.1.1932700437.1720534344',
    '_ants_utm_v2': '',
    '_pk_id.638735871.2fc5': 'b92ae025fbbdb31f.1605974236.1.1605974420.1605974236.',
    '_pk_ses.638735871.2fc5': '*',
    '_trackity': '4df6a159-9dd9-4386-04e0-f8e7e7cf163f',
    '_ga_S9GLR1RQF': 'GS1.1.1724208609.14.1.1724208645.24.0.0',
    '_ga': 'GA1.1.794254210.1720534341',
    'ai_client_id': '11935756853.1605974227',
    'an_session': 'zizkzrzjzlzizqzlzqzjzdzizizqzgzmzkzmzlzrzmzgzdzizlzjzmzqzkznzhzhzkzdzhzdzizlzjzmzqzkznzhzhzkzdzizlzjzmzqzkznzhzhzkzdzjzdzhzqzdzizd2f27zdzjzdzlzmzmznzq',
    'au_aid': '11935756853',
    'dgs': '1605974411%3A3%3A0',
    'au_gt': '1605974227146',
    '_ants_services': '%5B%22cuid%22%5D',
    '__admUTMtime': '1605974236',
    '__iid': '749',
    '__su': '0',
    '_bs': 'bb9a32f6-ab13-ce80-92d6-57fd3fd6e4c8',
    '_gid': 'GA1.2.867846791.1605974237',
    '_fbp': 'fb.1.1605974237134.1297408816',
    '_hjid': 'f152cf33-7323-4410-b9ae-79f6622ebc48',
    '_hjFirstSeen': '1',
    '_hjIncludedInPageviewSample': '1',
    '_hjAbsoluteSessionInProgress': '0',
    '_hjIncludedInSessionSample': '1',
    'tiki_client_id': '794254210.1720534341',
    '__gads': 'ID=ae56424189ecccbe-227eb8e1d6c400a8:T=1605974229:RT=1605974229:S=ALNI_MZFWYf2BAjzCSiRNLC3bKI-W_7YHA',
    'proxy_s_sv': '1605976041662',
    'TKSESSID': '8bcd49b02e1e16aa1cdb795c54d7b460',
    'TIKI_RECOMMENDATION': '21dd50e7f7c194df673ea3b717459249',
    '_gat': '1',
    'cto_bundle': '4PbN0F9ERSUyRklxSzRzVllieW9IUnNHb1IyTE1SaHJrOTg1S2NSbTFSbm5EQ0FaQkNiTjBoeFdaazMyY0N4JTJGdTZiVE1lUlFEWCUyRkMxQmpJbG9vdHd5UjFlTXJRcVNXbGFVS0NoUldvJTJCMEIxYzBId0hEMyUyRnlWV3J0djgzSmNTQ0dxTVZvNE13UjQxRmpBdkRsbnRMd3RWU3pBOUxzS0lDWEtIWkFYc08wN0JEZjM1Q0hVT1hnUVkwYTJ2eGhnRHZCQjlWNTNEeElzUkFGczRwV3hyWnI4aGpTJTJGRW1SWEdWQVVrWU9pN01zUSUyQmxuQm1kdDJDWjBzeVBvenJPekxOaGpKTXEwR1g',
    'TIKI_RECENTLYVIEWED': '58259141',
}

headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127.0.0.0 Safari/537.36',
    'Accept': 'application/json, text/plain, */*',
    'Accept-Language': 'en-US,en;q=0.9,vi;q=0.8',
    'Referer': 'https://tiki.vn/sua-tam-lashe-superfood-duong-am-chuyen-sau-640g-p208120829.html?itm_campaign=CTP_YPD_TKA_PLA_UNK_ALL_UNK_UNK_UNK_UNK_X.279834_Y.1862154_Z.3886455_CN.Slow-moving-1&itm_medium=CPC&itm_source=tiki-ads&spid=208120830',
    'x-guest-token': 'ajFhGKNXUClxEH5YtfeSzO1T68JZ0ri3',
    'Connection': 'keep-alive',
    'TE': 'Trailers',
}

params = (
    ('platform', 'web'),
    ('spid', 208120830)
    #('include', 'tag,images,gallery,promotions,badges,stock_item,variants,product_links,discount_tag,ranks,breadcrumbs,top_features,cta_desktop'),
)

def parser_product(json):
    id = (json.get('id') if json else "")
    sku = (json.get('sku') if json else "")
    name = (json.get('name') if json else "")
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
    brand_id = (json.get('brand').get('id') if json.get('brand') else "")
    brand_name = (json.get('brand').get('name') if json.get('brand') else "")
    
    return {'id': id, 'sku': sku, 'name' : name,'short_url': short_url
            , 'price': price, 'list_price': list_price
            , 'discount': discount, 'discount_rate': discount_rate,'review_count': review_count, 'order_count': order_count
            , 'inventory_status': inventory_status, 'stock_item_qty': stock_item_qty, 'stock_item_max_sale_qty': stock_item_max_sale_qty
            , 'brand_id': brand_id, 'brand name': brand_name
            }

df_id = pd.read_csv('CrawlData/crawl_productbody_id.csv')
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
df_product.to_csv('crawled_dataproductbody.csv', index=False, encoding = 'utf-8-sig')