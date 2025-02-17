import requests
import pandas as pd
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
    'TIKI_RECENTLYVIEWED': '174071720',
}

headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127.0.0.0 Safari/537.36',
    'Accept': 'application/json, text/plain, */*',
    'Accept-Language': 'en-US,en;q=0.9,vi;q=0.8',
    'Referer': 'https://tiki.vn/sua-rua-mat-hada-labo-cho-da-dau-mun-da-nhay-cam-hada-labo-acne-care-calming-cleanser-80g-p174071718.html?itm_campaign=CTP_YPD_TKA_PLA_UNK_ALL_UNK_UNK_UNK_UNK_X.295731_Y.1878051_Z.3974808_CN.Key-SKUs-l-CSDM---Sua-rua-mat&itm_medium=CPC&itm_source=tiki-ads&spid=174071720',
    'x-guest-token': 'ajFhGKNXUClxEH5YtfeSzO1T68JZ0ri3',
    'Connection': 'keep-alive',
    'TE': 'Trailers',
}

params = {
    'product_id': '174071720',
    'sort': 'score|desc,id|desc,stars|all',
    'page': '1',
    'limit': '10',
    'include': 'comments'
}

def comment_parser(json):
    d = dict()
    d['id'] = json.get(id)
    d['title'] = json.get('title')
    d['content'] = json.get('content')
    d['thank_count'] = json.get('thank_count')
    d['customer_id']  = json.get('customer_id')
    d['rating'] = json.get('rating')
    d['created_at'] = json.get('created_at')
    d['customer_name'] = json.get('created_by') and json.get('created_by').get('name') or ''
    d['purchased_at'] = json.get('created_by') and json.get('created_by').get('purchased_at') or ''
    return d


df_id = pd.read_csv('crawl_product_id.csv')
p_ids = df_id.id.to_list()
result = []
for pid in tqdm(p_ids, total=len(p_ids)):
    params['product_id'] = pid
    print('Crawl comment for product {}'.format(pid))
    for i in range(2):
        params['page'] = i
        response = requests.get('https://tiki.vn/api/v2/reviews', headers=headers, params=params, cookies=cookies)
        if response.status_code == 200:
            print('Crawl comment page {} success!!!'.format(i))
            for comment in response.json().get('data'):
                result.append(comment_parser(comment))
df_comment = pd.DataFrame(result)
df_comment.to_csv('comment_data.csv', index=False, encoding='utf-8-sig')