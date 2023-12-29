import requests
from my_base import My_base
from datetime import date, timedelta, datetime
from sqlalchemy import create_engine
from urllib.parse import quote_plus
import pandas as pd
import time
import json


BASE_API = 'https://gw-online.okko.ua:9443/api/erp'

def loader(url, LOGIN, PASSWORD):
    url0 = f'{BASE_API}/login'
    payload = json.dumps({'login': LOGIN, 'password': PASSWORD}, sort_keys=True, indent=4)
    response = requests.post(url0, headers={'Content-Type': 'application/json'}, data=payload)
    
    #print(response.content)
    print(response.status_code)

    data = response.json()
    print(data)

    TOKEN = data['access_token']

    # url = f'{BASE_API}/transactions?date_from=2023-01-01&date_to=2023-01-31&processed_in_bo=true'
    response = requests.get(url, headers={'Accept': 'application/json', 'Authorization': TOKEN})

    data = response.json()
    #with open('data_test.txt', 'w', encoding='utf-8') as f:
    #    f.write(str(data))

    print(len(data))
    return data

    
def test2():
    with open('data_test.txt', 'r', encoding='utf-8') as f:
        data = eval(f.read())
    print(data)

def main():

    db = My_base()
    db.open()
    
    today = datetime.today()
    date0 = datetime(2023, 11, 23)
    
    sql = f'DELETE FROM Okko_data WHERE trans_date LIKE "{today:%d.%m.%Y}%"'
    db.execute(sql)
    
    sql = 'SELECT trans_date FROM Okko_data ORDER BY trans_id DESC LIMIT 1'
    result = db.get_one_table(sql)
    print(result) 
    if result[0]:
        dt = datetime.strptime(result[0], '%d.%m.%Y %H:%M:%S') 
        print(dt)
    else:
        dt = date0
    date1 = dt + timedelta(days=1)
    date2 = today
    print(date1)
    print(date2)
    
    
    if date1 <= date2:
        url = f'{BASE_API}/transactions?date_from={date1:%Y-%m-%d}&date_to={date2:%Y-%m-%d}&processed_in_bo=true'
        print(url)
        result = loader(url, db.cfg['LOGIN'], db.cfg['PASSWORD'])

        if result:
            db.export_pd('Okko_data', result, 'append')
        
    db.close()
    
    
if __name__ == '__main__':
    main()