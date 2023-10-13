# import requests
# from config import (
#     DTC_8B_API_URL,SIGN_KEY_DTC_8B,DTC_8D_API_URL,SIGN_KEY_DTC_8D
# )
import hashlib
import json
import requests

from ..utils.log import LOGGER

def create_sign_str(user_name,start,end,type,key):
    # start = ''.join(start.split('-'))
    # end = ''.join(end.split('-'))
    input_str = f"{user_name}|{start}|{end}|{type}|{key}"
    print(input_str)
    res = hashlib.md5(input_str.encode())

    return res.hexdigest()

def call_api(url,data):
    payload = json.dumps(data)
    headers = {
    'Content-Type': 'application/json'
    }

    response = requests.post(url, headers=headers, data=payload)

    data_res = response.json()
    if data_res['code'] == 0:
        return data_res['data']['totalRechargeAmount'] , data_res['data']['totalValidBetAmount']
    else:
        # data_res["aa"] = data
        LOGGER.error(data_res)
        return None,None
    
def get_valid_bet_amount(user_name,start_date,end_date,url,sign_key):
    # user_name="testdudoan1"
    valid_bet_amount = 0
    recharge_amount = 0
    start = ''.join(start_date.split('-'))
    end = ''.join(end_date.split('-'))
    # data={
    # "userName":user_name,
    # "startTime":start, 
    # "endTime":end
    # }
    sign = create_sign_str(user_name="cavang151212",start=start,end=end,type=5,key=sign_key)
    
    data={
    "userName":"cavang151212",
    "startTime":start, 
    "endTime":end,
    "gameType":5,
    "sign":sign
    }
    print(data)
    test(url=url,data=data)
    # for type in range(1,6):
    #     sign = create_sign_str(user_name=user_name,start=start,end=end,type=type,key=sign_key)
    #     data["gameType"] = type
    #     data["sign"] = sign

    #     recharge, valid_bet  = call_api(url=url,data=data)

    #     valid_bet = 0 if valid_bet == None else valid_bet
    #     recharge = 0 if recharge == None else recharge

    #     valid_bet_amount += valid_bet
    #     recharge_amount = recharge
    return recharge_amount, valid_bet_amount

def test(url,data):
    import requests
    import json

    # url = "https://api.888b01.com/member/r/userCe"

    payload = json.dumps(data)
    headers = {
    'Content-Type': 'application/json'
    }

    response = requests.request("POST", url, headers=headers, data=payload)

    print(response.text)



    

