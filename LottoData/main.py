import pandas as pd
from urllib.request import urlopen
import json
from tqdm import tqdm

def load_lotto(round):
    url = "http://www.dhlottery.co.kr/common.do?method=getLottoNumber&drwNo="+str(round)

    result_data = urlopen(url)
    result = result_data.read()

    raw_data = json.loads(result)

    return raw_data

def cal_tax(permnt):
    if permnt >=300000000:
        tax = 300000000 * 0.22 + (permnt-300000000)*0.33
    elif permnt<300000000 and permnt>5000:
        tax= permnt*0.22
    else:
        tax = 0

    return round(permnt-tax,0)

def make_data(raw_data):
    dict_data ={}

    dict_data['date'] = raw_data['drwNoDate'] #추첨날짜
    dict_data['winner'] = raw_data['firstPrzwnerCo'] #당첨자 수
    dict_data['winmnt'] = raw_data['firstWinamnt'] #1등당첨금액
    winmnt = dict_data['winmnt']
    dict_data['permnt'] = round(dict_data['winmnt'] / dict_data['winner'],0) #인당 수령액
    permnt = dict_data['permnt']
    dict_data['relmnt'] = cal_tax(permnt) #인당 실수령액(세금제외)

    pd_data=pd.DataFrame.from_dict(dict_data,orient='index')
    pd_data=pd_data.transpose()

    return pd_data

def main():
    for r in range(950,960):
        raw_data = load_lotto(r)

        data = make_data(raw_data)
        print(data)
        '''
        if data.loc[0,"returnValue"] == "fail":
            break
        else:
            print(data)
        '''

if __name__ == '__main__':
    main()