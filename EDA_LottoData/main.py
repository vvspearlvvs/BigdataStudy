import pandas as pd
from urllib.request import urlopen
import json
import matplotlib.pyplot as plt
from tqdm import tqdm
from matplotlib import pyplot as plt
import numpy as np


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

    if raw_data['firstPrzwnerCo'] != 0: #당첨자가 한명도 안나온 회자는 제외하도록 수정
        dict_data['date'] = raw_data['drwNoDate'] #추첨날짜
        dict_data['winner'] = raw_data['firstPrzwnerCo'] #당첨자 수
        dict_data['winmnt'] = raw_data['firstWinamnt'] #1등당첨금액
        winmnt = dict_data['winmnt']
        dict_data['permnt'] = round(dict_data['winmnt'] / raw_data['firstPrzwnerCo'],0) #인당 수령액
        permnt = dict_data['permnt']
        dict_data['relmnt'] = cal_tax(permnt) #인당 실수령액(세금제외)
    else:
        dict_data ={} #당첨자가 한명도 안나온 당첨금은 제외

    pd_data=pd.DataFrame.from_dict(dict_data,orient='index')
    pd_data=pd_data.transpose()
    #csv로 저장해서 파일로 만들고, 거기에서 읽어오기
    return pd_data

def draw_graph(data):
    data['relmnt'].plot(figsize=(20,6))
    plt.legend()
    plt.show()
    #x=data['date']
    #y=data['relmnt']
    #plt.plot(x,y)
    #plt.show()

    print("##draw graph")

def main():
    data_list = [] #result_df
    for r in range(1,10):
        raw_data = load_lotto(r)
        if raw_data['returnValue'] =='success':
            data = make_data(raw_data)
            data_list.append(data)
        elif raw_data['returnValue'] =='fail':
            pass
    print(data_list)
    result = pd.concat(data_list,ignore_index=True)
    draw_graph(result)


def graph_test():
    x = np.arange(1,10)
    y = x*5

    plt.plot(x,y,'r')
    plt.show()

if __name__ == '__main__':
    #graph_test()
    main()