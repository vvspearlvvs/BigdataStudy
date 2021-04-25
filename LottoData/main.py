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

def make_data(raw_data):

    data=pd.DataFrame.from_dict(raw_data,orient='index')
    data=data.transpose()

    return data

def main():
    for r in range(1,10):
        raw_data = load_lotto(r)
        data = make_data(raw_data)
        if data.loc[0,"returnValue"] == "fail":
            break
        else:
            print(data)

if __name__ == '__main__':
    main()