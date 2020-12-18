from enum import Enum
import pandas as pd


class Universe(Enum):
    ALL = "ALL"
    SP100 = "S&P 100"
    SP500 = "S&P 500"
    NASDAQ100 = "NASDAQ 100"


def all_alpaca_assets(client):
    return [_.symbol for _ in client.list_assets()]


def get_sp500():
    table = pd.read_html('https://en.wikipedia.org/wiki/List_of_S%26P_500_companies')
    df = table[0]
    df.columns = df.iloc[0]
    df = df.iloc[1:]
    return df.Symbol.tolist()


def get_sp100():
    table = pd.read_html('https://en.wikipedia.org/wiki/S%26P_100')
    df = table[2]
    df.columns = df.iloc[0]
    df = df.iloc[1:]
    return df.Symbol.tolist()


def get_nasdaq100():
    table = pd.read_html('https://en.wikipedia.org/wiki/NASDAQ-100')
    df = table[3]
    df.columns = df.iloc[0]
    df = df.iloc[1:]
    return df.Ticker.tolist()


if __name__ == '__main__':
    print(get_sp100()[:10])
    # print(get_sp500()[:10])