import datetime
import json
import os
import pandas as pd
import requests
from collections import OrderedDict
from bs4 import BeautifulSoup


class SeafoodWholesaleScrapper:
    start_date = None

    # 全大寫為 Python 常數規範
    URL = "https://efish.fa.gov.tw/efish/statistics/daysinglemarketmultifish.htm"

    SOURCES = OrderedDict(
        {
            # 消費地市場
            "F109": "台北",
            "F241": "三重",
            "F300": "新竹",
            "F330": "桃園",
            "F360": "苗栗",
            "F400": "台中",
            "F500": "彰化",
            "F513": "埔心",
            "F600": "嘉義",
            "F630": "斗南",
            "F722": "佳里",
            "F730": "新營",
            "F820": "岡山",
            # 生產地市場
            "F200": "基隆",
            "F261": "頭城",
            "F270": "蘇澳",
            "F708": "台南",
            "F709": "興達港",
            "F800": "高雄",
            "F826": "梓官",
            "F880": "澎湖",
            "F916": "東港",
            "F936": "新港",
            "F950": "花蓮",
        }
    )

    HEADERS = {
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/132.0.0.0 Safari/537.36"
    }
    # 只有 calendarType, numbers, orderby 是固定的
    payload = {
        "dateStr": "",
        "calendarType": "tw",
        "year": "",
        "month": "",
        "day": "",
        "mid": "",
        "numbers": "999",
        "orderby": "w",
    }

    def __init__(self, start_date: str, end_date: str):
        """
        start_date 與 end_date 只能是字串形式，並且一定符合
        YYYY-MM-DD 的格式
        e.g. 2024-11-12, 2024-11-13
        """

        self.start_date = start_date
        self.end_date = end_date
        self.thead_columns = None
        self.rows = None
        self.data = []
        self.result_data = []

    # 解析 HTML
    def parse_html(self, response):
        soup = BeautifulSoup(response.text, "html.parser")
        table = soup.find("table", id="ltable")
        thead = table.find("thead")

        # 欄位名稱為 thead 的部分，欄位資料為 tboby 的部分
        if table:
            # 抓取欄位名稱
            thead = table.find("thead")
            self.thead_columns = [th.text.strip() for th in thead.find_all("th")]

            # 抓取 tbody 中的資料
            tbody = table.find("tbody")
            rows = []

            for row in tbody.find_all("tr"):

                tbody_columns = row.find_all("td")

                row_data = [col.text.strip() for col in tbody_columns]

                rows.append(row_data)

            self.rows = rows

    # 轉成 DataFrame
    def convert_to_data_frame(self, data, dt, source_code):

        df = pd.DataFrame(self.rows, columns=self.thead_columns)
        df = df.drop(columns=["交易量漲跌幅+(-)%", "平均價漲跌幅+(-)%"])
        df = df.rename(
            columns={
                "上價(元/公斤)": "上價",
                "下價(元/公斤)": "下價",
                "中價(元/公斤)": "中價",
                "交易量(公斤)": "交易量",
                "平均價(元/公斤)": "平均價",
            }
        )
        df["交易日期"] = (
            f"{dt.year - 1911}{str(dt.month).zfill(2)}{str(dt.day).zfill(2)}"
        )
        df["市場名稱"] = self.SOURCES[source_code]

        if "交易量" in df.columns:
            df["交易量"] = df["交易量"].str.replace(",", "")
        data.append(df)

        return df

    def fetch(self) -> pd.DataFrame:
        """
        根據 start_date 與 end_date 去 '漁產批發交易行情站' 爬取資料後，
        使用 BeautifulSoup 解析，並回傳為 DataFrame

        需求:
        DataFrame 的格式要與 API 的一樣:
        上價	    下價	    中價	     交易日期	    交易量	品種代碼	市場名稱	平均價	魚貨名稱
        83.0	37.6	64.6	"1140212"	9872.5	1011	三重	    64.7	吳郭魚
        100.3	44.0	63.7	"1140212"	38.6	1011	新營	    67.1	吳郭魚
        ...
        """

        # 日期格式為西元 YYYY-MM-DD 例: 2024-10-20
        start_date = datetime.datetime.strptime(self.start_date, "%Y-%m-%d")
        end_date = datetime.datetime.strptime(self.end_date, "%Y-%m-%d")
        # 起始日期到結束日期的範圍
        time_delta: datetime.timedelta = end_date - start_date
        # 起始日跟結束日為同一天時，至少要為 1 否則不會進迴圈
        days: int = time_delta.days or 1
        data = []

        for i in range(days):
            for source_code in self.SOURCES:
                dt = start_date + datetime.timedelta(days=i)

                payload = self.payload.copy()
                payload["dateStr"] = f"{dt.year - 1911}.{dt.month}.{dt.day}"
                payload["year"] = str(dt.year - 1911)
                payload["mid"] = source_code
                payload["month"] = str(dt.month)
                payload["day"] = str(dt.day)

                # 需考慮三種情況
                # 1. status_code == 200 只代表連線成功
                # 2. status_code != 200 的情況
                # 3. status_code == 200 但是 HTML 有問題

                # 當有異常時，最多進行三次爬取
                max_retry = 3
                retry_times = 0

                while retry_times < max_retry:
                    try:
                        response = requests.post(
                            self.URL, headers=self.HEADERS, data=payload
                        )
                        # 狀態碼為 200 時，會直接結束
                        if response.status_code == 200:

                            self.parse_html(response)

                            self.convert_to_data_frame(data, dt, source_code)

                            break
                        else:
                            retry_times += 1
                            print(f"伺服器回應有問題: {response.status_code}")
                    except Exception as e:
                        retry_times += 1
                        print(f"連線錯誤: {e}")
        # 將 DataFrame 合併後回傳
        return pd.concat(data, ignore_index=True) if data else pd.DataFrame()


# 爬取日期為 2024-10-10 為例
if __name__ == "__main__":
    scrapper = SeafoodWholesaleScrapper("2024-10-10", "2024-10-10")
    df = scrapper.fetch()

    # 轉成 JSON
    file_path = "table_outer.json"

    with open(file_path, "w", encoding="utf-8") as f:
        json.dump(df.to_dict(orient="records"), f, ensure_ascii=False, indent=4)

    # 轉成 CSV
    df.to_csv("table_outer.csv", index=False, encoding="utf-8")

    # 轉成 Excel
    df.to_excel("table_outer.xlsx", index=False, engine="openpyxl")

    print(df)
    print(os.path.abspath("table_outer.csv"))
    print(os.path.abspath("table_outer.json"))
    print(os.path.abspath("table_outer.xlsx"))
