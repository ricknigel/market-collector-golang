## テーブル設計資料

BigQueryを使う上で、クエリ読み取りやストリーミング挿入といった操作をした際に掛かる料金を明確にするため、1年間に増加するデータサイズを調査した。

### テーブルレイアウト

BTC_MARKET_PRICEのテーブルレイアウトは、下記の通りとなる。

|フィールド名|タイプ|モード|説明|
|:-:|:-:|:-:|:-:|
|CLOSE_TIME|TIMESTAMP|REQUIRED|日時|
|OPEN_PRICE|NUMERIC|REQUIRED|始値|
|HIGH_PRICE|NUMERIC|REQUIRED|高値|
|LOW_PRICE|NUMERIC|REQUIRED|低値|
|CLOSE_PRICE|NUMERIC|REQUIRED|終値|
|VOLUME|NUMERIC|REQUIRED|取引量(BTC) |
|QUOTE_VOLUME|NUMERIC|REQUIRED|取引量(USD)|


なぜタイプをNUMERICにしたかは、[こちらを参考にした](https://stackoverflow.com/questions/54789526/what-is-the-difference-between-numeric-and-float-in-bigquery/54791742)

### テーブル数

テーブル数は下記のようになっている。（テーブルのレイアウトは全て同じ）

4（取得対象の取引所） x 13（時間足） = **52テーブル**

#### データ取得対象の取引所とその通貨ペア

|取引所|通貨ペア|
|:-:|:-:|
|BitFlyer|BTCFX/JPY|
|BitFinex|BTC/USD|
|Bitmex|BTC/USD|
|Binance|BTC/USDT|

#### 取得対象の時間足

|No|時間足|秒数|
|:-:|:-:|:-:|
|1|1m|60|
|2|3m|180|
|3|5m|300|
|4|15m|900|
|5|30m|1800|
|6|1h|3600|
|7|2h|7200|
|8|4h|14400|
|9|6h|21600|
|10|12h|43200|
|11|1d|86400|
|12|3d|259200|
|13|1w|604800|

### データサイズ算出

#### 1レコード毎のデータサイズ

BigQueryでは、データのタイプごとにデータサイズが決められている。[参考](https://cloud.google.com/bigquery/pricing?hl=ja#data)

ここからまずテーブルの1レコード当たりのデータサイズを算出する。

|タイプ|Byte|カラム数|データサイズ(Byte)|
|:-:|:-:|:-:|:-:|
|TIMESTAMP|8|1カラム|8|
|NUMERIC|16|6カラム|96|

よって1レコード毎のデータサイズは、**104バイト**となる

#### 期間当たりのレコード量

登録されるレコードの量は、テーブル毎に異なる。

例えば1分足のテーブルは、1分足 * 60分 * 24h = 1,440レコードが1日に登録される計算となる。

このように各時間足のテーブル毎に増加するレコード量を算出する。

|時間足|1日|1週間|1ヵ月(30日)|1年(365日)|
|:-:|:-:|:-:|:-:|:-:|
|1m|1,440|10,080|43,200|525,600|
|3m|480|3,360|14,400|175,200|
|5m|288|2,016|8,640|105,120|
|15m|96|672|2,880|35,040|
|30m|48|336|1,440|17,520|
|1h|24|168|720|8,760|
|2h|12|84|360|4,380|
|4h|8|42|180|2,190|
|6h|4|28|120|1,460|
|12h|3|14|60|730|
|1d|1|7|30|365|
|3d|0|2|10|121|
|1w|0|1|4|52|
|**合計**|2,401|16,810|72,044|876,539|
|**4取引所分の合計**|9,604|67,240|288,176|3,560,156|

#### 期間毎のデータサイズ

増加するレコード量 * 104Byte(1レコード当たりのデータサイズ) = 期間毎のデータサイズ

||1日|1週間|1ヵ月(30日)|1年(365日)|
|:-:|:-:|:-:|:-:|:-:|
|**データサイズ(Byte)**|998,816|6,992,960|29,970,304|364,640,224|
|**データサイズ(MB)**|1.00|6.99|29.97|364.64|

### 結論

1年間に増加するデータサイズは、約364.64MBとなる。

増加するサイズは想定より少なかったため、データ登録にはストリーミング挿入を使用する。

※ストリーミング挿入は、200MB当たり$0.02
