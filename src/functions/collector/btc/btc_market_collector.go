package btc

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"reflect"
	"strconv"
	"time"

	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/compute/metadata"
	"cloud.google.com/go/pubsub"
	"cloud.google.com/go/storage"
	"github.com/jszwec/csvutil"
	"google.golang.org/api/iterator"
)

type BTC_MARKET_API_DATA struct {
	Allowance struct {
		Cost      float64 `json:"cost"`
		Remaining float64 `json:"remaining"`
	} `json:"allowance"`
	Result struct {
		OneMinute      [][]float64 `json:"60"`
		ThreeMinutes   [][]float64 `json:"180"`
		FiveMinutes    [][]float64 `json:"300"`
		FifteenMinutes [][]float64 `json:"900"`
		ThirtyMinutes  [][]float64 `json:"1800"`
		OneHour        [][]float64 `json:"3600"`
		TwoHours       [][]float64 `json:"7200"`
		FourHours      [][]float64 `json:"14400"`
		SixHours       [][]float64 `json:"21600"`
		TwelveHours    [][]float64 `json:"43200"`
		OneDay         [][]float64 `json:"86400"`
		ThreeDays      [][]float64 `json:"259200"`
		OneWeek        [][]float64 `json:"604800"`
	} `json:"result"`
	Error string `json:"error"`
}

type BTC_MARKET_PRICE struct {
	UNIX_TIME    string
	CLOSE_TIME   time.Time
	OPEN_PRICE   string
	HIGH_PRICE   string
	LOW_PRICE    string
	CLOSE_PRICE  string
	VOLUME       string
	QUOTE_VOLUME string
}

type RECENTLY_UNIXTIME struct {
	TABLE_NAME string
	UNIX_TIME  string
}

type Exchange struct {
	ExchangeName string
	Ticker       string
	TableName    string
}

type Period struct {
	ApiField   string
	PeriodName string
	Time       string
}

const (
	cryptoWatchUrl            = "https://api.cryptowat.ch/markets"
	ohlc                      = "ohlc"
	bucket                    = "market_data_accumlation"
	dataset                   = "BTC_MARKET_PRICE"
	recentlyUnixtimeTableName = "RECENTLY_UNIXTIME"
	topicId                   = "error-report-topic"
)

var exchanges = []Exchange{
	{ExchangeName: "bitflyer", Ticker: "btcfxjpy", TableName: "BITFLYER_BTCFXJPY"},
	{ExchangeName: "bitmex", Ticker: "btcusd-perpetual-future-inverse", TableName: "BITMEX_BTCUSD-PERPETUAL-FUTURE-INVERSE"},
	{ExchangeName: "bitfinex", Ticker: "btcusd", TableName: "BITFINEX_BTCUSD"},
	{ExchangeName: "binance", Ticker: "btcusdt", TableName: "BINANCE_BTCUSDT"},
}

var periods = []Period{
	{ApiField: "OneMinute", PeriodName: "1M", Time: "60"},
	{ApiField: "ThreeMinutes", PeriodName: "3M", Time: "180"},
	{ApiField: "FiveMinutes", PeriodName: "5M", Time: "300"},
	{ApiField: "FifteenMinutes", PeriodName: "15M", Time: "900"},
	{ApiField: "ThirtyMinutes", PeriodName: "30M", Time: "1800"},
	{ApiField: "OneHour", PeriodName: "1H", Time: "3600"},
	{ApiField: "TwoHours", PeriodName: "2H", Time: "7200"},
	{ApiField: "FourHours", PeriodName: "4H", Time: "14400"},
	{ApiField: "SixHours", PeriodName: "6H", Time: "21600"},
	{ApiField: "TwelveHours", PeriodName: "12H", Time: "43200"},
	{ApiField: "OneDay", PeriodName: "1D", Time: "86400"},
	{ApiField: "ThreeDays", PeriodName: "3D", Time: "259200"},
	{ApiField: "OneWeek", PeriodName: "1W", Time: "604800"},
}

type PubSubMessage struct {
	Data []byte `json:"data"`
}

func BtcMarketCollector(ctx context.Context, _ PubSubMessage) error {

	projectId, err := loadProjectId()
	if err != nil {
		// pubsubにエラーメッセージ送信(プロジェクトIDが取得出来ない場合、環境変数より取得)
		return reportErrorToSlack(ctx, os.Getenv("projectId"), err.Error())
	}

	err = btcMarketCollector(ctx, projectId)
	if err != nil {
		// pubsubにエラーメッセージ送信
		return reportErrorToSlack(ctx, projectId, err.Error())
	}

	return nil
}

func btcMarketCollector(ctx context.Context, projectId string) error {

	bigqueryClient, err := bigquery.NewClient(ctx, projectId)
	if err != nil {
		return err
	}
	defer bigqueryClient.Close()

	storageClient, err := storage.NewClient(ctx)
	if err != nil {
		return err
	}
	defer storageClient.Close()

	// BigQueryの各テーブルの最新UnixTimeを取得する。
	// 最新UnixTimeは、API実行時に取得するデータを絞り込むために使用する。(CryptoWatchAPIのparameterであるafterに設定する)
	recentlyUnixTimeData, err := loadRecentlyUnixTime(ctx, bigqueryClient)
	if err != nil {
		return err
	}

	// csvのファイル名
	execTime := time.Now().In(time.FixedZone("Asia/Tokyo", 9*60*60)).Format("20060102_15h")

	// 取得対象の取引所ごとにループ
	for _, exchange := range exchanges {

		// 時間足ごとにループ
		for _, period := range periods {

			tableName := exchange.TableName + "_" + period.PeriodName

			var unixTime string
			for _, target := range recentlyUnixTimeData {
				if target.TABLE_NAME == tableName {
					unixTime = target.UNIX_TIME
				}
			}

			apiData, err := loadBtcMarketPrice(exchange, period.Time, unixTime)
			if err != nil {
				return err
			}

			v := reflect.ValueOf(apiData.Result).FieldByName(period.ApiField)

			targetPeriodData, ok := v.Interface().([][]float64)
			if !ok {
				return fmt.Errorf("型変換エラー: Expected [][]float64, Actual %v", v.Type())
			}

			// スライスの長さが2未満の場合、データが無いかもしくは、未来のデータしかないため、処理を中断する
			if len(targetPeriodData) < 2 {
				continue
			}

			btcData, err := convertBtcData(targetPeriodData)
			if err != nil {
				return err
			}

			// GCSへBTCデータをアップロードする
			err = uploadCsvToGcs(ctx, storageClient, btcData, exchange.TableName, period.PeriodName, execTime)
			if err != nil {
				return err
			}

			// BigQueryへBTCデータをインサート
			err = insertToBigQuery(ctx, bigqueryClient, btcData, tableName)
			if err != nil {
				return err
			}

			// 最新UnixTime更新
			err = updateRecentlyUnixTime(ctx, bigqueryClient, btcData, tableName)
			if err != nil {
				return err
			}
		}
	}

	// 最新UnixTimeの重複削除
	err = deleteDeplicateUnixTime(ctx, bigqueryClient)
	if err != nil {
		return err
	}

	return nil
}

func loadProjectId() (string, error) {
	if !metadata.OnGCE() {
		return "", fmt.Errorf("this process is not running")
	}

	projectId, err := metadata.Get("project/project-id")

	if err != nil {
		return "", fmt.Errorf("metadata.Get Error: %v", err)
	}

	return projectId, nil
}

func loadRecentlyUnixTime(ctx context.Context, client *bigquery.Client) ([]*RECENTLY_UNIXTIME, error) {

	query := fmt.Sprintf("select TABLE_NAME, UNIX_TIME from `%s.%s`", dataset, recentlyUnixtimeTableName)

	it, err := client.Query(query).Read(ctx)
	if err != nil {
		return nil, err
	}

	var rows []*RECENTLY_UNIXTIME
	for {
		var row RECENTLY_UNIXTIME
		err := it.Next(&row)
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, err
		}
		rows = append(rows, &row)
	}

	return rows, nil
}

func loadBtcMarketPrice(exchange Exchange, time string, unixTime string) (*BTC_MARKET_API_DATA, error) {
	endpoint := fmt.Sprintf("%s/%s/%s/%s", cryptoWatchUrl, exchange.ExchangeName, exchange.Ticker, ohlc)
	request, err := http.NewRequest("GET", endpoint, nil)
	if err != nil {
		return nil, fmt.Errorf("API実行エラー %v", err)
	}

	params := request.URL.Query()
	params.Add("periods", time)

	// 初回実行時に限り、unixTimeは空文字になる
	if unixTime != "" {
		unixTimeInt, err := strconv.Atoi(unixTime)
		if err != nil {
			return nil, fmt.Errorf("UnixTimeの値が不正 %v", err)
		}

		// UnixTimeに1を加えた値を設定する
		params.Add("after", strconv.Itoa(unixTimeInt+1))
	}
	request.URL.RawQuery = params.Encode()

	response, err := http.DefaultClient.Do(request)
	if err != nil {
		return nil, fmt.Errorf("API実行エラー %v", err)
	}
	defer response.Body.Close()

	if response.StatusCode != 200 {
		return nil, fmt.Errorf("API実行エラー %v", response)
	}

	body, err := ioutil.ReadAll(response.Body)
	if err != nil {
		return nil, err
	}

	var data BTC_MARKET_API_DATA
	if err := json.Unmarshal(body, &data); err != nil {
		return nil, fmt.Errorf("json変換エラー %v", err)
	}

	if data.Error != "" {
		return nil, fmt.Errorf("cryptoWatchエラー %s", data.Error)
	}

	return &data, nil
}

func convertBtcData(periodData [][]float64) ([]BTC_MARKET_PRICE, error) {

	var dataList []BTC_MARKET_PRICE

	// 配列の末尾には未来日の価格に実行日時点の最新値が入るが、この値は実行日時によって変化してしまう。
	// このため、未来日の項目となる配列の末尾を削除する。
	periodData = periodData[:len(periodData)-1]

	for _, d := range periodData {
		if len(d) != 7 {
			return nil, fmt.Errorf("リストの要素数が不正 length=%v, data=%v", len(d), d)
		}

		// priceは小数点2桁、volumeは、小数点8桁まで取得される
		data := BTC_MARKET_PRICE{
			UNIX_TIME:    strconv.FormatFloat(d[0], 'f', 0, 64),
			CLOSE_TIME:   time.Unix(int64(d[0]), 0),
			OPEN_PRICE:   strconv.FormatFloat(d[1], 'f', 2, 64),
			HIGH_PRICE:   strconv.FormatFloat(d[2], 'f', 2, 64),
			LOW_PRICE:    strconv.FormatFloat(d[3], 'f', 2, 64),
			CLOSE_PRICE:  strconv.FormatFloat(d[4], 'f', 2, 64),
			VOLUME:       strconv.FormatFloat(d[5], 'f', 8, 64),
			QUOTE_VOLUME: strconv.FormatFloat(d[6], 'f', 8, 64),
		}

		dataList = append(dataList, data)
	}

	return dataList, nil
}

// GCSへCsvデータをアップロードする
func uploadCsvToGcs(ctx context.Context, client *storage.Client, btcData []BTC_MARKET_PRICE, tableName string, periodName string, execTime string) error {

	// 構造体からcsv形式へ変換
	byteData, err := csvutil.Marshal(btcData)
	if err != nil {
		return err
	}

	path := fmt.Sprintf("btc/%s/%s/%s.csv", tableName, periodName, execTime)

	writer := client.Bucket(bucket).Object(path).NewWriter(ctx)
	defer writer.Close()

	writer.ContentType = "text/csv"
	_, err = writer.Write(byteData)
	if err != nil {
		return err
	}

	return nil
}

func insertToBigQuery(ctx context.Context, client *bigquery.Client, btcData []BTC_MARKET_PRICE, tableName string) error {

	inserter := client.Dataset(dataset).Table(tableName).Inserter()

	if err := inserter.Put(ctx, btcData); err != nil {
		return err
	}

	return nil
}

func (i *RECENTLY_UNIXTIME) Save() (map[string]bigquery.Value, string, error) {
	return map[string]bigquery.Value{
		"TABLE_NAME": i.TABLE_NAME,
		"UNIX_TIME":  i.UNIX_TIME,
	}, i.TABLE_NAME, nil
}

func updateRecentlyUnixTime(ctx context.Context, client *bigquery.Client, btcData []BTC_MARKET_PRICE, tableName string) error {

	// スライス末尾のUnixTimeを取得
	latestUnixTime := btcData[len(btcData)-1].UNIX_TIME

	unixTimeData := RECENTLY_UNIXTIME{
		TABLE_NAME: tableName,
		UNIX_TIME:  latestUnixTime,
	}

	inserter := client.Dataset(dataset).Table(recentlyUnixtimeTableName).Inserter()

	if err := inserter.Put(ctx, unixTimeData); err != nil {
		return err
	}

	return nil
}

func deleteDeplicateUnixTime(ctx context.Context, client *bigquery.Client) error {
	query := `
		SELECT 
			* EXCEPT(rowNumber)
		FROM (
			SELECT
				*,
				ROW_NUMBER() OVER (PARTITION BY TABLE_NAME ORDER BY UNIX_TIME DESC) as rowNumber
			FROM
			BTC_MARKET_PRICE.RECENTLY_UNIXTIME
		)
		WHERE
			rowNumber = 1;
	`
	q := client.Query(query)
	q.QueryConfig.Dst = client.Dataset(dataset).Table(recentlyUnixtimeTableName)
	q.WriteDisposition = bigquery.WriteTruncate

	job, err := q.Run(ctx)
	if err != nil {
		return err
	}

	status, err := job.Wait(ctx)
	if err != nil {
		return err
	}

	if err := status.Err(); err != nil {
		return err
	}
	return nil
}

func reportErrorToSlack(ctx context.Context, projectId string, errorMsg string) error {

	client, err := pubsub.NewClient(ctx, projectId)
	if err != nil {
		return err
	}

	topic := client.Topic(topicId)
	result := topic.Publish(ctx, &pubsub.Message{
		Data: []byte(errorMsg),
		Attributes: map[string]string{
			"projectId":    projectId,
			"functionName": "CollectBtcMarketPrice",
			"eventTime":    strconv.FormatInt(time.Now().Unix(), 10),
		},
	})

	_, err = result.Get(ctx)
	if err != nil {
		return err
	}

	return nil
}
