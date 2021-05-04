package btc

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"reflect"
	"strconv"
	"time"

	"cloud.google.com/go/storage"
	"github.com/jszwec/csvutil"
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

type Exchange struct {
	ExchangeName string
	Ticker       string
	TableName    string
}

type Period struct {
	ApiField   string
	PeriodName string
}

const (
	cryptoWatchUrl = "https://api.cryptowat.ch/markets"
	ohlc           = "ohlc"
	bucket         = "upload_btc_test"
)

var exchanges = []Exchange{
	{ExchangeName: "bitflyer", Ticker: "btcfxjpy", TableName: "BITFLYER_BTCFXJPY"},
	{ExchangeName: "bitmex", Ticker: "btcusd-perpetual-future-inverse", TableName: "BITMEX_BTCUSD-PERPETUAL-FUTURE-INVERSE"},
	{ExchangeName: "bitfinex", Ticker: "btcusd", TableName: "BITFINEX_BTCUSD"},
	{ExchangeName: "binance", Ticker: "btcusdt", TableName: "BINANCE_BTCUSDT"},
}

var periods = []Period{
	{ApiField: "OneMinute", PeriodName: "1M"},
	{ApiField: "ThreeMinutes", PeriodName: "3M"},
	{ApiField: "FiveMinutes", PeriodName: "5M"},
	{ApiField: "FifteenMinutes", PeriodName: "15M"},
	{ApiField: "ThirtyMinutes", PeriodName: "30M"},
	{ApiField: "OneHour", PeriodName: "1H"},
	{ApiField: "TwoHours", PeriodName: "2H"},
	{ApiField: "FourHours", PeriodName: "4H"},
	{ApiField: "SixHours", PeriodName: "6H"},
	{ApiField: "TwelveHours", PeriodName: "12H"},
	{ApiField: "OneDay", PeriodName: "1D"},
	{ApiField: "ThreeDays", PeriodName: "3D"},
	{ApiField: "OneWeek", PeriodName: "1W"},
}

type PubSubMessage struct {
	Data []byte `json:"data"`
}

// TODO: bigqueryから最新日時を取得し、抽出する

func UploadBtcPriceToGcs(ctx context.Context, _ PubSubMessage) error {

	client, err := storage.NewClient(ctx)
	if err != nil {
		return err
	}
	defer client.Close()

	execTime := time.Now().Format("20060102_15h")

	// 取得対象の取引所ごとにループ
	for _, exchange := range exchanges {
		apiData, err := loadBtcMarketPrice(exchange)
		if err != nil {
			return err
		}

		// 時間足ごとにループ
		for _, period := range periods {

			v := reflect.ValueOf(apiData.Result).FieldByName(period.ApiField)

			targetPeriodData, ok := v.Interface().([][]float64)
			if !ok {
				return fmt.Errorf("型変換エラー: Expected [][]float64, Actual %v", v.Type())
			}

			csvData, err := convertCsvData(targetPeriodData)
			if err != nil {
				return err
			}

			path := fmt.Sprintf("%s/%s/%s.csv", exchange.TableName, period.PeriodName, execTime)

			writer := client.Bucket(bucket).Object(path).NewWriter(ctx)
			defer writer.Close()

			writer.ContentType = "text/csv"
			writer.Write(csvData)
		}
	}
	return nil
}

func loadBtcMarketPrice(exchange Exchange) (*BTC_MARKET_API_DATA, error) {
	endpoint := fmt.Sprintf("%s/%s/%s/%s", cryptoWatchUrl, exchange.ExchangeName, exchange.Ticker, ohlc)
	response, err := http.Get(endpoint)
	if err != nil {
		return nil, fmt.Errorf("API実行時エラー %v", err)
	}
	defer response.Body.Close()

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

func convertCsvData(periodData [][]float64) ([]byte, error) {

	var dataList []BTC_MARKET_PRICE

	// 配列の末尾を削除する理由
	// 配列の末尾には未来日の価格に実行日時点の最新値が入るが、この値は実行日時によって変化してしまう。
	// このため、BigQueryにストリーミングインサートしても、同じ日時に複数の価格が存在するデータとなってしまう。
	// これを防ぐために未来日の項目となる配列の末尾を削除する。
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

	// 構造体からcsv形式へ変換
	b, err := csvutil.Marshal(dataList)
	if err != nil {
		return nil, err
	}

	return b, nil
}
