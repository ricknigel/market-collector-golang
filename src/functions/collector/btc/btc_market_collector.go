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

	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/compute/metadata"
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
	UNIX_TIME    string    `bigquery:"UNIX_TIME"`
	CLOSE_TIME   time.Time `bigquery:"CLOSE_TIME"`
	OPEN_PRICE   float64   `bigquery:"OPEN_PRICE"`
	HIGH_PRICE   float64   `bigquery:"HIGH_PRICE"`
	LOW_PRICE    float64   `bigquery:"LOW_PRICE"`
	CLOSE_PRICE  float64   `bigquery:"CLOSE_PRICE"`
	VOLUME       float64   `bigquery:"VOLUME"`
	QUOTE_VOLUME float64   `bigquery:"QUOTE_VOLUME"`
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
	dataset        = "BTC_MARKET_PRICE"
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

func (i *BTC_MARKET_PRICE) Save() (map[string]bigquery.Value, string, error) {
	return map[string]bigquery.Value{
		"UNIX_TIME":    i.UNIX_TIME,
		"CLOSE_TIME":   i.CLOSE_PRICE,
		"OPEN_PRICE":   i.OPEN_PRICE,
		"HIGH_PRICE":   i.HIGH_PRICE,
		"LOW_PRICE":    i.LOW_PRICE,
		"CLOSE_PRICE":  i.CLOSE_PRICE,
		"VOLUME":       i.VOLUME,
		"QUOTE_VOLUME": i.QUOTE_VOLUME,
	}, i.UNIX_TIME, nil
}

func BtcMarketCollector(ctx context.Context, _ PubSubMessage) error {

	projectId, err := loadProjectId()
	if err != nil {
		return err
	}

	client, err := bigquery.NewClient(ctx, projectId)
	if err != nil {
		return err
	}
	defer client.Close()

	for _, exchange := range exchanges {
		apiData, err := loadBtcMarketPrice(exchange)
		if err != nil {
			return err
		}

		for _, period := range periods {

			v := reflect.ValueOf(apiData.Result).FieldByName(period.ApiField)

			targetPeriodData, ok := v.Interface().([][]float64)
			if !ok {
				return fmt.Errorf("型変換エラー: Expected [][]float64, Actual %v", v.Type())
			}

			tableData, err := convertApiDataToBigQuery(targetPeriodData, period.ApiField)
			if err != nil {
				return err
			}

			inserter := client.Dataset(dataset).Table(exchange.TableName + "_" + period.PeriodName).Inserter()

			if err := inserter.Put(ctx, tableData); err != nil {
				return err
			}
		}
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

func convertApiDataToBigQuery(periodData [][]float64, perios string) ([]BTC_MARKET_PRICE, error) {

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

		data := BTC_MARKET_PRICE{
			UNIX_TIME:    strconv.FormatFloat(d[0], 'f', 0, 64),
			CLOSE_TIME:   time.Unix(int64(d[0]), 0),
			OPEN_PRICE:   d[1],
			HIGH_PRICE:   d[2],
			LOW_PRICE:    d[3],
			CLOSE_PRICE:  d[4],
			VOLUME:       d[5],
			QUOTE_VOLUME: d[6],
		}

		dataList = append(dataList, data)
	}
	return dataList, nil
}
