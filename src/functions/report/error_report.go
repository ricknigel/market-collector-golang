package report

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"strconv"
	"time"
)

type PubSubMessage struct {
	Data       []byte `json:"data"`
	Attributes struct {
		ProjectId    string `json:"projectId"`
		FunctionName string `json:"functionName"`
		EventTime    string `json:"eventTime"`
	} `json:"attributes"`
}

type SlackMessage struct {
	Blocks []Blocks `json:"blocks"`
	Text   string   `json:"text,omitempty"`
}

type Blocks struct {
	Type   string `json:"type"`
	Text   *Text  `json:"text,omitempty"`
	Fields []Text `json:"fields,omitempty"`
}

type Text struct {
	Type string `json:"type"`
	Text string `json:"text"`
}

var (
	webhookUrl = os.Getenv("SlackWebhookUrl")
)

func ErrorReport(ctx context.Context, m PubSubMessage) error {

	fmt.Printf("functionName: %s, Time: %s, error: %s", m.Attributes.FunctionName, m.Attributes.EventTime, m.Data)

	time, err := strconv.ParseInt(m.Attributes.EventTime, 10, 64)
	if err != nil {
		return lastErrorReport(err)
	}

	err = postSlackMessage(m.Attributes.ProjectId, m.Attributes.FunctionName, time, string(m.Data))
	if err != nil {
		return lastErrorReport(err)
	}
	return nil
}

func lastErrorReport(err error) error {

	postError := postSlackMessage("-", "ErrorReport", time.Now().Unix(), err.Error())
	// ここで何かしらエラーが起こった場合は、ループが起きないよう通知せず、ログに出力するのみとする。
	if postError != nil {
		return postError
	}
	return err
}

func postSlackMessage(projectId string, functionName string, eventTime int64, errorLog string) error {

	// メッセージ
	blocks := []Blocks{
		{Type: "header", Text: &Text{
			Type: "plain_text", Text: ":warning:  Cause Error  :warning:",
		}},
		{Type: "section", Fields: []Text{
			{Type: "mrkdwn", Text: fmt.Sprintf("*Project:*\n%s", projectId)},
			{Type: "mrkdwn", Text: fmt.Sprintf("*Function:*\n%s", functionName)},
			{Type: "mrkdwn", Text: fmt.Sprintf("*EventTime:*\n%s", time.Unix(eventTime, 0).In(time.FixedZone("Asia/Tokyo", 9*60*60)).Format("2006/01/02 15:04"))},
		}},
		{Type: "section", Text: &Text{
			Type: "mrkdwn", Text: fmt.Sprintf("*Error Log:*\n```%s```", errorLog),
		}},
	}

	requestBody := SlackMessage{
		Blocks: blocks,
	}

	bodyJson, err := json.Marshal(requestBody)
	if err != nil {
		return err
	}

	resp, err := http.Post(webhookUrl, "application/json", bytes.NewBuffer(bodyJson))
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	result, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return err
	}

	if resp.StatusCode != 200 {
		return fmt.Errorf("Slack Webhook Error [Http Status: %s], [Result %s]", resp.Status, result)
	}
	return nil
}
