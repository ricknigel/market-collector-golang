package btc

import (
	"context"
	"fmt"

	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/compute/metadata"
)

type GCSEvent struct {
	Bucket string `json:"bucket"`
	Name   string `json:"name"`
}

func InsertBtcPriceToBigQuery(ctx context.Context, e GCSEvent) error {

	projectId, err := loadProjectId()
	if err != nil {
		return err
	}

	client, err := bigquery.NewClient(ctx, projectId)
	if err != nil {
		return err
	}
	defer client.Close()

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
