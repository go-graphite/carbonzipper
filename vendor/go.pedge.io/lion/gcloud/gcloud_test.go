package gcloudlion_test

import (
	"os"

	"go.pedge.io/lion"
	"go.pedge.io/lion/gcloud"
	"golang.org/x/net/context"
	"golang.org/x/oauth2/google"
	"google.golang.org/api/logging/v1beta3"
)

func Example() {
	projectID, _ := os.LookupEnv("GCLOUD_PROJECT_ID")
	logName := "lion"
	client, err := google.DefaultClient(
		context.Background(),
		logging.LoggingWriteScope,
	)
	if err != nil {
		lion.Errorln(err)
		return
	}
	service, err := logging.New(client)
	if err != nil {
		lion.Errorln(err)
		return
	}
	logger := lion.NewLogger(
		gcloudlion.NewPusher(
			service.Projects.Logs.Entries,
			projectID,
			logName,
		),
	)
	logger.Infoln("Hello from lion!")
}
