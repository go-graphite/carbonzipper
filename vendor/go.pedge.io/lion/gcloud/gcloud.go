// Package gcloudlion integrates lion with the Google Cloud Logging service.
//
// See https://cloud.google.com/logging/docs/ for more details.
package gcloudlion

import (
	"go.pedge.io/lion"
	"google.golang.org/api/logging/v1beta3"
)

// NewPusher creates a new lion.Pusher that logs using the Google Cloud Logging API.
func NewPusher(
	service *logging.ProjectsLogsEntriesService,
	projectID string,
	logName string,
) lion.Pusher {
	return newPusher(
		service,
		projectID,
		logName,
	)
}
