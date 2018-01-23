package gcloudlion

import (
	"time"

	"go.pedge.io/lion"
	"google.golang.org/api/logging/v1beta3"
)

const customServiceName = "compute.googleapis.com"

var (
	// https://cloud.google.com/logging/docs/api/ref/rest/v1beta3/projects.logs.entries/write#LogSeverity
	severityName = map[lion.Level]string{
		lion.LevelNone:  "DEFAULT",
		lion.LevelDebug: "DEBUG",
		lion.LevelInfo:  "INFO",
		lion.LevelWarn:  "WARNING",
		lion.LevelError: "ERROR",
		lion.LevelFatal: "ERROR",
		lion.LevelPanic: "ALERT",
	}
)

type pusher struct {
	service   *logging.ProjectsLogsEntriesService
	projectID string
	logName   string
}

func newPusher(
	service *logging.ProjectsLogsEntriesService,
	projectID string,
	logName string,
) *pusher {
	return &pusher{
		service,
		projectID,
		logName,
	}
}

func (p *pusher) Push(entry *lion.Entry) error {
	id := entry.ID
	if id == "" {
		id = lion.DefaultIDAllocator.Allocate()
	}
	_, err := p.service.Write(
		p.projectID,
		p.logName,
		&logging.WriteLogEntriesRequest{
			Entries: []*logging.LogEntry{
				{
					InsertId:      id,
					StructPayload: entry,
					Metadata: &logging.LogEntryMetadata{
						ServiceName: customServiceName,
						Severity:    severityName[entry.Level],
						Timestamp:   entry.Time.Format(time.RFC3339),
					},
				},
			},
		},
	).Do()
	return err
}

func (p *pusher) Flush() error {
	return nil
}
