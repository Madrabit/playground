package httpapi

import (
	"fmt"
	"playground/in-memory-scheduler/internal/httpdto"

	"playground/in-memory-scheduler/internal/domain"
)

func toJobResponse(job domain.Job) httpdto.JobResponse {
	attrs := job.Attrs
	if attrs == nil {
		attrs = map[string]string{}
	}

	tags := job.Tags
	if tags == nil {
		tags = map[string]struct{}{}
	}

	return httpdto.JobResponse{
		ID:          job.ID,
		Name:        job.Name,
		Description: job.Description,
		Complexity:  job.Complexity,
		Attrs:       attrs,
		Tags:        tags,
		State:       fmt.Sprint(job.State),
	}
}
