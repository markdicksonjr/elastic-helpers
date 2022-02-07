package elastic_helpers

import (
	"context"
	"errors"
	"github.com/olivere/elastic/v7"
	"io"
	"strconv"
	"strings"
	"time"
)

type Scroller struct {
	Client    *elastic.Client
	Index     string
	Type      string
	Query     elastic.Query
	Body      interface{}
	Size      int
	KeepAlive string
	Sort      string
	UsePIT    bool

	scrollId string
}

func (s *Scroller) Continuous(
	onBatch func(result elastic.SearchResult, index int) error,
	onComplete func() error,
	sourceIncludes ...string,
) error {
	return s.ContinuousWithRetry(onBatch, onComplete, 3, sourceIncludes...)
}

func (s *Scroller) ContinuousWithRetry(
	onBatch func(result elastic.SearchResult, index int) error,
	onComplete func() error,
	retriesRemaining int,
	sourceIncludes ...string,
) error {
	var pit *elastic.OpenPointInTimeService

	if s.Sort != "" && s.UsePIT {
		pit = s.Client.OpenPointInTime().Index(s.Index)

		if s.KeepAlive != "" {
			pit = pit.KeepAlive(s.KeepAlive)
		}

		pitResponse, err := pit.Do(context.TODO())
		if err != nil {
			return err
		}

		defer s.Client.ClosePointInTime(pitResponse.Id)

		service := s.Client.Search().Size(s.Size).PointInTime(elastic.NewPointInTime(pitResponse.Id)).TrackTotalHits(true)

		if s.Query != nil {
			service = service.Query(s.Query)
		}

		// TODO: can't use body

		if sourceIncludes != nil {
			service = service.FetchSourceContext(elastic.NewFetchSourceContext(true).Include(sourceIncludes...))
		}

		// if sort is specified (e.g. "timestamp:asc" or "timestamp:desc"), apply it - default to asc if in an unknown form
		if s.Sort != "" {
			parts := strings.Split(s.Sort, ":")
			asc := true
			if len(parts) == 2 && parts[1] == "desc" {
				asc = false
			}
			service = service.Sort(parts[0], asc)
		}

		res, err := service.Do(context.TODO())
		if err != nil {
			if err == io.EOF {
				return onComplete()
			}

			return err
		}
		if res == nil {
			return errors.New("expected results != nil; got nil")
		}

		index := 0
		if err = onBatch(*res, index); err != nil {
			return err
		}
		index++

		complete := false
		for !complete {
			service := s.Client.Search().From(s.Size * index).Size(s.Size).PointInTime(elastic.NewPointInTime(pitResponse.Id))

			res, err := service.Do(context.TODO())
			if err == io.EOF {
				complete = true
				continue
			}

			if err != nil {
				if retriesRemaining > 0 {
					time.Sleep(time.Duration(45) * time.Second)
					retriesRemaining--
					continue
				}
				return err
			}

			if res.Hits == nil || len(res.Hits.Hits) == 0 {
				complete = true
			}

			if err = onBatch(*res, index); err != nil {
				// NOTE: any error from clearing the scroll is discarded
				return err
			}

			// dereference to give GC a hint
			res.Hits = nil

			index++
		}

		return onComplete()
	}

	service := s.Client.Scroll(s.Index).Type(s.Type).Size(s.Size)

	if s.Query != nil {
		service = service.Query(s.Query)
	}

	if s.Body != nil {
		service = service.Body(s.Body)
	}

	if sourceIncludes != nil {
		service = service.FetchSourceContext(elastic.NewFetchSourceContext(true).Include(sourceIncludes...))
	}

	if s.KeepAlive != "" {
		service = service.KeepAlive(s.KeepAlive)
	}

	// if sort is specified (e.g. "timestamp:asc" or "timestamp:desc"), apply it - default to asc if in an unknown form
	if s.Sort != "" {
		parts := strings.Split(s.Sort, ":")
		asc := true
		if len(parts) == 2 && parts[1] == "desc" {
			asc = false
		}
		service = service.Sort(parts[0], asc)
	}

	res, err := service.Do(context.TODO())
	if err != nil {
		if err == io.EOF {
			return onComplete()
		}

		return err
	}
	if res == nil {
		return errors.New("expected results != nil; got nil")
	}
	if res.ScrollId == "" {
		return errors.New("expected scrollId in results; got \"\"")
	}

	settings, err := s.Client.IndexGetSettings(s.Index).Do(context.TODO())
	if err != nil {
		return err
	}

	currentWindow := 1000
	if settings[s.Index] != nil {
		indexVal, _ := settings[s.Index]
		if indexVal.Settings["index"] != nil {
			indexInfo, _ := indexVal.Settings["index"].(map[string]interface{})
			if indexInfo != nil && indexInfo["max_result_window"] != nil {
				windowText, _ := indexInfo["max_result_window"].(string)
				if windowText != "" {
					parsedWindow, err := strconv.ParseInt(windowText, 10, 32)
					if err == nil {
						currentWindow = int(parsedWindow)
					}
				}
			}
		}
	}

	if currentWindow < s.Size {
		if acked, err := SetMaxResultWindowForIndex(s.Client, s.Index, s.Size); err != nil {
			return err
		} else if !acked {
			s.Size = currentWindow
		}
	}

	index := 0
	if err = onBatch(*res, index); err != nil {
		return err
	}
	index++

	scrollId := res.ScrollId

	complete := false
	for !complete {
		service = s.Client.Scroll(s.Index).ScrollId(scrollId).Size(s.Size)

		if s.KeepAlive != "" {
			service = service.KeepAlive(s.KeepAlive)
		}

		res, err := service.Do(context.TODO())
		if err == io.EOF {
			complete = true
			continue
		}

		if err != nil {
			if retriesRemaining > 0 {
				time.Sleep(time.Duration(45) * time.Second)
				retriesRemaining--
				continue
			}
			_, _ = s.Client.ClearScroll(scrollId).Do(context.TODO())
			return err
		}

		if res.Hits == nil || len(res.Hits.Hits) == 0 {
			complete = true
		}

		if err = onBatch(*res, index); err != nil {
			// NOTE: any error from clearing the scroll is discarded
			_, _ = s.Client.ClearScroll(scrollId).Do(context.TODO())
			return err
		}

		// dereference to give GC a hint
		res.Hits = nil

		index++
	}

	_, err = s.Client.ClearScroll(res.ScrollId).Do(context.TODO())
	if err != nil {
		return err
	}

	return onComplete()
}

func (s *Scroller) ContinuousBlocking(
	onBatch func(result elastic.SearchResult, index int) error,
	onComplete func() error,
	sourceIncludes ...string,
) error {
	return s.ContinuousBlockingWithRetry(onBatch, onComplete, 3, sourceIncludes...)
}

func (s *Scroller) ContinuousBlockingWithRetry(
	onBatch func(result elastic.SearchResult, index int) error,
	onComplete func() error,
	retriesRemaining int,
	sourceIncludes ...string,
) error {
	var err error
	complete := make(chan bool)

	go func() {
		if err = s.ContinuousWithRetry(onBatch, func() error {
			complete <- true
			return onComplete()
		}, retriesRemaining, sourceIncludes...); err != nil {
			complete <- true
		}
	}()

	<-complete

	return err
}
