package elastic_helpers

import (
	"context"
	"errors"
	"github.com/olivere/elastic"
	"io"
)

type Scroller struct {
	Client		*elastic.Client
	Index 		string
	Type 		string
	Query		elastic.Query
	Body		interface{}
	Size		int
	KeepAlive	string

	scrollId	string
}

func (s *Scroller) Continuous(
	onBatch func(result *elastic.SearchResult) error,
	onComplete func() error,
	sourceIncludes ...string,
) error {
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

	res, err :=  service.Do(context.TODO())
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

	if err = onBatch(res); err != nil {
		return err
	}

	complete := false
	for !complete {
		service := s.Client.Scroll(s.Index).ScrollId(res.ScrollId).Size(s.Size)

		if s.KeepAlive != "" {
			service = service.KeepAlive(s.KeepAlive)
		}

		res, err := service.Do(context.TODO())
		if err == io.EOF {
			complete = true
			continue
		}

		if err != nil {
			return err
		}

		if res.Hits == nil || len(res.Hits.Hits) == 0 {
			complete = true
		}

		err = onBatch(res)
	}

	return onComplete()
}

func (s *Scroller) ContinuousBlocking(
	onBatch func(result *elastic.SearchResult) error,
	onComplete func() error,
	sourceIncludes ...string,
) error {
	var err error
	complete := make(chan bool)

	go func() {
		if err = s.Continuous(onBatch, func() error {
			complete <- true
			return onComplete()
		}, sourceIncludes...); err != nil {
			complete <- true
		}
	}()

	<- complete

	return err
}
