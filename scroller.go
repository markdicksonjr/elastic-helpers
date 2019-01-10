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
	Size		int
	KeepAlive	string

	scrollId	string
}

func (s *Scroller) Continuous(
	onBatch func(result *elastic.SearchResult) error,
	onComplete func() error,
) error {
	service := s.Client.Scroll(s.Index).Type(s.Type).Query(s.Query).Size(s.Size)

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

	err = onBatch(res)

	complete := false
	for !complete {
		res, err := s.Client.Scroll(s.Index).ScrollId(res.ScrollId).Size(s.Size).Do(context.TODO())

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

