package elastic_helpers

import (
	"context"
	"errors"
	"github.com/olivere/elastic/v7"
	"net/http"
	"syscall"
	"time"
)

// sample usage:
//client, err := elastic.NewClient(
//elastic.SetURL("http://127.0.0.1:9200"),
//elastic.SetRetrier(NewExponentialRetrier()),
//)
//if err != nil { ... }

type Retrier struct {
	backoff elastic.Backoff
}

func NewSimpleBackOff(ticks ...int) *Retrier {
	return &Retrier{
		backoff: elastic.NewSimpleBackoff(ticks...),
	}
}

func NewExponentialRetrier() *Retrier {
	return &Retrier{
		backoff: elastic.NewExponentialBackoff(10 * time.Millisecond, 8 * time.Second),
	}
}

func NewConstantBackoff(interval time.Duration) *Retrier {
	return &Retrier{
		backoff: elastic.NewConstantBackoff(interval),
	}
}

func (r *Retrier) Retry(ctx context.Context, retry int, req *http.Request, resp *http.Response, err error) (time.Duration, bool, error) {
	// fail hard on a specific error
	if err == syscall.ECONNREFUSED {
		return 0, false, errors.New("elasticsearch or network down")
	}

	// stop after 5 retries
	if retry >= 5 {
		return 0, false, nil
	}

	// let the backoff strategy decide how long to wait and whether to stop
	wait, stop := r.backoff.Next(retry)
	return wait, stop, nil
}
