package elastic_helpers

import (
	"encoding/base64"
	"github.com/olivere/elastic/v7"
	"net/http"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"
)

func TestScroller_Continuous(t *testing.T) {
	esUrlEnv := os.Getenv("ELASTIC_URL")
	if esUrlEnv == "" {
		t.Skipped()
		return
	}

	esUrl, err := url.Parse(esUrlEnv)
	if err != nil {
		t.Fatal(err)
	}

	opts := GetEsV7Options(esUrl, true)

	// connect to the input ES
	esClientInput, err := elastic.NewClient(opts...)
	if err != nil {
		t.Fatal(err)
	}

	scroller := Scroller{
		Client:    esClientInput,
		Index:     os.Getenv("ELASTIC_INDEX"),
		Type:      "_doc",
		Query:     nil,
		Body:      nil,
		Size:      100,
		KeepAlive: "5m",
		Sort:      "__modified:desc",
		UsePIT:    true,
	}

	err = scroller.ContinuousWithRetry(func(result elastic.SearchResult, index int) error {
		// TODO: validate here
		return nil
	}, func() error {
		return nil
	}, 3)
	if err != nil {
		t.Fatal(err)
	}
}

func GetEsV7Options(url *url.URL, useGzip bool) []elastic.ClientOptionFunc {
	opts := []elastic.ClientOptionFunc{
		elastic.SetSniff(false),
		//elastic_v7.SetHealthcheck(false),
		elastic.SetHealthcheckTimeout(15 * time.Second),
		elastic.SetHealthcheckTimeoutStartup(15 * time.Second),
		elastic.SetURL(url.Scheme + "://" + url.Host),
		elastic.SetRetrier(&Retrier{
			backoff: elastic.NewExponentialBackoff(10 * time.Millisecond, 8 * time.Second),
		}),
		elastic.SetGzip(useGzip),
	}

	// handle username/pw in http://<user>:<pw>@host:port or http://APIKEY<key id>:<key secret>@host:port
	if url.User != nil {
		if strings.HasPrefix(url.User.Username(), "APIKEY") {
			user := url.User.Username()[6:]
			pw, _ := url.User.Password()

			apiKeyHeader := http.Header{}
			apiKeyHeader.Add("Authorization", "ApiKey "+base64.StdEncoding.EncodeToString([]byte(user+":"+pw)))
			opts = append(opts, elastic.SetHeaders(apiKeyHeader))
			opts = append(opts, elastic.SetHealthcheck(false)) // olivere does not apply api key appropriately for health checks
		} else {
			pw, _ := url.User.Password()
			opts = append(opts, elastic.SetBasicAuth(url.User.Username(), pw))
		}
	}

	return opts
}