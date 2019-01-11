package formats

import (
	"encoding/json"
	"github.com/olivere/elastic"
)

type ConvertJsonFn = func(*json.RawMessage) (interface{}, error)

func UnmarshalSearchResult(result *elastic.SearchResult, convertFn ConvertJsonFn) ([]interface{}, error) {
	var unmarshalledResults []interface{}

	for _, hit := range result.Hits.Hits {
		result, err := convertFn(hit.Source)
		if err != nil {
			return nil, err
		}
		unmarshalledResults = append(unmarshalledResults, result)
	}

	return unmarshalledResults, nil
}
