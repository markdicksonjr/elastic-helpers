package formats

import (
	"encoding/json"
	"github.com/olivere/elastic"
)

type ConvertJsonFn = func(*json.RawMessage) (interface{}, error)

func UnmarshalRawJson(source *json.RawMessage) (map[string]interface{}, error) {
	var item interface{}
	if err := json.Unmarshal(*source, &item); err != nil {
		return nil, err
	}
	return item.(map[string]interface{}), nil
}

func UnmarshalSearchResult(result *elastic.SearchResult) ([]map[string]interface{}, error) {
	var unmarshalledResults []map[string]interface{}

	for _, hit := range result.Hits.Hits {
		result, err := UnmarshalRawJson(hit.Source)
		if err != nil {
			return nil, err
		}
		unmarshalledResults = append(unmarshalledResults, result)
	}

	return unmarshalledResults, nil
}

func UnmarshalSearchResultFromFn(result *elastic.SearchResult, convertFn ConvertJsonFn) ([]interface{}, error) {
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
