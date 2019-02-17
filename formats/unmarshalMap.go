package formats

import (
	"encoding/json"
	"github.com/olivere/elastic"
)

func UnmarshalRawJsonToMap(source *json.RawMessage) (map[string]interface{}, error) {
	var item interface{}
	if err := json.Unmarshal(*source, &item); err != nil {
		return nil, err
	}
	return item.(map[string]interface{}), nil
}

func UnmarshalSearchResultToMap(result *elastic.SearchResult) ([]map[string]interface{}, error) {
	var unmarshalledResults = make([]map[string]interface{}, len(result.Hits.Hits))
	var err error

	for i, hit := range result.Hits.Hits {
		unmarshalledResults[i], err = UnmarshalRawJsonToMap(hit.Source)
		if err != nil {
			return nil, err
		}
	}

	return unmarshalledResults, nil
}

