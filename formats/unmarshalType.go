package formats

import (
	"encoding/json"
	"github.com/olivere/elastic"
	"reflect"
)

// e.g. "reflect.TypeOf(Score{})"
func UnmarshalJson(jsonString *json.RawMessage, typeOfVal reflect.Type) (interface{}, error) {
	obj := reflect.New(typeOfVal).Interface()
	return obj, json.Unmarshal(*jsonString, obj)
}

func UnmarshalSearchResultToType(result *elastic.SearchResult, typeOfVal reflect.Type) ([]interface{}, error) {
	results := make([]interface{}, len(result.Hits.Hits))
	var err error

	for index, hit := range result.Hits.Hits {
		results[index], err = UnmarshalJson(hit.Source, typeOfVal)
		if err != nil {
			return nil, err
		}
	}

	return results, nil
}
