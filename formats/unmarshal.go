package formats

import (
	"encoding/json"
	"github.com/olivere/elastic"
	"reflect"
)

type ConvertJsonFn = func(*json.RawMessage) (interface{}, error)

func UnmarshalRawJsonToMap(source *json.RawMessage) (map[string]interface{}, error) {
	var item interface{}
	if err := json.Unmarshal(*source, &item); err != nil {
		return nil, err
	}
	return item.(map[string]interface{}), nil
}

// typeOfPtr should be the type of a pointer to the type you're unmarshalling to
// e.g. "reflect.TypeOf(&Score{})"
func UnmarshalJson(jsonString *json.RawMessage, typeOfPtr reflect.Type) (interface{}, error) {
	obj := reflect.New(typeOfPtr.Elem()).Interface()
	return obj, json.Unmarshal(*jsonString, obj)
}

func UnmarshalSearchResultToMap(result *elastic.SearchResult) ([]map[string]interface{}, error) {
	var unmarshalledResults []map[string]interface{}

	for _, hit := range result.Hits.Hits {
		result, err := UnmarshalRawJsonToMap(hit.Source)
		if err != nil {
			return nil, err
		}
		unmarshalledResults = append(unmarshalledResults, result)
	}

	return unmarshalledResults, nil
}

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

func UnmarshalSearchResultToType(result *elastic.SearchResult, typeOfPtr reflect.Type) ([]interface{}, error) {
	results := make([]interface{}, len(result.Hits.Hits))
	var err error

	for index, hit := range result.Hits.Hits {
		results[index], err = UnmarshalJson(hit.Source, typeOfPtr)
		if err != nil {
			return nil, err
		}
	}

	return results, nil
}
