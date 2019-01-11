package formats

import (
	"encoding/json"
	"github.com/olivere/elastic"
	"reflect"
)

// typeOfPtr should be the type of a pointer to the type you're unmarshalling to
// e.g. "reflect.TypeOf(&Score{})"
func UnmarshalJson(jsonString *json.RawMessage, typeOfPtr reflect.Type) (interface{}, error) {
	obj := reflect.New(typeOfPtr.Elem()).Interface()
	return obj, json.Unmarshal(*jsonString, obj)
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
