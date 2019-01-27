package operations

import (
	"context"
	"github.com/markdicksonjr/elastic-helpers"
	"github.com/markdicksonjr/elastic-helpers/formats"
	"github.com/olivere/elastic"
	"reflect"
)

func GetOne(
	client *elastic.Client,
	indexValue string,
	query elastic.Query,
	reflectType reflect.Type,
	sourceIncludes ...string,
) (interface{}, error) {
	search := client.Search().
		Index(indexValue).
		Size(1).
		Query(query)

	if sourceIncludes != nil {
		search = search.FetchSourceContext(elastic.NewFetchSourceContext(true).Include(sourceIncludes...))
	}

	baseResult, err := search.Do(context.TODO())

	if err != nil {
		return nil, err
	}

	if baseResult.Hits == nil && baseResult.Hits.Hits == nil || len(baseResult.Hits.Hits) == 0 {
		return nil, nil
	}

	return formats.UnmarshalJson(baseResult.Hits.Hits[0].Source, reflectType)
}

func GetAll(
	client *elastic.Client,
	indexValue string,
	typeValue string,
	query elastic.Query,
) ([]map[string]interface{}, error) {
	scrollerInstance := elastic_helpers.Scroller{
		Index: indexValue,
		Type: typeValue,
		Size: 1000,
		Client: client,
		Query: query,
	}

	var finalResults []map[string]interface{}
	var asyncError error
	complete := make(chan bool)
	go func() {
		asyncError = scrollerInstance.Continuous(func(result *elastic.SearchResult) error {
			results, err := formats.UnmarshalSearchResultToMap(result)
			if err != nil {
				return err
			}

			finalResults = append(finalResults, results...)

			return nil
		}, func() error {
			complete <- true
			return nil
		})

		if asyncError != nil {
			complete <- true
		}
	}()

	// wait until the complete channel is written to
	<- complete

	return finalResults, asyncError
}

/**
Sample usage:

	resultInterfaces, err := operations.GetAllGeneric(client, motorIndex, "item", elastic.NewQueryStringQuery("id:12"), func(message *json.RawMessage) (interface{}, error) {
		var e motor.Application
		err := json.Unmarshal(*message, &e)
		return e, err
	})
	if err != nil {
		return err
	}

	results = funk.Map(resultInterfaces, func(i interface{}) Data {
		return i.(Data)
	}).([]Data)
 */
func GetAllGeneric(
	client *elastic.Client,
	indexValue string,
	typeValue string,
	query elastic.Query,
	convertFn formats.ConvertJsonFn,
) ([]interface{}, error) {
	scrollerInstance := elastic_helpers.Scroller{
		Index: indexValue,
		Type: typeValue,
		Size: 1000,
		Client: client,
		Query: query,
	}

	var finalResults []interface{}
	var asyncError error
	complete := make(chan bool)
	go func() {
		asyncError = scrollerInstance.Continuous(func(result *elastic.SearchResult) error {
			results, err := formats.UnmarshalSearchResult(result, convertFn)
			if err != nil {
				return err
			}

			finalResults = append(finalResults, results...)

			return nil
		}, func() error {
			complete <- true
			return nil
		})

		if asyncError != nil {
			complete <- true
		}
	}()

	// wait until the complete channel is written to
	<- complete

	return finalResults, asyncError
}