package operations

import (
	"github.com/markdicksonjr/elastic-helpers"
	"github.com/markdicksonjr/elastic-helpers/formats"
	"github.com/olivere/elastic"
)

func GetAll(
	client *elastic.Client,
	indexValue string,
	typeValue string,
	query elastic.Query,
	sourceIncludes ...string,
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
		asyncError = scrollerInstance.Continuous(func(result *elastic.SearchResult, _ int) error {
			results, err := formats.UnmarshalSearchResultToMap(result)
			if err != nil {
				return err
			}

			finalResults = append(finalResults, results...)

			return nil
		}, func() error {
			complete <- true
			return nil
		}, sourceIncludes...)

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
	sourceIncludes ...string,
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
		asyncError = scrollerInstance.Continuous(func(result *elastic.SearchResult, _ int) error {
			results, err := formats.UnmarshalSearchResult(result, convertFn)
			if err != nil {
				return err
			}

			finalResults = append(finalResults, results...)

			return nil
		}, func() error {
			complete <- true
			return nil
		}, sourceIncludes...)

		if asyncError != nil {
			complete <- true
		}
	}()

	// wait until the complete channel is written to
	<- complete

	return finalResults, asyncError
}
