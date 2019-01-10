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
			results, err := formats.UnmarshalSearchResult(result)
			if err != nil {
				return err
			}

			finalResults = append(finalResults, results...)

			return nil
		}, func() error {
			complete <- true
			return nil
		})
	}()

	// wait until the complete channel is written to
	<- complete

	return finalResults, asyncError
}

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
			results, err := formats.UnmarshalSearchResultFromFn(result, convertFn)
			if err != nil {
				return err
			}

			finalResults = append(finalResults, results...)

			return nil
		}, func() error {
			complete <- true
			return nil
		})
	}()

	// wait until the complete channel is written to
	<- complete

	return finalResults, asyncError
}