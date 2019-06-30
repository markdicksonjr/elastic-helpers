package operations

import (
	"context"
	"github.com/markdicksonjr/elastic-helpers/formats"
	"github.com/olivere/elastic/v7"
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
