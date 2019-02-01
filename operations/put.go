package operations

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/olivere/elastic"
)

func Put(
	client *elastic.Client,
	documentsToInsert []interface{},
	indexValue string,
	typeValue string,
	idFunc func(interface{}) (string, error),
) error {
	if len(documentsToInsert) == 0 {
		return nil
	}

	bulkRequest := client.Bulk()

	// build the bulk insert into EsUrl
	for _, v := range documentsToInsert {
		jsonVal, err := json.Marshal(v)

		if err != nil {
			return err
		}

		id, err := idFunc(v)

		if err != nil {
			return err
		}

		thisDoc := string(jsonVal)
		bulkRequest.Add(elastic.NewBulkIndexRequest().
			Index(indexValue).
			Type(typeValue).
			Id(id).
			Doc(thisDoc))
	}

	res, err := bulkRequest.Do(context.TODO())

	if err != nil {
		return err
	}

	if res.Errors {
		for _, item := range res.Items {
			for _, keys := range item {
				if keys.Error != nil {
					return errors.New("error(s) occurred during bulk index request: " + keys.Error.Reason)
				}
			}
		}
		return errors.New("error(s) occurred during bulk index request")
	}

	return nil
}

func PutMap(
	client *elastic.Client,
	documentsToInsert []map[string]interface{},
	indexValue string,
	typeValue string,
	idFunc func(map[string]interface{}) (string, error),
) error {
	if len(documentsToInsert) == 0 {
		return nil
	}

	bulkRequest := client.Bulk()

	// build the bulk insert into EsUrl
	for _, v := range documentsToInsert {
		jsonVal, err := json.Marshal(v)

		if err != nil {
			return err
		}

		id, err := idFunc(v)

		if err != nil {
			return err
		}

		thisDoc := string(jsonVal)
		bulkRequest.Add(elastic.NewBulkIndexRequest().
			Index(indexValue).
			Type(typeValue).
			Id(id).
			Doc(thisDoc))
	}

	res, err := bulkRequest.Do(context.TODO())

	if err != nil {
		return err
	}

	if res.Errors {
		for _, item := range res.Items {
			for _, keys := range item {
				if keys.Error != nil {
					return errors.New("error(s) occurred during map bulk index request: " + keys.Error.Reason)
				}
			}
		}
		return errors.New("error(s) occurred during map bulk index request")
	}

	return nil
}
