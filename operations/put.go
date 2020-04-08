package operations

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/google/uuid"
	"github.com/olivere/elastic/v7"
)

type PutResultCounts struct {
	Errors  int
	Created int
	Updated int
}

type DocumentContentsWithId struct {
	Id       string
	Document []byte
}

func PutDocuments(
	client *elastic.Client,
	documentsToInsert []DocumentContentsWithId,
	indexValue string,
	docTypeValue string,
) error {
	if len(documentsToInsert) == 0 {
		return nil
	}

	bulkRequest := client.Bulk()

	// build the bulk insert into EsUrl
	for _, v := range documentsToInsert {
		id := v.Id
		if v.Id == "" {
			id = uuid.New().String()
		}

		bulkRequest.Add(elastic.NewBulkIndexRequest().
			Index(indexValue).
			Type(docTypeValue).
			Id(id).
			Doc(string(v.Document)))
	}

	if res, err := bulkRequest.Do(context.TODO()); err != nil {
		return err
	} else if res.Errors {
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

func Put(
	client *elastic.Client,
	documentsToInsert []interface{},
	indexValue string,
	docTypeValue string,
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

		// get the id from the ID func, if it's provided
		var id string
		if idFunc != nil {
			id, err = idFunc(v)
		} else {
			id = uuid.New().String()
		}

		if err != nil {
			return err
		}

		thisDoc := string(jsonVal)
		bulkRequest.Add(elastic.NewBulkIndexRequest().
			Index(indexValue).
			Type(docTypeValue).
			RetryOnConflict(3).
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
	docTypeValue string,
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

		// get the id from the ID func, if it's provided
		var id string
		if idFunc != nil {
			id, err = idFunc(v)
		} else {
			id = uuid.New().String()
		}

		if err != nil {
			return err
		}

		thisDoc := string(jsonVal)
		bulkRequest.Add(elastic.NewBulkIndexRequest().
			Index(indexValue).
			Type(docTypeValue).
			RetryOnConflict(3).
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

func PutMapWithResults(
	client *elastic.Client,
	documentsToInsert []map[string]interface{},
	indexValue string,
	docTypeValue string,
	idFunc func(map[string]interface{}) (string, error),
) (PutResultCounts, error) {
	if len(documentsToInsert) == 0 {
		return PutResultCounts{
			Errors:  0,
			Updated: 0,
			Created: 0,
		}, nil
	}

	bulkRequest := client.Bulk()

	// build the bulk insert into EsUrl
	for _, v := range documentsToInsert {
		jsonVal, err := json.Marshal(v)

		if err != nil {
			return PutResultCounts{
				Errors:  0,
				Updated: 0,
				Created: 0,
			}, err
		}

		// get the id from the ID func, if it's provided
		var id string
		if idFunc != nil {
			id, err = idFunc(v)
		} else {
			id = uuid.New().String()
		}

		if err != nil {
			return PutResultCounts{
				Errors:  0,
				Updated: 0,
				Created: 0,
			}, err
		}

		thisDoc := string(jsonVal)
		bulkRequest.Add(elastic.NewBulkIndexRequest().
			Index(indexValue).
			Type(docTypeValue).
			Id(id).
			Doc(thisDoc))
	}

	res, err := bulkRequest.Do(context.TODO())

	counts := PutResultCounts{
		Errors:  0,
		Updated: 0,
		Created: 0,
	}

	if res != nil {
		errorReason := ""
		for _, item := range res.Items {
			for _, keys := range item {
				if keys.Error != nil {
					errorReason = keys.Error.Reason
					counts.Errors++
				}
				if keys.Result == "updated" {
					counts.Updated++
				} else if keys.Result == "created" {
					counts.Created++
				}
			}
		}

		if len(errorReason) > 0 {
			return counts, errors.New("error(s) occurred during map bulk index request: " + errorReason)
		}

		if res.Errors {
			return counts, errors.New("error(s) occurred during map bulk index request")
		}
	}

	return counts, err
}
