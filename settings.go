package elastic_helpers

import (
	"context"
	"github.com/olivere/elastic/v7"
	"strconv"
)

func SetMaxResultWindow(client *elastic.Client, index string, windowSize int) (acked bool, err error) {
	jsonBody := "{ \"index\" : { \"max_result_window\" : " + strconv.Itoa(windowSize) + " } }"
	settingsRes, err := client.IndexPutSettings(index).BodyJson(jsonBody).Do(context.TODO())
	acked = settingsRes != nil && settingsRes.Acknowledged
	return
}
