package main

import (
	"fmt"
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/athena"
)

/**
 * Submits a sample query to Athena and returns the execution ID of the query.
 */
var rid string
var input athena.GetQueryResultsInput

func submitAthenaQuery(query string, outputBucket string, client *athena.Athena) (string, error) {
	//The result configuration specifies where the results of the query should go in S3 and encryption options

	resultConf := &athena.ResultConfiguration{
		OutputLocation: aws.String(outputBucket),
	}
	// The Parameters passed into the Execution Request which includes the query string and the Output Location.
	params := &athena.StartQueryExecutionInput{
		QueryString:         aws.String(query),
		ResultConfiguration: resultConf,
	}

	req, resp := client.StartQueryExecutionRequest(params)

	err := req.Send()
	if err != nil {
		fmt.Printf("StartQueryExecutionRequest error is : %v\n", err)
		return "", err
	}
	rid = *resp.QueryExecutionId
	return *resp.QueryExecutionId, nil
}

func awaitAthenaQuery(executionID string, client *athena.Athena) error {
	params2 := &athena.GetQueryExecutionInput{
		QueryExecutionId: &executionID,
	}
	duration := time.Duration(10) * time.Second // Pause for 2 seconds

	for {
		executionOutput, err := client.GetQueryExecution(params2)
		if err != nil {
			fmt.Println(err)
			return err
		}
		if *executionOutput.QueryExecution.Status.State != "RUNNING" {
			fmt.Println("Query is complete!")
			if *executionOutput.QueryExecution.Status.State == "FAILED" {
				return fmt.Errorf("Query encountered an error: %v", *executionOutput.QueryExecution.Status.StateChangeReason)
			}
			break
		}
		fmt.Println("Waiting for query to finish...")
		time.Sleep(duration)
	}
	return nil
}

/*
 * This code calls Athena and retrieves the results of a query.
 */

func processAthenaResults(executionID string, client *athena.Athena) error {

	input.SetQueryExecutionId(executionID)

	results, err := client.GetQueryResults(&input)
	if err != nil {
		fmt.Println(err)
		return err
	}
	//fmt.Print(results.ResultSet.Rows)

	//to calculate mean value
	cost, err := strconv.ParseFloat(*results.ResultSet.Rows[1].Data[1].VarCharValue, 8)
	if err != nil {
		fmt.Println(err)
	}

	fmt.Printf("Mean value = %.2f\n", cost/30)

	return nil
}

func main() {

	sess := session.Must(session.NewSessionWithOptions(session.Options{
		Profile:           "dev",
		SharedConfigState: session.SharedConfigEnable,
	}))
	// Build an AthenaClient client
	client := athena.New(sess)

	// The outputBucket is the bucket result will sit in
	outputBucket := "s3://report-bucket-aqfer/CURfile/query-result/"

	//mention service
	service := "AmazonS3"

	//query := "SELECT line_item_usage_amount  FROM athenacurcfn_a_w_s_c_u_r.varsh where line_item_product_code='AmazonEC2' limit 20;"
	query_by_service_last30 := "SELECT line_item_product_code, round(sum(cast(line_item_unblended_cost AS double)),2) AS sum_unblended_cost FROM athenacurcfn_a_w_s_c_u_r.varsh  where line_item_product_code='" + service + "' and line_item_usage_start_date between timestamp '2022-03-01' AND timestamp '2022-04-01' GROUP BY line_item_product_code "
	strExecutionID, err := submitAthenaQuery(query_by_service_last30, outputBucket, client)
	if err != nil {
		fmt.Println(err)
		return
	}

	fmt.Printf("Initialized & submitted query for query ID: %v\n", strExecutionID)

	err2 := awaitAthenaQuery(strExecutionID, client)
	if err2 != nil {
		fmt.Println(err2)
		return
	}

	err3 := processAthenaResults(strExecutionID, client)
	if err3 != nil {
		fmt.Println(err3)
		return
	}
