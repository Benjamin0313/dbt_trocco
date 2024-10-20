resource "trocco_bigquery_datamart_definition" "test" {
  name                     = "test"
  is_runnable_concurrently = false
  bigquery_connection_id   = 3302
  query                    = "SELECT * FROM `trocco-dbt.trocco_job_deploy_mart.sample_mart_tbl`"
  query_mode               = "insert"
  destination_dataset      = "trocco_job_deploy_mart"
  destination_table        = "test"
  write_disposition        = "truncate"
}