drop table if exists call_center_text;
create table call_center_text
(
    cc_call_center_sk         int,
    cc_call_center_id         string,
    cc_rec_start_date         string,
    cc_rec_end_date           string,
    cc_closed_date_sk         int,
    cc_open_date_sk           int,
    cc_name                   string,
    cc_class                  string,
    cc_employees              int,
    cc_sq_ft                  int,
    cc_hours                  string,
    cc_manager                string,
    cc_mkt_id                 int,
    cc_mkt_class              string,
    cc_mkt_desc               string,
    cc_market_manager         string,
    cc_division               int,
    cc_division_name          string,
    cc_company                int,
    cc_company_name           string,
    cc_street_number          string,
    cc_street_name            string,
    cc_street_type            string,
    cc_suite_number           string,
    cc_city                   string,
    cc_county                 string,
    cc_state                  string,
    cc_zip                    string,
    cc_country                string,
    cc_gmt_offset             double,
    cc_tax_percentage         double
)
USING csv
OPTIONS(header "false", delimiter "|", path "${TPCDS_GENDATA_DIR}/call_center")
;
drop table if exists call_center;
create table call_center 
using parquet
as (select * from call_center_text)
;
drop table if exists call_center_text;
