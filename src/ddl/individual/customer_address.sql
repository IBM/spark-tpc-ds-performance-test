drop table if exists customer_address_text;
create table customer_address_text
(
    ca_address_sk             int,
    ca_address_id             string,
    ca_street_number          string,
    ca_street_name            string,
    ca_street_type            string,
    ca_suite_number           string,
    ca_city                   string,
    ca_county                 string,
    ca_state                  string,
    ca_zip                    string,
    ca_country                string,
    ca_gmt_offset             double,
    ca_location_type          string
)
USING csv
OPTIONS(header "false", delimiter "|", path "${TPCDS_GENDATA_DIR}/customer_address")
;
drop table if exists customer_address;
create table customer_address
using parquet
as (select * from customer_address_text)
;
drop table if exists customer_address_text;
