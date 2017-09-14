drop table if exists ship_mode_text;
create table ship_mode_text
(
    sm_ship_mode_sk           int,
    sm_ship_mode_id           string,
    sm_type                   string,
    sm_code                   string,
    sm_carrier                string,
    sm_contract               string
)
USING csv
OPTIONS(header "false", delimiter "|", path "${TPCDS_GENDATA_DIR}/ship_mode")
;
drop table if exists ship_mode;
create table ship_mode
using parquet
as (select * from ship_mode_text)
;
drop table if exists ship_mode_text;
