drop table if exists reason_text;
create table reason_text
(
    r_reason_sk               int,
    r_reason_id               string,
    r_reason_desc             string
)
USING csv
OPTIONS(header "false", delimiter "|", path "${TPCDS_GENDATA_DIR}/reason")
;
drop table if exists reason;
create table reason 
using parquet
as (select * from reason_text)
;
drop table if exists reason_text;
