drop table if exists catalog_page_text;
create table catalog_page_text
(
    cp_catalog_page_sk        int,
    cp_catalog_page_id        string,
    cp_start_date_sk          int,
    cp_end_date_sk            int,
    cp_department             string,
    cp_catalog_number         int,
    cp_catalog_page_number    int,
    cp_description            string,
    cp_type                   string
)
USING csv
OPTIONS(header "false", delimiter "|", path "${TPCDS_GENDATA_DIR}/catalog_page")
;
drop table if exists catalog_page;
create table catalog_page
using parquet
as (select * from catalog_page_text)
;
drop table if exists catalog_page_text;
