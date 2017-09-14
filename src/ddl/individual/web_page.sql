drop table if exists web_page_text;
create table web_page_text
(
    wp_web_page_sk            int,
    wp_web_page_id            string,
    wp_rec_start_date         string,
    wp_rec_end_date           string,
    wp_creation_date_sk       int,
    wp_access_date_sk         int,
    wp_autogen_flag           string,
    wp_customer_sk            int,
    wp_url                    string,
    wp_type                   string,
    wp_char_count             int,
    wp_link_count             int,
    wp_image_count            int,
    wp_max_ad_count           int
)
USING csv
OPTIONS(header "false", delimiter "|", path "${TPCDS_GENDATA_DIR}/web_page")
;
drop table if exists web_page;
create table web_page 
using parquet
as (select * from web_page_text)
;
drop table if exists web_page_text;
