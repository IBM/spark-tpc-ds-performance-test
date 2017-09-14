drop table if exists customer_demographics_text;
create table customer_demographics_text
(
    cd_demo_sk                int,
    cd_gender                 string,
    cd_marital_status         string,
    cd_education_status       string,
    cd_purchase_estimate      int,
    cd_credit_rating          string,
    cd_dep_count              int,
    cd_dep_employed_count     int,
    cd_dep_college_count      int
)
USING csv
OPTIONS(header "false", delimiter "|", path "${TPCDS_GENDATA_DIR}/customer_demographics")
;
drop table if exists customer_demographics;
create table customer_demographics
using parquet
as (select * from customer_demographics_text)
;
drop table if exists customer_demographics_text;
