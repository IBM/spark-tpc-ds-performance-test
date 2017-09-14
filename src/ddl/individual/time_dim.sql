drop table if exists time_dim_text;
create table time_dim_text
(
    t_time_sk                 int,
    t_time_id                 string,
    t_time                    int,
    t_hour                    int,
    t_minute                  int,
    t_second                  int,
    t_am_pm                   string,
    t_shift                   string,
    t_sub_shift               string,
    t_meal_time               string
)
USING csv
OPTIONS(header "false", delimiter "|", path "${TPCDS_GENDATA_DIR}/time_dim")
;
drop table if exists time_dim;
create table time_dim
using parquet
as (select * from time_dim_text)
;
drop table if exists time_dim_text;
