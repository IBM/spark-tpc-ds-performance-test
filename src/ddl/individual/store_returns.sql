drop table if exists store_returns_text;
create table store_returns_text
(
    sr_returned_date_sk       int,
    sr_return_time_sk         int,
    sr_item_sk                int,
    sr_customer_sk            int,
    sr_cdemo_sk               int,
    sr_hdemo_sk               int,
    sr_addr_sk                int,
    sr_store_sk               int,
    sr_reason_sk              int,
    sr_ticket_number          int,
    sr_return_quantity        int,
    sr_return_amt             double,
    sr_return_tax             double,
    sr_return_amt_inc_tax     double,
    sr_fee                    double,
    sr_return_ship_cost       double,
    sr_refunded_cash          double,
    sr_reversed_charge        double,
    sr_store_credit           double,
    sr_net_loss               double
)
USING csv
OPTIONS(header "false", delimiter "|", path "${TPCDS_GENDATA_DIR}/store_returns")
;
drop table if exists store_returns;
create table store_returns 
using parquet
as (select * from store_returns_text)
;
drop table if exists store_returns_text;
