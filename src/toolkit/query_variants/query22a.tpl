--
-- Legal Notice
--
-- This document and associated source code (the "Work") is a part of a
-- benchmark specification maintained by the TPC.
--
-- The TPC reserves all right, title, and interest to the Work as provided
-- under U.S. and international laws, including without limitation all patent
-- and trademark rights therein.
--
-- No Warranty
--
-- 1.1 TO THE MAXIMUM EXTENT PERMITTED BY APPLICABLE LAW, THE INFORMATION
--     CONTAINED HEREIN IS PROVIDED "AS IS" AND WITH ALL FAULTS, AND THE
--     AUTHORS AND DEVELOPERS OF THE WORK HEREBY DISCLAIM ALL OTHER
--     WARRANTIES AND CONDITIONS, EITHER EXPRESS, IMPLIED OR STATUTORY,
--     INCLUDING, BUT NOT LIMITED TO, ANY (IF ANY) IMPLIED WARRANTIES,
--     DUTIES OR CONDITIONS OF MERCHANTABILITY, OF FITNESS FOR A PARTICULAR
--     PURPOSE, OF ACCURACY OR COMPLETENESS OF RESPONSES, OF RESULTS, OF
--     WORKMANLIKE EFFORT, OF LACK OF VIRUSES, AND OF LACK OF NEGLIGENCE.
--     ALSO, THERE IS NO WARRANTY OR CONDITION OF TITLE, QUIET ENJOYMENT,
--     QUIET POSSESSION, CORRESPONDENCE TO DESCRIPTION OR NON-INFRINGEMENT
--     WITH REGARD TO THE WORK.
-- 1.2 IN NO EVENT WILL ANY AUTHOR OR DEVELOPER OF THE WORK BE LIABLE TO
--     ANY OTHER PARTY FOR ANY DAMAGES, INCLUDING BUT NOT LIMITED TO THE
--     COST OF PROCURING SUBSTITUTE GOODS OR SERVICES, LOST PROFITS, LOSS
--     OF USE, LOSS OF DATA, OR ANY INCIDENTAL, CONSEQUENTIAL, DIRECT,
--     INDIRECT, OR SPECIAL DAMAGES WHETHER UNDER CONTRACT, TORT, WARRANTY,
--     OR OTHERWISE, ARISING IN ANY WAY OUT OF THIS OR ANY OTHER AGREEMENT
--     RELATING TO THE WORK, WHETHER OR NOT SUCH AUTHOR OR DEVELOPER HAD
--     ADVANCE NOTICE OF THE POSSIBILITY OF SUCH DAMAGES.
--
-- Contributors:
--

define DMS = random(1176,1224,uniform);
define _LIMIT=100;

with results as
(select  i_product_name
             ,i_brand
             ,i_class
             ,i_category
             ,inv_quantity_on_hand qoh
       from inventory
           ,date_dim
           ,item
           ,warehouse
       where  inv_date_sk=d_date_sk
              and inv_item_sk=i_item_sk
              and inv_warehouse_sk = w_warehouse_sk
              and d_month_seq between [DMS] and [DMS] + 11
--       group by i_product_name,i_brand,i_class,i_category
),
results_rollup as
(select i_product_name, i_brand, i_class, i_category,avg(qoh) qoh
from results
group by i_product_name,i_brand,i_class,i_category
union all
select i_product_name, i_brand, i_class, null i_category,avg(qoh) qoh
from results
group by i_product_name,i_brand,i_class
union all
select i_product_name, i_brand, null i_class, null i_category,avg(qoh) qoh
from results
group by i_product_name,i_brand
union all
select i_product_name, null i_brand, null i_class, null i_category,avg(qoh)  qoh
from results
group by i_product_name
union all
select null i_product_name, null i_brand, null i_class, null i_category,avg(qoh) qoh
from results)
[_LIMITA] select [_LIMITB] i_product_name, i_brand, i_class, i_category,qoh
      from results_rollup
      order by qoh, i_product_name, i_brand, i_class, i_category
[_LIMITC];
