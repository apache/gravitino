select count(*) from call_center;
select count(*) from catalog_page;
select count(*) from catalog_returns;
select count(*) from catalog_sales;
select count(*) from customer;
select count(*) from customer_address;
select count(*) from customer_demographics;
select count(*) from date_dim;
select count(*) from household_demographics;
select count(*) from income_band;
select count(*) from inventory;
select count(*) from item;
select count(*) from promotion;
select count(*) from reason;
select count(*) from ship_mode;
select count(*) from store;
select count(*) from store_returns;
select count(*) from store_sales;
select count(*) from time_dim;
select count(*) from warehouse;
select count(*) from web_page;
select count(*) from web_returns;
select count(*) from web_sales;
select count(*) from web_site;

SELECT * FROM call_center ORDER BY cc_call_center_sk, cc_call_center_id, cc_rec_start_date LIMIT 10;

SELECT * FROM catalog_page ORDER BY cp_catalog_page_sk, cp_catalog_page_id, cp_start_date_sk LIMIT 10;

SELECT * FROM catalog_returns ORDER BY cr_returned_date_sk, cr_returned_time_sk, cr_item_sk LIMIT 10;

SELECT * FROM catalog_sales ORDER BY cs_sold_date_sk, cs_sold_time_sk, cs_ship_date_sk LIMIT 10;

SELECT * FROM customer ORDER BY c_customer_sk, c_customer_id, c_current_cdemo_sk LIMIT 10;

SELECT * FROM customer_address ORDER BY ca_address_sk, ca_address_id, ca_street_number LIMIT 10;

SELECT * FROM customer_demographics ORDER BY cd_demo_sk, cd_gender, cd_marital_status LIMIT 10;

SELECT * FROM date_dim ORDER BY d_date_sk, d_date_id, d_date LIMIT 10;

SELECT * FROM household_demographics ORDER BY hd_demo_sk, hd_income_band_sk, hd_buy_potential LIMIT 10;

SELECT * FROM income_band ORDER BY ib_income_band_sk, ib_lower_bound, ib_upper_bound LIMIT 10;

SELECT * FROM inventory ORDER BY inv_date_sk, inv_item_sk, inv_warehouse_sk LIMIT 10;

SELECT * FROM item ORDER BY i_item_sk, i_item_id, i_rec_start_date LIMIT 10;

SELECT * FROM promotion ORDER BY p_promo_sk, p_promo_id, p_start_date_sk LIMIT 10;

SELECT * FROM reason ORDER BY r_reason_sk, r_reason_id, r_reason_desc LIMIT 10;

SELECT * FROM ship_mode ORDER BY sm_ship_mode_sk, sm_ship_mode_id, sm_type LIMIT 10;

SELECT * FROM store ORDER BY s_store_sk, s_store_id, s_rec_start_date LIMIT 10;

SELECT * FROM store_returns ORDER BY sr_returned_date_sk, sr_return_time_sk, sr_item_sk LIMIT 10;

SELECT * FROM store_sales ORDER BY ss_sold_date_sk, ss_sold_time_sk, ss_item_sk LIMIT 10;

SELECT * FROM time_dim ORDER BY t_time_sk, t_time_id, t_time LIMIT 10;

SELECT * FROM warehouse ORDER BY w_warehouse_sk, w_warehouse_id, w_warehouse_name LIMIT 10;

SELECT * FROM web_page ORDER BY wp_web_page_sk, wp_web_page_id, wp_rec_start_date LIMIT 10;

SELECT * FROM web_returns ORDER BY wr_returned_date_sk, wr_returned_time_sk, wr_item_sk LIMIT 10;

SELECT * FROM web_sales ORDER BY ws_sold_date_sk, ws_sold_time_sk, ws_ship_date_sk LIMIT 10;

SELECT * FROM web_site ORDER BY web_site_sk, web_site_id, web_rec_start_date LIMIT 10;
