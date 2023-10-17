CREATE TABLE warehouse ( w_id int, w_name char(10), w_street_1 char(20), w_street_2 char(20), w_city char(20), w_state char(2), w_zip char(9), w_tax float, w_ytd float );
CREATE TABLE item ( i_id int, i_im_id int, i_name char(24), i_price float, i_data char(50) );
CREATE TABLE stock ( s_i_id int, s_w_id int, s_quantity int, s_dist_01 char(24), s_dist_02 char(24), s_dist_03 char(24), s_dist_04 char(24), s_dist_05 char(24), s_dist_06 char(24), s_dist_07 char(24), s_dist_08 char(24), s_dist_09 char(24), s_dist_10 char(24), s_ytd int, s_order_cnt int, s_remote_cnt int, s_data char(50));
CREATE TABLE district ( d_id int, d_w_id int, d_name char(10), d_street_1 char(20), d_street_2 char(20), d_city char(20), d_state char(2), d_zip char(9), d_tax float, d_ytd float, d_next_o_id int);
CREATE TABLE customer ( c_id int, c_d_id int, c_w_id int, c_first char(16), c_middle char(2), c_last char(16), c_street_1 char(20), c_street_2 char(20), c_city char(20), c_state char(2), c_zip char(9), c_phone char(16), c_since datetime, c_credit char(2), c_credit_lim float, c_discount float, c_balance float, c_ytd_payment float, c_payment_cnt int, c_delivery_cnt int, c_data char(300) );
CREATE TABLE history ( h_c_id int, h_c_d_id int, h_c_w_id int, h_d_id int, h_w_id int, h_datetime datetime, h_amount float, h_data char(24) );
CREATE TABLE orders ( o_id int, o_d_id int, o_w_id int, o_c_id int, o_entry_d datetime, o_carrier_id int, o_ol_cnt int, o_all_local int );
CREATE TABLE new_orders ( no_o_id int, no_d_id int, no_w_id int );
CREATE TABLE order_line ( ol_o_id int, ol_d_id int, ol_w_id int, ol_number int, ol_i_id int, ol_supply_w_id int, ol_delivery_d datetime, ol_quantity int, ol_amount float, ol_dist_info char(24) );
