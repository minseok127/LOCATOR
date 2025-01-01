CREATE OR REPLACE PROCEDURE EXPLAIN_NEWORD (
no_w_id         IN INTEGER,
no_max_w_id     IN INTEGER,
no_d_id         IN INTEGER,
no_c_id         IN INTEGER,
no_o_ol_cnt     IN INTEGER,
no_c_discount   INOUT NUMERIC,
no_c_last       INOUT VARCHAR,
no_c_credit     INOUT VARCHAR,
no_d_tax        INOUT NUMERIC,
no_w_tax        INOUT NUMERIC,
no_d_next_o_id  INOUT INTEGER,
tstamp          IN TIMESTAMP )
AS $$
DECLARE
    no_s_quantity		NUMERIC;
    no_o_all_local		SMALLINT;
    rbk					SMALLINT;
    item_id_array 		INT[];
    supply_wid_array	INT[];
    quantity_array		SMALLINT[];
    order_line_array	SMALLINT[];
    stock_dist_array	CHAR(24)[];
    s_quantity_array	SMALLINT[];
    price_array			NUMERIC(5,2)[];
    amount_array		NUMERIC(5,2)[];
BEGIN
	no_o_all_local := 1;
	
	EXPLAIN SELECT c_discount, c_last, c_credit, w_tax
	INTO no_c_discount, no_c_last, no_c_credit, no_w_tax
	FROM customer, warehouse
	WHERE warehouse.w_id = no_w_id AND customer.c_w_id = no_w_id AND customer.c_d_id = no_d_id AND customer.c_id = no_c_id;

	--#2.4.1.4
	rbk := round(DBMS_RANDOM(1,100));
    rbk := 2;
	--#2.4.1.5
	FOR loop_counter IN 1 .. no_o_ol_cnt
	LOOP
		IF ((loop_counter = no_o_ol_cnt) AND (rbk = 1))
		THEN
			item_id_array[loop_counter] := 100001;
		ELSE
			item_id_array[loop_counter] := round(DBMS_RANDOM(1,100000));
		END IF;

		--#2.4.1.5.2
		IF ( round(DBMS_RANDOM(1,100)) > 1 )
		THEN
			supply_wid_array[loop_counter] := no_w_id;
		ELSE
			no_o_all_local := 0;
			supply_wid_array[loop_counter] := 1 + MOD(CAST (no_w_id + round(DBMS_RANDOM(0,no_max_w_id-1)) AS INT), no_max_w_id);
		END IF;

		--#2.4.1.5.3
		quantity_array[loop_counter] := round(DBMS_RANDOM(1,10));
		order_line_array[loop_counter] := loop_counter;
	END LOOP;

	/*+ IndexScan(district) */ EXPLAIN ANALYZE UPDATE district SET d_next_o_id = d_next_o_id + 1 WHERE d_id = no_d_id AND d_w_id = no_w_id RETURNING d_next_o_id, d_tax INTO no_d_next_o_id, no_d_tax;

	INSERT INTO ORDERS (o_id, o_d_id, o_w_id, o_c_id, o_entry_d, o_ol_cnt, o_all_local) VALUES (no_d_next_o_id, no_d_id, no_w_id, no_c_id, current_timestamp, no_o_ol_cnt, no_o_all_local);
	INSERT INTO NEW_ORDER (no_o_id, no_d_id, no_w_id) VALUES (no_d_next_o_id, no_d_id, no_w_id);

	SELECT array_agg ( i_price )
	INTO price_array
	FROM UNNEST(item_id_array) item_id
	LEFT JOIN item ON i_id = item_id;

	IF no_d_id = 1
	THEN
		WITH stock_update AS (
	        /*+ IndexScan(stock) */ UPDATE stock
    	       SET s_quantity = ( CASE WHEN s_quantity < (item_stock.quantity + 10) THEN s_quantity + 91 ELSE s_quantity END) - item_stock.quantity
			  FROM UNNEST(item_id_array, supply_wid_array, quantity_array, price_array)
				   AS item_stock (item_id, supply_wid, quantity, price)
			 WHERE stock.s_i_id = item_stock.item_id
			   AND stock.s_w_id = item_stock.supply_wid
			   AND stock.s_w_id = ANY(supply_wid_array)
			RETURNING stock.s_dist_01 as s_dist, stock.s_quantity, ( item_stock.quantity + item_stock.price * ( 1 + no_w_tax + no_d_tax ) * ( 1 - no_c_discount ) ) amount
    	)
		SELECT array_agg ( s_dist ), array_agg ( s_quantity ), array_agg ( amount )
		FROM stock_update
		INTO stock_dist_array, s_quantity_array, amount_array;
	ELSIF no_d_id = 2
	THEN
		WITH stock_update AS (
	        /*+ IndexScan(stock) */ UPDATE stock
    	       SET s_quantity = ( CASE WHEN s_quantity < (item_stock.quantity + 10) THEN s_quantity + 91 ELSE s_quantity END) - item_stock.quantity
			  FROM UNNEST(item_id_array, supply_wid_array, quantity_array, price_array)
				   AS item_stock (item_id, supply_wid, quantity, price)
			 WHERE stock.s_i_id = item_stock.item_id
			   AND stock.s_w_id = item_stock.supply_wid
			   AND stock.s_w_id = ANY(supply_wid_array)
			RETURNING stock.s_dist_02 as s_dist, stock.s_quantity, ( item_stock.quantity + item_stock.price * ( 1 + no_w_tax + no_d_tax ) * ( 1 - no_c_discount ) ) amount
    	)
		SELECT array_agg ( s_dist ), array_agg ( s_quantity ), array_agg ( amount )
		FROM stock_update
		INTO stock_dist_array, s_quantity_array, amount_array;
	ELSIF no_d_id = 3
	THEN
		WITH stock_update AS (
	        /*+ IndexScan(stock) */ UPDATE stock
    	       SET s_quantity = ( CASE WHEN s_quantity < (item_stock.quantity + 10) THEN s_quantity + 91 ELSE s_quantity END) - item_stock.quantity
			  FROM UNNEST(item_id_array, supply_wid_array, quantity_array, price_array)
				   AS item_stock (item_id, supply_wid, quantity, price)
			 WHERE stock.s_i_id = item_stock.item_id
			   AND stock.s_w_id = item_stock.supply_wid
			   AND stock.s_w_id = ANY(supply_wid_array)
			RETURNING stock.s_dist_03 as s_dist, stock.s_quantity, ( item_stock.quantity + item_stock.price * ( 1 + no_w_tax + no_d_tax ) * ( 1 - no_c_discount ) ) amount
    	)
		SELECT array_agg ( s_dist ), array_agg ( s_quantity ), array_agg ( amount )
		FROM stock_update
		INTO stock_dist_array, s_quantity_array, amount_array;
	ELSIF no_d_id = 4
	THEN
		WITH stock_update AS (
	        /*+ IndexScan(stock) */ UPDATE stock
    	       SET s_quantity = ( CASE WHEN s_quantity < (item_stock.quantity + 10) THEN s_quantity + 91 ELSE s_quantity END) - item_stock.quantity
			  FROM UNNEST(item_id_array, supply_wid_array, quantity_array, price_array)
				   AS item_stock (item_id, supply_wid, quantity, price)
			 WHERE stock.s_i_id = item_stock.item_id
			   AND stock.s_w_id = item_stock.supply_wid
			   AND stock.s_w_id = ANY(supply_wid_array)
			RETURNING stock.s_dist_04 as s_dist, stock.s_quantity, ( item_stock.quantity + item_stock.price * ( 1 + no_w_tax + no_d_tax ) * ( 1 - no_c_discount ) ) amount
    	)
		SELECT array_agg ( s_dist ), array_agg ( s_quantity ), array_agg ( amount )
		FROM stock_update
		INTO stock_dist_array, s_quantity_array, amount_array;
	ELSIF no_d_id = 5
	THEN
		WITH stock_update AS (
	        /*+ IndexScan(stock) */ UPDATE stock
    	       SET s_quantity = ( CASE WHEN s_quantity < (item_stock.quantity + 10) THEN s_quantity + 91 ELSE s_quantity END) - item_stock.quantity
			  FROM UNNEST(item_id_array, supply_wid_array, quantity_array, price_array)
				   AS item_stock (item_id, supply_wid, quantity, price)
			 WHERE stock.s_i_id = item_stock.item_id
			   AND stock.s_w_id = item_stock.supply_wid
			   AND stock.s_w_id = ANY(supply_wid_array)
			RETURNING stock.s_dist_05 as s_dist, stock.s_quantity, ( item_stock.quantity + item_stock.price * ( 1 + no_w_tax + no_d_tax ) * ( 1 - no_c_discount ) ) amount
    	)
		SELECT array_agg ( s_dist ), array_agg ( s_quantity ), array_agg ( amount )
		FROM stock_update
		INTO stock_dist_array, s_quantity_array, amount_array;
	ELSIF no_d_id = 6
	THEN
		WITH stock_update AS (
	        /*+ IndexScan(stock) */ UPDATE stock
    	       SET s_quantity = ( CASE WHEN s_quantity < (item_stock.quantity + 10) THEN s_quantity + 91 ELSE s_quantity END) - item_stock.quantity
			  FROM UNNEST(item_id_array, supply_wid_array, quantity_array, price_array)
				   AS item_stock (item_id, supply_wid, quantity, price)
			 WHERE stock.s_i_id = item_stock.item_id
			   AND stock.s_w_id = item_stock.supply_wid
			   AND stock.s_w_id = ANY(supply_wid_array)
			RETURNING stock.s_dist_06 as s_dist, stock.s_quantity, ( item_stock.quantity + item_stock.price * ( 1 + no_w_tax + no_d_tax ) * ( 1 - no_c_discount ) ) amount
    	)
		SELECT array_agg ( s_dist ), array_agg ( s_quantity ), array_agg ( amount )
		FROM stock_update
		INTO stock_dist_array, s_quantity_array, amount_array;
	ELSIF no_d_id = 7
	THEN
		WITH stock_update AS (
	        /*+ IndexScan(stock) */ UPDATE stock
    	       SET s_quantity = ( CASE WHEN s_quantity < (item_stock.quantity + 10) THEN s_quantity + 91 ELSE s_quantity END) - item_stock.quantity
			  FROM UNNEST(item_id_array, supply_wid_array, quantity_array, price_array)
				   AS item_stock (item_id, supply_wid, quantity, price)
			 WHERE stock.s_i_id = item_stock.item_id
			   AND stock.s_w_id = item_stock.supply_wid
			   AND stock.s_w_id = ANY(supply_wid_array)
			RETURNING stock.s_dist_07 as s_dist, stock.s_quantity, ( item_stock.quantity + item_stock.price * ( 1 + no_w_tax + no_d_tax ) * ( 1 - no_c_discount ) ) amount
    	)
		SELECT array_agg ( s_dist ), array_agg ( s_quantity ), array_agg ( amount )
		FROM stock_update
		INTO stock_dist_array, s_quantity_array, amount_array;
	ELSIF no_d_id = 8
	THEN
		WITH stock_update AS (
	        /*+ IndexScan(stock) */ UPDATE stock
    	       SET s_quantity = ( CASE WHEN s_quantity < (item_stock.quantity + 10) THEN s_quantity + 91 ELSE s_quantity END) - item_stock.quantity
			  FROM UNNEST(item_id_array, supply_wid_array, quantity_array, price_array)
				   AS item_stock (item_id, supply_wid, quantity, price)
			 WHERE stock.s_i_id = item_stock.item_id
			   AND stock.s_w_id = item_stock.supply_wid
			   AND stock.s_w_id = ANY(supply_wid_array)
			RETURNING stock.s_dist_08 as s_dist, stock.s_quantity, ( item_stock.quantity + item_stock.price * ( 1 + no_w_tax + no_d_tax ) * ( 1 - no_c_discount ) ) amount
    	)
		SELECT array_agg ( s_dist ), array_agg ( s_quantity ), array_agg ( amount )
		FROM stock_update
		INTO stock_dist_array, s_quantity_array, amount_array;
	ELSIF no_d_id = 9
	THEN
		WITH stock_update AS (
	        /*+ IndexScan(stock) */ UPDATE stock
    	       SET s_quantity = ( CASE WHEN s_quantity < (item_stock.quantity + 10) THEN s_quantity + 91 ELSE s_quantity END) - item_stock.quantity
			  FROM UNNEST(item_id_array, supply_wid_array, quantity_array, price_array)
				   AS item_stock (item_id, supply_wid, quantity, price)
			 WHERE stock.s_i_id = item_stock.item_id
			   AND stock.s_w_id = item_stock.supply_wid
			   AND stock.s_w_id = ANY(supply_wid_array)
			RETURNING stock.s_dist_09 as s_dist, stock.s_quantity, ( item_stock.quantity + item_stock.price * ( 1 + no_w_tax + no_d_tax ) * ( 1 - no_c_discount ) ) amount
    	)
		SELECT array_agg ( s_dist ), array_agg ( s_quantity ), array_agg ( amount )
		FROM stock_update
		INTO stock_dist_array, s_quantity_array, amount_array;
	ELSIF no_d_id = 10
	THEN
		WITH stock_update AS (
	        /*+ IndexScan(stock) */ UPDATE stock
    	       SET s_quantity = ( CASE WHEN s_quantity < (item_stock.quantity + 10) THEN s_quantity + 91 ELSE s_quantity END) - item_stock.quantity
			  FROM UNNEST(item_id_array, supply_wid_array, quantity_array, price_array)
				   AS item_stock (item_id, supply_wid, quantity, price)
			 WHERE stock.s_i_id = item_stock.item_id
			   AND stock.s_w_id = item_stock.supply_wid
			   AND stock.s_w_id = ANY(supply_wid_array)
			RETURNING stock.s_dist_10 as s_dist, stock.s_quantity, ( item_stock.quantity + item_stock.price * ( 1 + no_w_tax + no_d_tax ) * ( 1 - no_c_discount ) ) amount
    	)
		SELECT array_agg ( s_dist ), array_agg ( s_quantity ), array_agg ( amount )
		FROM stock_update
		INTO stock_dist_array, s_quantity_array, amount_array;
	END IF;

    IF (array_length(stock_dist_array, 1) > array_length(order_line_array, 1))
    THEN
        RAISE LOG 'value of stock_dist_array : %', stock_dist_array;
        RAISE LOG 'value of order_line_array : %', order_line_array;
        RAISE LOG 'value of item_id_array : %', item_id_array;
        RAISE LOG 'value of supply_wid_array : %', supply_wid_array;
        RAISE LOG 'value of quantity_array : %', quantity_array;
        RAISE LOG 'value of price_array : %', price_array;
    END IF;

	INSERT INTO order_line (ol_o_id, ol_d_id, ol_w_id, ol_number, ol_i_id, ol_supply_w_id, ol_quantity, ol_amount, ol_dist_info)
	SELECT no_d_next_o_id,
		   no_d_id,
		   no_w_id,
		   data.line_number,
		   data.item_id,
		   data.supply_wid,
		   data.quantity,
		   data.amount,
		   data.stock_dist
	  FROM UNNEST(order_line_array,
				   item_id_array,
				   supply_wid_array,
				   quantity_array,
				   amount_array,
				   stock_dist_array)
		   AS data( line_number, item_id, supply_wid, quantity, amount, stock_dist);

	no_s_quantity := 0;
	FOR loop_counter IN 1 .. no_o_ol_cnt
	LOOP
		no_s_quantity := no_s_quantity + CAST( amount_array[loop_counter] AS NUMERIC);
	END LOOP;

    EXCEPTION
        WHEN serialization_failure OR deadlock_detected OR no_data_found
            THEN ROLLBACK;
END;
$$

/*+
	HashJoin(order_line orders)
*/
EXPLAIN UPDATE order_line
SET ol_amount=0
FROM orders
WHERE ol_o_id=o_id
	AND ol_d_id = o_d_id
	AND ol_w_id = o_w_id
	AND o_id=593
	AND o_d_id=10
	AND o_w_id=1;

/*+
	IndexScan(order_line)
*/
EXPLAIN UPDATE order_line
SET ol_delivery_d = current_timestamp
FROM UNNEST(ARRAY[1, 2, 3, 4], ARRAY[1, 2, 3, 4]) AS ids(o_id, d_id)
WHERE ol_o_id = ids.o_id
	AND ol_d_id = ids.d_id
    AND ol_w_id = 1;