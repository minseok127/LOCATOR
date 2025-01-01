-- Set zoned layout (details are in locator_external_catalog.h, locator_external_catalog.c)
SELECT locator_set_columnar_layout(to_regclass('order_line')::oid, '1|2|5|1|0|5|1|2|5|6|8|2|3|4|2|7|10|1|9');
SELECT locator_set_columnar_layout(to_regclass('orders')::oid, '1|2|5|1|5|3|1|2|4|1|3|3|0|6|8|1|7');
