-- Set hidden attributes of dimension tables
SELECT locator_hap_encode('public.region.r_name');
SELECT locator_hap_encode('public.nation.n_name');
SELECT locator_hap_encode('public.item.i_brand');

-- Set the propagation path for the hidden attributes
ALTER TABLE order_line SET (hidden_partition_key = '{
public.item.i_brand (item->stock->order_line),
public.nation.n_name (nation->supplier->stock->order_line),
public.nation.n_name (nation->customer->orders->order_line),
public.region.r_name (region->nation->supplier->stock->order_line),
public.region.r_name (region->nation->customer->orders->order_line)}',
locator_spread_factor=10);

ALTER TABLE orders SET (hidden_partition_key = '{
public.nation.n_name (nation->customer->orders),
public.region.r_name (region->nation->customer->orders)}',
locator_spread_factor=4);

ALTER TABLE stock SET (hidden_partition_key = '{
public.item.i_brand (item->stock),
public.nation.n_name (nation->supplier->stock),
public.region.r_name (region->nation->supplier->stock)}',
locator_spread_factor=12);

ALTER TABLE customer SET (hidden_partition_key = '{
public.nation.n_name (nation->customer),
public.region.r_name (region->nation->customer)}',
locator_spread_factor=8);
