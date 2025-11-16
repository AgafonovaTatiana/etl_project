CREATE TABLE public.sales (
	client_id varchar NOT NULL,
	gender varchar NULL,
	product_id varchar NULL,
	quantity int4 NULL,
	price_per_item float8 NULL,
	discount_per_item float8 NULL,
	total_price float8 NULL,
	date_time timestamp NULL,
	CONSTRAINT sales_unique UNIQUE (client_id, gender, product_id, quantity, price_per_item, discount_per_item, total_price, date_time)
);