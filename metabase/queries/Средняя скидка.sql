SELECT
  SUM(discount_per_item * quantity) / NULLIF(SUM(price_per_item * quantity), 0)
FROM
  public.sales
WHERE
  {{date_time}} [[AND {{product_id}}]];