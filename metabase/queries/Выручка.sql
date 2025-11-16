SELECT
  SUM(total_price) AS revenue
FROM
  public.sales
WHERE
  {{date_time}} [[AND {{product_id}}]];