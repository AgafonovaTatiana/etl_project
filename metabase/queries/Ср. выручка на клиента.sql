SELECT
  SUM(total_price)::numeric / NULLIF(COUNT(DISTINCT client_id), 0) AS arpc
FROM
  public.sales
WHERE
  {{date_time}} [[AND {{product_id}}]];