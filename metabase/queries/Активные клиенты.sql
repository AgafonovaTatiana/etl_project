SELECT
  COUNT(DISTINCT client_id) AS active_clients
FROM
  public.sales
WHERE
  {{date_time}} [[AND {{product_id}}]];