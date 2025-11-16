SELECT
  product_id,
  SUM(quantity) AS units_sold,
  SUM(total_price) AS revenue
FROM
  public.sales
WHERE
  {{date_time}} [[AND {{product_id}}]]
GROUP BY
  product_id
ORDER BY
  revenue DESC
LIMIT
  3;