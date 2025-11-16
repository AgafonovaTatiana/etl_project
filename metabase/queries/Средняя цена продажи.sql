SELECT
  date_time::date AS d,
  SUM(total_price) / NULLIF(SUM(quantity), 0) AS avg_sell_price
FROM
  public.sales
WHERE
  {{date_time}} [[AND {{product_id}}]]
GROUP BY
  d
ORDER BY
  d;