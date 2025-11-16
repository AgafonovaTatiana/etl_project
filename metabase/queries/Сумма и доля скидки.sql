SELECT
  date_time::date AS d,
  SUM(discount_per_item) AS total_discount,
  SUM(total_price) AS revenue,
  SUM(discount_per_item) / NULLIF(SUM(discount_per_item + total_price), 0) AS discount_share
FROM
  public.sales
WHERE
  {{date_time}} [[AND {{product_id}}]]
GROUP BY
  d
ORDER BY
  d;