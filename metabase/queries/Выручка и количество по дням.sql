SELECT
  date_time::date AS d,
  SUM(total_price) AS Выручка,
  SUM(quantity) AS Количество
FROM
  public.sales
WHERE
  {{date_time}} [[AND {{product_id}}]]
GROUP BY
  d
ORDER BY
  d;