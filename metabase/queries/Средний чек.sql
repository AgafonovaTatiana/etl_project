WITH
  order_like AS (
    SELECT
      client_id,
      date_time::date AS d,
      SUM(total_price) AS order_sum
    FROM
      public.sales
    WHERE
      {{date_time}} [[AND {{product_id}}]]
    GROUP BY
      client_id,
      d
  )
SELECT
  AVG(order_sum) AS avg_check
FROM
  order_like;