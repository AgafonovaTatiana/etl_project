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
  ),
  client_orders AS (
    SELECT
      client_id,
      COUNT(*) AS purchase_events
    FROM
      order_like
    GROUP BY
      client_id
  )
SELECT
  CASE
    WHEN purchase_events = 1 THEN '1 покупка'
    WHEN purchase_events = 2 THEN '2 покупки'
    ELSE '3+ покупок'
  END AS segment,
  COUNT(*) AS clients_count
FROM
  client_orders
GROUP BY
  segment
ORDER BY
  segment;