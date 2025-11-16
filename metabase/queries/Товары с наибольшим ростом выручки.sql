WITH
  current_rows AS (
    SELECT
      product_id,
      date_time::date AS d,
      total_price
    FROM
      public.sales
    WHERE
      {{date_time}} [[AND {{product_id}}]]
  ),
  bounds AS (
    SELECT
      MIN(d) AS start_dt,
      MAX(d) AS end_dt
    FROM
      current_rows
  ),
  current_agg AS (
    SELECT
      product_id,
      SUM(total_price) AS rev_now
    FROM
      current_rows
    GROUP BY
      product_id
  ),
  prev_bounds AS (
    SELECT
      (
        start_dt - ((end_dt - start_dt + 1) * INTERVAL '1 day')
      )::date AS prev_start_dt,
      (start_dt - INTERVAL '1 day')::date AS prev_end_dt
    FROM
      bounds
  ),
  prev_period AS (
    SELECT
      s.product_id,
      SUM(s.total_price) AS rev_prev
    FROM
      public.sales s
      CROSS JOIN prev_bounds pb
    WHERE
      s.date_time::date BETWEEN pb.prev_start_dt AND pb.prev_end_dt  [[AND {{product_id}}]]
    GROUP BY
      s.product_id
  ),
  combined AS (
    SELECT
      current_agg.product_id,
      current_agg.rev_now,
      COALESCE(prev_period.rev_prev, 0) AS rev_prev,
      current_agg.rev_now - COALESCE(prev_period.rev_prev, 0) AS growth_revenue
    FROM
      current_agg
      LEFT JOIN prev_period USING (product_id)
  )
SELECT
  product_id,
  rev_now,
  rev_prev,
  growth_revenue
FROM
  combined
WHERE
  growth_revenue > 0
ORDER BY
  growth_revenue DESC
LIMIT
  10;