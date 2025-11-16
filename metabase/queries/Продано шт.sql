SELECT
  SUM(quantity) AS units_sold
FROM
  public.sales
WHERE
  {{date_time}} [[AND {{product_id}}]]