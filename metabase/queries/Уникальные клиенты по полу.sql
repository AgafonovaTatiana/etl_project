SELECT
  gender,
  COUNT(DISTINCT client_id) AS clients_count
FROM
  public.sales
WHERE
  {{date_time}} [[AND {{product_id}}]]
GROUP BY
  gender
ORDER BY
  gender;