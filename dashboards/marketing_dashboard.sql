-- Marketing dashboard datasets (Databricks AI/BI dashboard).
-- Every query reads Gold only. Create one dataset per block, then add the
-- suggested visualization. Run notebooks/run_pipeline.ipynb first.
-- If your catalog/schema differs, replace `workspace.marketing_gold`.


-- ============================================================
-- Q1 · Activation rate by segment            viz: Bar (X segment, Y activation_rate)
--      KPI counter: filter market_segment = 'BUILDING'
-- ============================================================
SELECT
  market_segment,
  COUNT(*)                                         AS total_customers,
  SUM(CASE WHEN is_activated THEN 1 ELSE 0 END)    AS activated_customers,
  AVG(CASE WHEN is_activated THEN 1.0 ELSE 0 END)  AS activation_rate
FROM workspace.marketing_gold.customer_marketing
GROUP BY market_segment
ORDER BY market_segment;


-- ============================================================
-- Q2 · New customers in 1996-Q1              viz: Counter (sum new_customers)
--                                                  + Bar (X month, Y new_customers)
-- ============================================================
SELECT month, new_customers
FROM workspace.marketing_gold.monitoring_new_customers_monthly
WHERE quarter = '1996-Q1'
ORDER BY month;


-- ============================================================
-- Q3 · Repeat purchase rate by segment       viz: Bar (X segment, Y repeat_purchase_rate)
-- ============================================================
SELECT market_segment, total_customers, repeat_customers, repeat_purchase_rate
FROM workspace.marketing_gold.repeat_purchase_rate_by_segment
ORDER BY repeat_purchase_rate DESC;


-- ============================================================
-- Q4 · 3-month retention of 1996 cohorts     viz: Bar (X cohort, Y retention_rate)
--                                                  + Table
-- ============================================================
SELECT cohort, cohort_customers, returned_customers, retention_rate
FROM workspace.marketing_gold.cohort_retention_1996
ORDER BY cohort;


-- ============================================================
-- Monitoring · Activation rate by segment over time
--      viz: Line (X month, Y activation_rate, color market_segment)
-- ============================================================
SELECT month, market_segment, activation_rate, change_pp, is_alert
FROM workspace.marketing_gold.monitoring_activation_rate_by_segment
ORDER BY month, market_segment;


-- ============================================================
-- Monitoring · New customers per month / quarter
--      viz: Line (X month, Y new_customers) — or group by quarter for a Bar
-- ============================================================
SELECT month, quarter, new_customers, trailing_avg, is_alert
FROM workspace.marketing_gold.monitoring_new_customers_monthly
ORDER BY month;


-- ============================================================
-- Alerts · Rows that broke a monitoring rule  viz: Table
--      Also usable as a Databricks SQL Alert: trigger when COUNT(*) > 0
--      for the latest month.
-- ============================================================
SELECT month, 'activation_rate_drop' AS alert, market_segment AS scope,
       CAST(ROUND(change_pp, 2) AS STRING) AS detail
FROM workspace.marketing_gold.monitoring_activation_rate_by_segment
WHERE is_alert
UNION ALL
SELECT month, 'new_customers_drop', 'ALL',
       CONCAT(CAST(new_customers AS STRING), ' vs avg ', CAST(ROUND(trailing_avg, 1) AS STRING))
FROM workspace.marketing_gold.monitoring_new_customers_monthly
WHERE is_alert
ORDER BY month DESC;
