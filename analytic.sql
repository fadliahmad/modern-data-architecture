-- Sales Performance Metrics
WITH monthly_sales AS (
    SELECT 
        dd.year_actual,
        dd.month_actual,
        SUM(fo.total_due) AS total_revenue,
        COUNT(DISTINCT fo.sales_order_id) AS total_orders,
        SUM(fo.total_due) / NULLIF(COUNT(DISTINCT fo.sales_order_id), 0) AS avg_order_value
    FROM fact_order fo
    JOIN dim_date dd ON fo.order_date = dd.date_id
    GROUP BY dd.year_actual, dd.month_actual
),
growth_calc AS (
    SELECT *,
        LAG(total_revenue) OVER (ORDER BY year_actual, month_actual) AS prev_month_revenue,
        ((total_revenue - LAG(total_revenue) OVER (ORDER BY year_actual, month_actual)) / NULLIF(LAG(total_revenue) OVER (ORDER BY year_actual, month_actual), 0)) * 100 AS revenue_growth_pct
    FROM monthly_sales
)
SELECT * FROM growth_calc
ORDER BY year_actual DESC, month_actual DESC;


-- Customer 
SELECT 
    dc.customer_id,
    CONCAT(dc.first_name, ' ', COALESCE(dc.middle_name, ''), ' ', dc.last_name) AS customer_name,
    COUNT(DISTINCT fo.sales_order_id) AS total_orders,
    SUM(fo.total_due) AS total_spent,
    RANK() OVER (ORDER BY SUM(fo.total_due) DESC) AS customer_rank
FROM fact_order fo
JOIN dim_customer dc ON fo.customer_id = dc.customer_id
GROUP BY dc.customer_id, dc.first_name, dc.middle_name, dc.last_name
ORDER BY total_spent DESC
LIMIT 10;


-- Product Performance
SELECT 
    dp.product_id,
    dp.name AS product_name,
    SUM(fo.order_qty) AS total_units_sold,
    SUM(fo.total_due) AS total_revenue,
    RANK() OVER (ORDER BY SUM(fo.total_due) DESC) AS revenue_rank,
    RANK() OVER (ORDER BY SUM(fo.order_qty) DESC) AS sales_rank
FROM fact_order fo
JOIN dim_product dp ON fo.product_id = dp.product_id
GROUP BY dp.product_id, dp.name
ORDER BY total_revenue DESC
LIMIT 10;


-- Sales Performance
SELECT 
    dsp.sales_person_id,
    dsp.first_name AS sales_person_name,
    dst."name" AS territory_name,
    COUNT(DISTINCT fo.sales_order_id) AS total_orders,
    SUM(fo.total_due) AS total_sales,
    ROUND(AVG(fo.total_due), 2) AS avg_sales_per_order,
    RANK() OVER (ORDER BY SUM(fo.total_due) DESC) AS sales_rank
FROM fact_order fo
JOIN dim_sales_person dsp ON fo.sales_person_id = dsp.sales_person_id
LEFT JOIN dim_sales_territory dst ON dsp.sales_teritory_id = dst.sales_teritory_id
GROUP BY dsp.sales_person_id, dsp.first_name, dsp.middle_name, dsp.last_name, dst."name"
ORDER BY total_sales DESC
LIMIT 10;

