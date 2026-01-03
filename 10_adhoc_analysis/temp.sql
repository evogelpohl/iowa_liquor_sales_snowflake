SELECT
    CONCAT(YEAR(sale_date), LPAD(DATE_PART('WEEKISO', sale_date)::VARCHAR, 2, '0')) AS sale_year_week,
    SUM(sale_dollars) AS total_sale_dollars
FROM iowa_liquor_sales
GROUP BY 1
ORDER BY 1;