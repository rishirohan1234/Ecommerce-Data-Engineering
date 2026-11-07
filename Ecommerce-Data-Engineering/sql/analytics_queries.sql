
-- 1. Monthly Revenue
SELECT *
FROM monthly_sales
ORDER BY month;

-- 2. Top 10 Products
SELECT *
FROM product_sales
ORDER BY total_revenue DESC
LIMIT 10;

-- 3. Top 10 Cities
SELECT *
FROM city_sales
ORDER BY total_revenue DESC
LIMIT 10;

-- 4. High Value Customers
SELECT *
FROM customer_sales
WHERE total_spent > 1000
ORDER BY total_spent DESC;

-- 5. Payment Analysis
SELECT *
FROM payment_analysis
ORDER BY total_payment DESC;

-- 6. Delivery Performance
SELECT *
FROM delivery_analysis;
