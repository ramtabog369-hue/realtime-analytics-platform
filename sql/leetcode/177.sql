-- LeetCode 177. Nth Highest Salary
-- ример для N=3
SELECT DISTINCT Salary
FROM (
    SELECT Salary, DENSE_RANK() OVER (ORDER BY Salary DESC) AS rnk
    FROM leetcode.Employee
) ranked
WHERE rnk = 3;
