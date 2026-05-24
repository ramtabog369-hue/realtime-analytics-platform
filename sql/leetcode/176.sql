-- LeetCode 176. Second Highest Salary
-- ешение для ClickHouse
SELECT 
    IFNULL(
        (SELECT DISTINCT Salary 
         FROM leetcode.Employee 
         ORDER BY Salary DESC 
         LIMIT 1 OFFSET 1),
        NULL
    ) AS SecondHighestSalary;
