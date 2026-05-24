-- LeetCode 183. Customers Who Never Order
SELECT c.Name AS Customers
FROM leetcode.Customers c
LEFT JOIN leetcode.Orders o ON c.Id = o.CustomerId
WHERE o.Id IS NULL;
