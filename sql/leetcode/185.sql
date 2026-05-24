-- LeetCode 185. Department Top Three Salaries
SELECT d.Name AS Department, e.Name AS Employee, e.Salary
FROM (
    SELECT *,
        DENSE_RANK() OVER (PARTITION BY DepartmentId ORDER BY Salary DESC) AS rnk
    FROM leetcode.Employee
) e
JOIN leetcode.Department d ON e.DepartmentId = d.Id
WHERE e.rnk <= 3
ORDER BY Department, Salary DESC;
