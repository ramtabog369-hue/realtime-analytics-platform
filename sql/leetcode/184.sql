-- LeetCode 184. Department Highest Salary
-- ешение для ClickHouse
SELECT d.Name AS Department, e.Name AS Employee, e.Salary
FROM leetcode.Employee e
JOIN leetcode.Department d ON e.DepartmentId = d.Id
WHERE e.Salary = (
    SELECT MAX(Salary)
    FROM leetcode.Employee
    WHERE DepartmentId = e.DepartmentId
)
ORDER BY d.Name;
