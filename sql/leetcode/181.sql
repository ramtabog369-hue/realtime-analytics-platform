-- LeetCode 181. Employees Earning More Than Their Managers
SELECT e1.Name AS Employee
FROM leetcode.Employee e1
JOIN leetcode.Employee e2 ON e1.ManagerId = e2.Id
WHERE e1.Salary > e2.Salary;
