-- LeetCode 196. Delete Duplicate Emails
SELECT MIN(Id) AS Id, Email
FROM leetcode.Person
GROUP BY Email
ORDER BY Id;
