-- LeetCode 182. Duplicate Emails
SELECT Email
FROM leetcode.Person
GROUP BY Email
HAVING COUNT(*) > 1;
