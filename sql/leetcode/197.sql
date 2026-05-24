-- LeetCode 197. Rising Temperature
SELECT RecordDate
FROM (
    SELECT RecordDate,
        Temperature - LAG(Temperature) OVER (ORDER BY RecordDate) AS diff
    FROM leetcode.Weather
) t
WHERE diff > 0
ORDER BY RecordDate;
