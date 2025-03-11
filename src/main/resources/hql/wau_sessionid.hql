SELECT
    WEEKOFYEAR(EVENT_TIME) AS WEEK,
    MIN(EVENT_TIME) AS START_OF_WEEK,
    COUNT(DISTINCT NEW_USER_SESSION) AS WAU
FROM USER_EVENT
GROUP BY WEEKOFYEAR(EVENT_TIME)
ORDER BY 1,2;
