-- test data (GroupIDs 1, 2 normal regressions, 3, 4 = no variance)
WITH some_table(GroupID, x, y) AS
(       SELECT 1,  1,  1    UNION SELECT 1,  2,  2    UNION SELECT 1,  3,  1.3
        UNION SELECT 1,  4,  3.75 UNION SELECT 1,  5,  2.25 UNION SELECT 2, 95, 85
        UNION SELECT 2, 85, 95    UNION SELECT 2, 80, 70    UNION SELECT 2, 70, 65
        UNION SELECT 2, 60, 70    UNION SELECT 3,  1,  2    UNION SELECT 3,  1, 3
        UNION SELECT 4,  1,  2    UNION SELECT 4,  2,  2),
  -- linear regression query
  /*WITH*/ mean_estimates AS
(   SELECT GroupID
      ,AVG(x)                                                  AS xmean
      ,AVG(y)                                                  AS ymean
    FROM some_table
    GROUP BY GroupID
),
    stdev_estimates AS
  (   SELECT pd.GroupID
        -- T-SQL STDEV() implementation is not numerically stable
        ,CASE      SUM(SQUARE(x - xmean)) WHEN 0 THEN 1
         ELSE SQRT(SUM(SQUARE(x - xmean)) / (COUNT(*) - 1)) END AS xstdev
        ,     SQRT(SUM(SQUARE(y - ymean)) / (COUNT(*) - 1))     AS ystdev
      FROM some_table pd
        INNER JOIN mean_estimates  pm ON pm.GroupID = pd.GroupID
      GROUP BY pd.GroupID, pm.xmean, pm.ymean
  ),
    standardized_data AS                   -- increases numerical stability
  (   SELECT pd.GroupID
        ,(x - xmean) / xstdev                                    AS xstd
        ,CASE ystdev WHEN 0 THEN 0 ELSE (y - ymean) / ystdev END AS ystd
      FROM some_table pd
        INNER JOIN stdev_estimates ps ON ps.GroupID = pd.GroupID
        INNER JOIN mean_estimates  pm ON pm.GroupID = pd.GroupID
  ),
    standardized_beta_estimates AS
  (   SELECT GroupID
        ,CASE WHEN SUM(xstd * xstd) = 0 THEN 0
         ELSE SUM(xstd * ystd) / (COUNT(*) - 1) END         AS betastd
      FROM standardized_data pd
      GROUP BY GroupID
  )
SELECT pb.GroupID
  ,ymean - xmean * betastd * ystdev / xstdev                   AS Alpha
  ,betastd * ystdev / xstdev                                   AS Beta
FROM standardized_beta_estimates pb
  INNER JOIN stdev_estimates ps ON ps.GroupID = pb.GroupID
  INNER JOIN mean_estimates  pm ON pm.GroupID = pb.GroupID