# ALTER TABLE ray.`2014q1-capitalbikeshare-tripdata` RENAME TO ray.`bikedata`;
SET SQL_SAFE_UPDATES = 0;

ALTER TABLE ray.bikedata ADD start_time INT FIRST;


ALTER TABLE ray.bikedata 
MODIFY COLUMN start_time DATE;


UPDATE ray.bikedata SET start_time = DATE_FORMAT(`Start date`, '%Y-%m-%d');


SELECT duration,`start_time` ,
       SUM(duration) OVER (ORDER BY `start_time`) AS running_total
  FROM bikedata;
  
SELECT `Start station number`,
		start_time,
       Duration,
       SUM(Duration) OVER
         (PARTITION BY `Start station number` ORDER BY start_time)
         AS running_total
  FROM bikedata;
 #WHERE start_time < '2012-01-08'
 
 
 
 SELECT start_time, 
		ROW_NUMBER() OVER (PARTITION BY `Start station number` ORDER BY start_time) as row_num,
        RANK() OVER (PARTITION BY `Start station number` ORDER BY start_time) as rank_num,
        DENSE_RANK() OVER (PARTITION BY `Start station number` ORDER BY start_time) as dense_rank_num
FROM bikedata;



SELECT Duration,
	`Start station number`,
		NTILE(4) OVER
         (PARTITION BY `Start station number` ORDER BY Duration)
          AS quartile,
       NTILE(5) OVER
         (PARTITION BY `Start station number` ORDER BY Duration)
         AS quintile,
       NTILE(100) OVER
         (PARTITION BY `Start station number` ORDER BY Duration)
         AS percentile
FROM bikedata
ORDER BY `Start station number`, Duration;


SELECT `Start station number`,
       Duration,
       LAG(Duration, 1) OVER
         (PARTITION BY `Start station number` ORDER BY Duration) AS lag_,
       LEAD(Duration, 1) OVER
         (PARTITION BY `Start station number` ORDER BY Duration) AS lead_
  FROM ray.bikedata
 ORDER BY `Start station number`, Duration;
 
 SELECT `Start station number`,
       Duration,
       Duration - LAG(Duration, 1) OVER
         (PARTITION BY `Start station number` ORDER BY Duration) AS diff_lag
 FROM ray.bikedata
 ORDER BY `Start station number`, Duration;
 
 #define a window alias
 
 SELECT `Start station number`,
       Duration,
       NTILE(4) OVER ntile_window AS quartile,
       NTILE(5) OVER ntile_window AS quintile,
       NTILE(100) OVER ntile_window AS percentile
  FROM ray.bikedata
 #WHERE start_time < '2012-01-08'
WINDOW ntile_window AS
         (PARTITION BY `Start station number` ORDER BY Duration)
 ORDER BY `Start station number`, Duration;
 