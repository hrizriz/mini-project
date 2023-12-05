WITH daily_trends AS (
  SELECT
    date,
    SUM(new_cases) AS daily_new_cases,
    SUM(new_deaths) AS daily_new_deaths,
    SUM(new_recovered) AS daily_new_recoveries
  FROM
    covid19.public.covid_indonesia
  GROUP BY
    date
)

SELECT
  date,
  daily_new_cases,
  daily_new_deaths,
  daily_new_recoveries,
  LAG(daily_new_cases) OVER (ORDER BY date) AS previous_day_new_cases,
  LAG(daily_new_deaths) OVER (ORDER BY date) AS previous_day_new_deaths,
  LAG(daily_new_recoveries) OVER (ORDER BY date) AS previous_day_new_recoveries
FROM
  daily_trends
ORDER BY
  date
