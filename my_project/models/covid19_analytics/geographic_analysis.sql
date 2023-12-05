WITH geographic_analysis AS (
  SELECT
    date,
    location,
    province,
    country,
    continent,
    island,
    SUM(new_cases) OVER (PARTITION BY location ORDER BY date) AS total_cases,
    SUM(new_deaths) OVER (PARTITION BY location ORDER BY date) AS total_deaths,
    SUM(new_recovered) OVER (PARTITION BY location ORDER BY date) AS total_recoveries
  FROM
    covid19.public.covid_indonesia
)

SELECT
  date,
  location,
  province,
  country,
  continent,
  island,
  total_cases,
  total_deaths,
  total_recoveries
FROM
  geographic_analysis
ORDER BY
  date, location
