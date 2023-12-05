WITH demographic_analysis AS (
  SELECT
    date,
    location,
    population,
    population_density,
    SUM(new_cases) OVER (PARTITION BY location ORDER BY date) AS total_cases,
    SUM(new_deaths) OVER (PARTITION BY location ORDER BY date) AS total_deaths,
    SUM(new_recovered) OVER (PARTITION BY location ORDER BY date) AS total_recoveries
  FROM
    covid19.public.covid_indonesia
)

SELECT
  date,
  location,
  population,
  population_density,
  total_cases,
  total_deaths,
  total_recoveries,
  total_cases / population * 1000000 AS cases_per_million,
  total_deaths / population * 1000000 AS deaths_per_million,
  total_recoveries / population * 1000000 AS recoveries_per_million
FROM
  demographic_analysis
ORDER BY
  date, location
