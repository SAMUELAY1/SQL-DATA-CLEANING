-- DATA CLEANING WITH SQL
 use world_layoffs;

select *
from layoffs ;

-- 1. remove duplicates 
-- 2. standardize data 
-- null values 
-- remove any unnecessary columns 



-- creating a similar table to the original one 
CREATE TABLE layoffs_staging 
like layoffs ;

INSERT layoffs_staging 
select *
from layoffs ;

-- 1. remove duplicates FROM DATASET
WITH duplicate_cte AS
(
SELECT *,
row_number() over (
partition by company ,location ,industry ,total_laid_off , percentage_laid_off, 'date', stage,
country, funds_raised_millions) AS row_num
from layoffs_staging
)

select *
from duplicate_cte 
where row_num > 1 ;


CREATE TABLE `layoffs_staging2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `row_num` INT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

SELECT *
FROM layoffs_staging2
Where row_num > 1;

INSERT INTO layoffs_staging2
SELECT *,
row_number() over (
partition by company ,location ,industry ,total_laid_off , percentage_laid_off, 'date', stage,
country, funds_raised_millions) AS row_num
from layoffs_staging;

SET SQL_SAFE_UPDATES = 0; 

DELETE FROM layoffs_staging2 
WHERE row_num > 1;

select *
from layoffs_staging2

-- standardizing data 
-- CLEANING THE COMPANY COLUMN
select company, TRIM(company)
from layoffs_staging2;

UPDATE layoffs_staging2
SET company = TRIM(company);
-- CLEANING THE INDUSTRY COLUMN
select *
from layoffs_staging2
WHERE industry LIKE 'crypto%';

UPDATE layoffs_staging2
SET industry ='crypto'
WHERE industry LIKE 'crypto';

-- CLEANING THE COUNTRY COLUMN
SELECT DISTINCT country, 
       TRIM(TRAILING '.' FROM country) AS trimmed_country
FROM layoffs_staging2
ORDER BY country;

UPDATE layoffs_staging2
SET country = TRIM(TRAILING '.' FROM country)
WHERE country LIKE 'united states%'

-- chaging date column format 
select `date`,
STR_TO_DATE(`date`, '%m/%d/%Y')
FROM layoffs_staging2;



update  layoffs_staging2
SET `date` = STR_TO_DATE(`date`, '%m/%d/%Y')

ALTER TABLE layoffs_staging2
MODIFY COLUMN `date` DATE;


select *
from layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NUll;

-- populating  empty columns with values 
SELECT *
FROM layoffs_staging2 
WHERE industry IS NULL
OR  industry = '';


SELECT *
FROM layoffs_staging2 
WHERE company = 'Airbnb';

UPDATE layoffs_staging2
SET industry = 'travel'
WHERE company = 'Airbnb' AND (industry = '' OR industry IS NULL);

'''select t1.industry,t2.industry
from layoffs_staging2 t1
JOIN layoffs_staging2 t2
 ON t1.company= t2.company
WHERE (t1.industry IS NULL OR t1.industry = '')
AND t2.industry IS NOT NULL '''

-- working with null values
delete
from layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NUll;

-- dropping unnecessary columns 
ALTER TABLE layoffs_staging2
DROP COLUMN row_num;

-- VERIFY THE FINal table after cleaning
SELECT * 
FROM layoffs_staging2;
