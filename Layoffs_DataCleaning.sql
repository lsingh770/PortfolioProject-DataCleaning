-- SQL Project - Data Cleaning
-- https://github.com/AlexTheAnalyst/MySQL-YouTube-Series/blob/main/layoffs.csv


USE world_layoffs;

## Creating staging area for existing layoffs table
CREATE TABLE layoffs_staging
Like layoffs;

INSERT INTO layoffs_staging
SELECT * FROM layoffs;

SELECT COUNT(*) FROM layoffs_staging;

## Removing duplicate rows

with duplicate_cte as (
Select *, ROW_NUMBER() OVER(PARTITION BY company,location, industry, total_laid_off, percentage_laid_off,`date`, stage, country, funds_raised_millions) as row_num
from layoffs_staging)
SELECT * FROM duplicate_cte
WHERE row_num >1;


-- Creating another table with additional column row_num, so that it would be easy to remove duplicates 
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
  `row_num` int
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;


INSERT INTO layoffs_staging2
Select *, ROW_NUMBER() OVER(PARTITION BY company,location, industry, total_laid_off, percentage_laid_off,`date`, stage, country, funds_raised_millions) as row_num
from layoffs_staging;

DELETE FROM layoffs_staging2
WHERE row_num >1;

-- checking each individual column if there is any ambiguity in data
SELECT DISTINCT(company) FROM layoffs_staging2;
SELECT DISTINCT(location) FROM layoffs_staging2;
SELECT DISTINCT(industry) FROM layoffs_staging2; /* Here we can see we have 'Crypto', 'CryptoCurrency' and 'Crypto Currency' as values in industry column*/

/*Lets check if all have same meaning or belongs to same industry*/
SELECT * FROM layoffs_staging2 WHERE industry LIKE 'Crypto%'; 

/* So as per the data we can find that all are same and related rows can be updated with single value in industry column */
UPDATE layoffs_staging2
SET industry = 'Crypto' WHERE industry LIKE 'Crypto%';

SELECT DISTINCT(stage) FROM layoffs_staging2;
SELECT DISTINCT(country) FROM layoffs_staging2; /* Here values 'United States', 'United States.' both are same , let's update */
UPDATE layoffs_staging2
SET country = 'United States' WHERE country = 'United States.';

SELECT DISTINCT(date) FROM layoffs_staging2;

/* Updating the date from String to date format */
UPDATE layoffs_staging2
SET `date` = STR_TO_DATE(`date`, '%m/%d/%Y'); /* As we have given dates in '%m/%d/%Y' format */

/* Now we can convert the data type of date from text to date */
ALTER TABLE layoffs_staging2
MODIFY COLUMN `date` DATE;

## Check for null values
SELECT * FROM layoffs_staging2 
WHERE company IS NULL OR company = '' OR
location IS NULL OR location = '' OR 
industry IS NULL OR industry = '' OR
`date` IS NULL  OR
stage IS NULL OR stage = '' OR
country IS NULL OR country = '';

/* Here we can see industry column has blank values , lets check if these can be filled */
SELECT * FROM layoffs_staging2 WHERE company IN ('Airbnb', 'Carvana', 'Juul') AND location IN ('SF Bay Area', 'Phoenix');

/* We have found that blank values can be replaced with the existing values of industry having same company and location */
UPDATE layoffs_staging2 SET industry = NULL WHERE industry = '';

UPDATE layoffs_staging2 t1
JOIN layoffs_staging2 t2
ON t1.company = t2.company AND t1.location = t2.location
SET t1.industry = t2.industry
WHERE t1.industry IS NULL AND t2.industry IS NOT NULL;


SELECT * FROM layoffs_staging2 WHERE industry IN (NULL,''); /* 1 more industry value is null */

SELECT * FROM layoffs_staging2 WHERE company LIKE 'Bally%'; /* searched for similar records but found none , we can leave this as it is */

## Remove any rows or column if required
SELECT * FROM layoffs_staging2 
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

/* Since the record is about layoffs and we do not have any values is the rows for both the columns, lets delete these rows */

DELETE FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

SELECT * 
FROM layoffs_staging2;

/* Now lets drop column 'rownum' as it is of no significance at this stage */
ALTER TABLE layoffs_staging2
DROP COLUMN row_num;

SELECT * 
FROM layoffs_staging2;