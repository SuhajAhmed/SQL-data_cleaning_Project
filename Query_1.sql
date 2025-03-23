-- Data Cleaning  --

SELECT 
    *
FROM
    layoffs;
-- now when we are data cleaning we usually follow a few steps
-- 1. check for duplicates and remove any
-- 2. standardize data and fix errors
-- 3. Look at null values and see what 
-- 4. remove any columns and rows that are not necessary - few ways
CREATE TABLE layoffs_staging LIKE layoffs;

INSERT layoffs_staging 
SELECT * FROM layoffs;

SELECT 
    *
FROM
    layoffs_staging;
    
SELECT *
FROM (
	SELECT company, industry, total_laid_off,`date`,
		ROW_NUMBER() OVER (
			PARTITION BY company, industry, total_laid_off,`date`
			) AS row_num
	FROM 
		layoffs_staging
) duplicates
WHERE 
	row_num > 1;
    
SELECT 
    *
FROM
    layoffs_staging
WHERE
    company = 'Oda'
;

SELECT *
FROM (
	SELECT company, location, industry, total_laid_off,percentage_laid_off,`date`, stage, country, funds_raised_millions,
		ROW_NUMBER() OVER (
			PARTITION BY company, location, industry, total_laid_off,percentage_laid_off,`date`, stage, country, funds_raised_millions
			) AS row_num
	FROM 
		layoffs_staging
) duplicates
WHERE 
	row_num > 1;

CREATE TABLE `layoffs_staging2` (
    `company` TEXT,
    `location` TEXT,
    `industry` TEXT,
    `total_laid_off` INT DEFAULT NULL,
    `percentage_laid_off` TEXT,
    `date` TEXT,
    `stage` TEXT,
    `country` TEXT,
    `funds_raised_millions` INT DEFAULT NULL,
    `row_num` INT
)  ENGINE=INNODB DEFAULT CHARSET=UTF8MB4 COLLATE = UTF8MB4_0900_AI_CI;
SELECT 
    *
FROM
    layoffs_staging2
WHERE
    row_num > 1;


insert into layoffs_staging2
	SELECT *,
		ROW_NUMBER() OVER (
			PARTITION BY company, location, industry, total_laid_off,percentage_laid_off,`date`, stage, country, funds_raised_millions
			) AS row_num
	FROM 
		layoffs_staging;

SET SQL_SAFE_UPDATES = 0;

DELETE FROM layoffs_staging2 
WHERE
    row_num > 1;
SELECT 
    *
FROM
    layoffs_staging2
WHERE
    row_num > 1;
    -- Standardizing Data 
SELECT 
    company, TRIM(company)
FROM
    layoffs_staging2;
UPDATE layoffs_staging2 
SET 
    company = TRIM(company);
    
SELECT 
    *
FROM
    layoffs_staging2
WHERE
    industry LIKE 'crypto%';
    
UPDATE layoffs_staging2 
SET 
    industry = 'crypto'
WHERE
    industry LIKE 'crypto%';
    
SELECT DISTINCT
    country, TRIM(TRAILING '.' FROM country)
FROM
    layoffs_staging2
ORDER BY 1;

UPDATE layoffs_staging2 
SET 
    country = TRIM(TRAILING '.' FROM country)
WHERE
    country LIKE 'United States%';
    
SELECT DISTINCT
    country
FROM
    layoffs_staging2
ORDER BY 1;

SELECT 
    'date', STR_TO_DATE(`date`, '%m/%d/%Y')
FROM
    layoffs_staging2;
    
UPDATE layoffs_staging2 
SET 
    `date` = STR_TO_DATE(`date`, '%m/%d/%Y');
    
SELECT 
    `date`
FROM
    layoffs_staging2;
    
ALTER TABLE layoffs_staging2
MODIFY COLUMN `date` DATE;

SELECT 
    *
FROM
    layoffs_staging2
WHERE
    total_laid_off IS NULL
        AND percentage_laid_off IS NULL;

SELECT 
    *
FROM
    layoffs_staging2
WHERE
    industry IS NULL OR industry = '';
    select * from layoffs_staging2 
    where company='Airbnb';
   
 SELECT t1.company, t1.location, t1.industry, t2.industry AS updated_industry
FROM layoffs_staging2 t1
JOIN layoffs_staging2 t2
ON t1.company = t2.company
AND t1.location = t2.location
WHERE t1.industry IS NULL OR t1.industry = ''
AND t2.industry IS NOT NULL AND t2.industry <> '';
UPDATE layoffs_staging2 t1
JOIN layoffs_staging2 t2
ON t1.company = t2.company
AND t1.location = t2.location
SET t1.industry = t2.industry
WHERE t1.industry IS NULL OR t1.industry = ''
AND t2.industry IS NOT NULL AND t2.industry <> '';
 SELECT * FROM layoffs_staging2 WHERE industry IS NULL;
  SELECT * FROM layoffs_staging2 WHERE company like 'Bally%';
    select * from layoffs_staging2
    where total_laid_off is null 
    and percentage_laid_off is  null;
     delete from layoffs_staging2
    where total_laid_off is null 
    and percentage_laid_off is  null;
    alter table layoffs_staging2
    drop column row_num;
      select * from layoffs_staging2;

