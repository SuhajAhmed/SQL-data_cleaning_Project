-- Data Cleaning Process

-- Step 1: Checking the Original Data
SELECT * FROM layoffs;

-- Step 2: Creating a Staging Table for Data Cleaning
-- This is done to avoid modifying the original table directly
CREATE TABLE layoffs_staging LIKE layoffs;

-- Step 3: Copying Data from Original Table to Staging Table
INSERT layoffs_staging 
SELECT * FROM layoffs;

-- Step 4: Checking for Duplicates  
-- Using ROW_NUMBER() to identify duplicate records based on key attributes  
SELECT *
FROM (
	SELECT company, industry, total_laid_off, `date`,
		ROW_NUMBER() OVER (
			PARTITION BY company, industry, total_laid_off, `date`
		) AS row_num
	FROM layoffs_staging
) duplicates
WHERE row_num > 1;

-- Step 5: Checking for Specific Entries in the Data
SELECT * FROM layoffs_staging WHERE company = 'Oda';

-- Step 6: Checking for Duplicates with Additional Attributes
SELECT *
FROM (
	SELECT company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions,
		ROW_NUMBER() OVER (
			PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions
		) AS row_num
	FROM layoffs_staging
) duplicates
WHERE row_num > 1;

-- Step 7: Creating a New Staging Table with a Row Number Column  
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
) ENGINE=INNODB DEFAULT CHARSET=UTF8MB4 COLLATE = UTF8MB4_0900_AI_CI;

-- Step 8: Inserting Data into the New Staging Table with Row Numbers for Duplicates  
INSERT INTO layoffs_staging2
	SELECT *,
		ROW_NUMBER() OVER (
			PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions
		) AS row_num
	FROM layoffs_staging;

-- Step 9: Deleting Duplicate Records  
SET SQL_SAFE_UPDATES = 0; -- Disable safe updates mode to allow DELETE operation
DELETE FROM layoffs_staging2 
WHERE row_num > 1;

-- Step 10: Verifying that No Duplicate Records Remain  
SELECT * FROM layoffs_staging2 WHERE row_num > 1;

-- Standardizing Data 

-- Step 11: Trimming Spaces from Company Names
SELECT company, TRIM(company) FROM layoffs_staging2;
UPDATE layoffs_staging2 
SET company = TRIM(company);

-- Step 12: Standardizing Industry Names (Fixing Different Variations of 'crypto')
SELECT * FROM layoffs_staging2 WHERE industry LIKE 'crypto%';
UPDATE layoffs_staging2 
SET industry = 'crypto'
WHERE industry LIKE 'crypto%';

-- Step 13: Cleaning Country Names  
-- Removing unnecessary trailing dots from country names  
SELECT DISTINCT country, TRIM(TRAILING '.' FROM country)
FROM layoffs_staging2
ORDER BY 1;

UPDATE layoffs_staging2 
SET country = TRIM(TRAILING '.' FROM country)
WHERE country LIKE 'United States%';

-- Step 14: Checking Unique Country Names after Standardization  
SELECT DISTINCT country FROM layoffs_staging2 ORDER BY 1;

-- Step 15: Converting Date Format  
SELECT 'date', STR_TO_DATE(`date`, '%m/%d/%Y') FROM layoffs_staging2;
UPDATE layoffs_staging2 
SET `date` = STR_TO_DATE(`date`, '%m/%d/%Y');

-- Step 16: Changing the Date Column Data Type to Proper DATE Format  
ALTER TABLE layoffs_staging2
MODIFY COLUMN `date` DATE;

-- Step 17: Checking for Missing Data  
SELECT * FROM layoffs_staging2 WHERE total_laid_off IS NULL AND percentage_laid_off IS NULL;
SELECT * FROM layoffs_staging2 WHERE industry IS NULL OR industry = '';

-- Step 18: Filling in Missing Industry Values Using Data from the Same Company
SELECT 
    t1.company,
    t1.location,
    t1.industry,
    t2.industry AS updated_industry
FROM layoffs_staging2 t1
JOIN layoffs_staging2 t2 
    ON t1.company = t2.company
    AND t1.location = t2.location
WHERE (t1.industry IS NULL OR t1.industry = '')
    AND t2.industry IS NOT NULL
    AND t2.industry <> '';

-- Step 19: Updating Missing Industry Values Based on the Same Company's Existing Data  
UPDATE layoffs_staging2 t1
JOIN layoffs_staging2 t2 
    ON t1.company = t2.company
    AND t1.location = t2.location 
SET t1.industry = t2.industry
WHERE (t1.industry IS NULL OR t1.industry = '')
    AND t2.industry IS NOT NULL
    AND t2.industry <> '';

-- Step 20: Checking if Any Industry Values are Still Missing  
SELECT * FROM layoffs_staging2 WHERE industry IS NULL;

-- Step 21: Checking for a Specific Company  
SELECT * FROM layoffs_staging2 WHERE company LIKE 'Bally%';

-- Step 22: Removing Rows with Both 'total_laid_off' and 'percentage_laid_off' Missing  
DELETE FROM layoffs_staging2 
WHERE total_laid_off IS NULL AND percentage_laid_off IS NULL;

-- Step 23: Dropping the 'row_num' Column Since It's No Longer Needed  
ALTER TABLE layoffs_staging2 DROP COLUMN row_num;

-- Step 24: Final Check to Ensure Data is Cleaned Properly  
SELECT * FROM layoffs_staging2;
