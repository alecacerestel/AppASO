# Refactoring Summary - AppASO ETL Pipeline

## Changes Completed

### 1. Configuration Updates
- **Control cell**: Changed from B1 to B3
- **RAW folder**: Added fixed folder ID (1HptFA1vpGiLZaLzZZZO5wI0P3EjKTDlL)
- **Agency start date**: Added constant for business logic (2025-07-15)
- **Removed**: LOGS_FOLDER and DATA_WAREHOUSE_FOLDER constants

### 2. New Module: Column Mapping
**File**: `src/etl/column_mapping.py`
- Handles differences between Apple and Google column names
- Provides standardized column mappings for all 3 data types
- Includes file pattern matching logic

### 3. Extract Module Refactored
**File**: `src/etl/extract.py`
- **Before**: Generated mock data
- **After**: 
  - Downloads real Excel files from Drive RAW folder
  - Handles 6 files (keywords, installs, users x 2 platforms)
  - Auto-detects file format (.xlsx, .csv)
  - Returns grouped data by type

### 4. Transform Module Enhanced
**File**: `src/etl/transform.py`
- **New functionality**:
  - Column standardization using ColumnMapper
  - Platform unification (Apple + Google)
  - French date parsing for Google users data
  - Apple users metadata row skipping
  - Business logic: Pre-Agencia / Con-Agencia stage classification
- **Three specialized transformation methods**:
  - `_transform_keywords()`
  - `_transform_installs()`
  - `_transform_users()`

### 5. Load Module Redesigned
**File**: `src/etl/load.py`
- **Before**: Single sheet update
- **After**:
  - Writes to 3 separate worksheets in MASTER_DATA_CLEAN
  - Auto-creates worksheets if they don't exist
  - Saves 3 separate CSV files to Data Lake (one per data type)
  - Proper datetime to string conversion for Sheets

### 6. Services Layer Updated
**File**: `src/services/drive.py`
- **Removed methods**:
  - `update_master_data()` - replaced by worksheet-specific updates
  - `upload_log_file()` - logs folder no longer used
- **Modified methods**:
  - `save_to_data_lake()` - now accepts data_type parameter

### 7. Error Handler Simplified
**File**: `src/utils/error_handler.py`
- Removed Drive service dependency
- No longer uploads logs to Drive
- Only sends email notifications

### 8. Pipeline Orchestration
**File**: `src/etl/pipeline.py`
- Updated to handle dictionary of DataFrames instead of single DataFrame
- Better progress reporting with step separators
- Updated instantiation (DataExtractor now needs drive_service)

### 9. Main Script
**File**: `etl_pipeline.py`
- Simplified error handler initialization (no drive_service needed)
- Updated control panel messaging (B3 instead of B1)

### 10. GitHub Actions
**File**: `.github/workflows/daily_etl.yml`
- Updated to 9:00 AM Paris time (8:00 AM UTC)
- Better workflow naming

### 11. Dependencies
**File**: `requirements.txt`
- Added: `openpyxl==3.1.2` for Excel file reading

## Key Business Logic Implemented

### Stage Classification
```python
if Date < 2025-07-15:
    Stage = "Pre-Agencia"
else:
    Stage = "Con-Agencia"
```

### Data Type Processing
1. **Keywords**: Rank distribution across 7 buckets
2. **Installs**: Daily installation counts
3. **Users**: Active users per day

### Platform Unification
- Apple + Google data merged into single datasets
- Platform identifier column added
- Standardized column names across platforms

## Testing Checklist

Before deploying to production:

- [ ] Verify RAW folder ID is correct and shared
- [ ] Confirm control panel cell is B3
- [ ] Test with actual Excel files in RAW folder
- [ ] Verify MASTER_DATA_CLEAN sheet exists and is shared
- [ ] Confirm Data Lake folder structure exists (year/month folders)
- [ ] Test email notifications
- [ ] Verify GitHub secrets are configured
- [ ] Run manual workflow trigger test

## Known Considerations

1. **French Dates**: Google users file uses French month names - parser implemented
2. **Apple Users Structure**: First 2 rows are metadata - skipped in processing
3. **Encoding**: Google installs file may have special characters - handled with error catching
4. **Date Formats**: All dates normalized to YYYY-MM-DD in output
5. **Historical Data**: Each data type saved separately in Data Lake

## File Structure Summary

```
Changes to existing files: 9
New files created: 2 (column_mapping.py, new README.md)
Total files modified: 11
```

## Next Steps for Production

1. Upload RAW files to Drive folder (1HptFA1vpGiLZaLzZZZO5wI0P3EjKTDlL)
2. Create MASTER_DATA_CLEAN sheet with 3 worksheets
3. Set control panel B3 to ON
4. Configure GitHub secrets
5. Test manual workflow run
6. Verify Data Lake folders exist
7. Monitor first scheduled run
