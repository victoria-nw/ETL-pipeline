# Customer Order Data Pipeline

## Business Problem
Daily processing of customer order data with comprehensive quality validation 
to support business analytics and reporting. Ensures data integrity through 
multi-layer validation before loading to data warehouse.

## What This Project Demonstrates

### Core Data Engineering Skills:
- **ETL Pipeline Design** - Structured extract, transform, load workflow
- **Data Quality Validation** - Multi-layer checks for integrity
- **Production Logging** - Comprehensive audit trails
- **Incremental Loading** - Efficient data updates
- **Schema Enforcement** - Controlled data structure
- **Error Handling** - Graceful failure management

### Technical Implementation:
- Modular function architecture for maintainability
- Separation of concerns (validate, transform, load)
- Idempotent operations (safe to re-run)
- Database transaction management
- Multiple export formats (DB, CSV, Parquet)

## Tech Stack

| Component | Technology |
|-----------|-----------|
| Language | Python 3.12 |
| Data Processing | Pandas |
| Database | SQLite3 |
| Logging | Python logging module |
| Data Formats | CSV, Parquet, SQLite |

## Pipeline Architecture
```
CSV Source → Extract → Validate → Transform → Load → SQLite + Backups
              ↓         ↓           ↓          ↓
           Logging   Quality    Business   Verification
                     Checks     Logic
```

## Project Structure
```
customer-orders-etl/
├── customer_orders_etl.py    # Main pipeline
├── dataset.csv               # Source data
├── processed_orders.db       # SQLite warehouse
├── processed_orders.csv      # CSV backup
├── processed_orders.parquet  # Parquet backup
├── test.log                  # Execution logs
└── README.md
```

## How to Run
```bash
# Install dependencies
pip install pandas

# Run pipeline
python customer_orders_etl.py
```

## Pipeline Steps

### 1. Extract
- Reads CSV source data
- Validates file existence
- Logs data shape and sample

### 2. Validate
Implements comprehensive data quality checks:
- **Missing values**: Identifies and removes incomplete records
- **Duplicates**: Detects duplicate order IDs
- **Business rules**: Validates quantity >= 0, price >= 0
- **Status values**: Ensures valid order status codes
- **Data types**: Confirms numeric fields are properly formatted

### 3. Transform
- Calculates derived metrics (total_order_value)
- Converts dates to proper datetime format
- Extracts time dimensions (order_month)
- Ensures numeric type consistency
- Filters business logic (removes invalid statuses)

### 4. Load
- Enforces target schema
- Supports incremental loading
- Writes to SQLite database
- Creates backup files (CSV + Parquet)
- Verifies successful load

## Data Quality Metrics

**Validation Results (from logs):**
- Records processed: 25
- Duplicates removed: 0
- Invalid values removed: 0
- Missing data handled: 0
- Final valid records: 23 (after filtering cancelled orders)

## Key Features

### Incremental Loading
Supports date-based incremental loads to process only new data:
```python
last_run = '2022-12-01'
df_incremental = incremental_loading(df_final, last_run)
```

### Schema Enforcement
Defines and enforces target schema before loading:
```python
target_schema = ['order_id', 'order_date', 'order_month', 
                 'customer_id', 'product', 'quantity', 
                 'price_usd', 'total_order_value', 'status']
```

### Multiple Export Formats
- **SQLite**: Primary data warehouse
- **Parquet**: Columnar format for analytics
- **CSV**: Human-readable backup

## Data Validation Logic
```python
# Example validation checks
- quantity < 0 → Removed
- price_usd < 0 → Removed
- status not in ['completed', 'pending', 'cancelled'] → Removed
- duplicate order_id → Removed (keeping first occurrence)
```

## Challenges Faced & Solutions

### Challenge 1: Inconsistent Status Values
**Problem**: Status field contained mixed case ('Completed', 'completed', 'COMPLETED')

**Solution**: Normalized to lowercase before validation
```python
invalid_status = ~df['status'].str.lower().isin(valid_status)
```

### Challenge 2: Type Coercion Errors
**Problem**: Numeric fields stored as strings caused calculation failures

**Solution**: Explicit type conversion with error handling
```python
df['quantity'] = pd.to_numeric(df['quantity'], errors='coerce')
```

### Challenge 3: Incremental Load Logic
**Problem**: Needed to support both full and incremental loads

**Solution**: Optional date parameter with fallback to full load
```python
if last_load_date:
    df_incremental = df[df['order_date'] > last_load_date]
else:
    return df  # Full load
```

## Future Enhancements

### Architecture Improvements
1. **Medallion Architecture** (Bronze → Silver → Gold)
   - Separate raw, cleaned, and business layers
   - Better data lineage
   - Support multiple consumers

2. **Orchestration** (Apache Airflow)
   - Scheduled pipeline runs
   - Dependency management
   - Retry mechanisms
   - Monitoring dashboards

3. **Data Quality Framework**
   - Great Expectations or Pandera
   - Automated quality reports
   - Quality score tracking

### Performance Optimization
4. **Chunked Processing**
   - Handle larger-than-memory datasets
   - Process data in batches

5. **Parallel Processing**
   - Multi-threaded validation
   - Concurrent file I/O

### Operational Improvements
6. **Unit Testing** (pytest)
   - Test each pipeline stage
   - Edge case coverage
   - CI/CD integration

7. **Configuration Management**
   - Environment-based config
   - Separate dev/prod settings
   - Secrets management

8. **Cloud Migration**
   - Azure blob for storage
   - Synapse for warehouse
   - Azure functions for serverless execution

## What I Learned

### Technical Skills:
- Pandas for data manipulation and validation
- SQLite for local data warehousing
- Python logging for production systems
- Schema design and enforcement
- Incremental loading patterns

### Best Practices:
- Modular function design for maintainability
- Separation of concerns (extract, validate, transform, load)
- Comprehensive error handling
- Production-grade logging
- Data quality as a first-class concern

### Data Engineering Concepts:
- ETL pipeline architecture
- Data validation strategies
- Schema evolution considerations
- Idempotent operations
- Incremental vs full loads

## Performance

**Execution Time:** ~2-3 seconds for 25 records
**Memory Usage:** Minimal (~50MB)
**Scalability:** Tested up to 10,000 records successfully

## Assumptions

- CSV file follows expected schema
- Order dates in YYYY-MM-DD format
- Single file processing (not streaming)
- Data volume fits in memory
- SQLite sufficient for current scale
