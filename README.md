# Brahim_OUHAMMOU_dt_eng_test

## Project Overview
This project demonstrates a complete data engineering pipeline using Python, MySQL, and Docker. The pipeline processes demographic data (people and places) and generates population statistics by country.

## Technical Stack
- **Database**: MySQL 8.0
- **Programming Language**: Python
- **Dependencies**: SQLAlchemy for database operations
- **Containerization**: Docker and Docker Compose
- **Data Formats**: CSV (input), JSON (output)

## Project Structure
```
.
├── docker-compose.yml          # Docker services configuration
├── example_schema.sql         # Database schema
├── data/                     # Data directory
│   ├── people.csv           # People demographic data
│   ├── places.csv          # Places reference data
│   └── sample_output.json  # Example output format
├── image-insert-csv-data/  # Data insertion service
│   ├── Dockerfile
│   ├── insert.py
│   └── requirements.txt
├── image-compute-data/    # Data computation service
│   ├── Dockerfile
│   ├── compute.py
│   └── requirements.txt
└── image-python/        # Example Python service (reference)
    ├── Dockerfile
    ├── example.py
    └── requirements.txt
```

## Implementation Details

### 1. Database Schema Design
The database schema consists of two main tables:
- `places`: Stores location data (city, county, country)
  - `id`: Primary key
  - `city`: City name
  - `county`: County name
  - `country`: Country name
  
- `people`: Stores demographic data
  - `id`: Primary key
  - `given_name`: First name
  - `family_name`: Last name
  - `date_of_birth`: Date of birth
  - `place_of_birth_id`: Foreign key referencing places(id)

### 2. Data Pipeline Services

#### 2.1 Data Insertion Service (image-insert-csv-data)
- Reads data from CSV files (`people.csv` and `places.csv`)
- Establishes connection to MySQL database
- Implements retry logic for database connection (10 attempts)
- Inserts place data first to maintain referential integrity
- Maps people to their birthplaces using SQL queries
- Handles data insertion using SQLAlchemy ORM

#### 2.2 Data Computation Service (image-compute-data)
- Connects to MySQL database with retry mechanism
- Performs aggregation query to count population by country
- Uses SQL JOIN operations to combine people and places data
- Outputs results in JSON format to `summary_output.json`
- Implements proper connection handling and cleanup

### 3. Docker Implementation
The project uses Docker Compose to orchestrate three services:
1. `database`: MySQL 8.0 instance
   - Configured with custom user and database
   - Initializes schema using mounted SQL file
   - Exposed on port 3306
   
2. `insert-data`: CSV data insertion service
   - Depends on database service
   - Mounts data volume for CSV access
   
3. `compute-data`: Population computation service
   - Executes after successful data insertion
   - Generates JSON output in data volume
   - Implements service dependency chain

## Error Handling
- Database connection retry mechanism
- Proper connection cleanup
- Service dependency management
- Volume mounting for data persistence
- Foreign key constraint enforcement

## How to Run
1. Ensure Docker and Docker Compose are installed
2. Clone the repository
3. Run: `docker-compose up -d`

The pipeline will:
1. Start MySQL database
2. Load schema
3. Insert CSV data
4. Compute population statistics
5. Generate JSON output

## Output Format
The final output (`summary_output.json`) contains population counts by country:
```json
{
    "Scotland": 8048,
    "Northern Ireland": 1952,
    ...
}
```
