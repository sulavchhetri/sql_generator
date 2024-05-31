# SQL Query Generator

This project processes a list of tables from a crawler report and generates SQL queries to count rows in each table. 

## Getting Started

### Prerequisites

- Python 3.x installed
- VS Code (Visual Studio Code) installed or any IDE

### Installation

1. **Clone this repository:**

    ```sh
    git clone https://github.com/sulavchhetri/sql-query-generator.git
    ```

2. **Navigate to the project directory:**

    ```sh
    cd sql-query-generator
    ```

3. **Open the project in VS Code:**

    ```sh
    code .
    ```

## Usage

1. **Prepare the input data:**

    - Open the `crawler.txt` file.
    - Paste the tables from the crawler report Excel file into `crawler.txt`.

2. **Run the script with the required arguments:**

    ```sh
    python create.py --source_schema="RPT_EDM" --target_schema="WC_DATA_MART" --unknown_schema_name="RPT_EDM" --sql_server=True
    ```

    ### Arguments

    - `--source_schema`: The source schema name (e.g., `"SOURCE_TABLE_OWNER"`).
    - `--target_schema`: The target schema name (e.g., `"TARGET_TABLE_OWNER"`).
    - `--unknown_schema_name`: The default schema name for tables with unknown schema (e.g., `"SOURCE_TABLE_OWNER"`).
    - `--sql_server`: Set to `True` for SQL Server format, `False` for Snowflake format (e.g., `True`).

3. **Get the output:**

    - Open `output.txt` to find the generated SQL queries.
    - Copy the desired output from `output.txt`.
