import pandas as pd
import preprocessing as pp
import io
from maps import make_map, color_map, dtype_map
from maps import boolean_columns, search_columns, state_columns, other_columns, valid_codes
from sqlalchemy import create_engine

engine = create_engine(
    "postgresql+psycopg2://username:password@localhost:5432/your_db",
    echo=False
)


# --------------------- Helper Functions --------------------- #
def load_csv(file_path):
    try:
        df = pd.read_csv(file_path)
        print(f"CSV loaded successfully: {file_path} ({len(df)} rows)")
        return df
    except FileNotFoundError:
        print(f"Error: CSV file not found at {file_path}")
        exit(1) #Failed due to an error
    except Exception as e:
        print(f"Error loading CSV: {e}")
        exit(1)


def save_csv(df, file_path):
    try:
        df.to_csv(file_path, index=False)
        print(f"Preprocessed CSV saved: {file_path}")
    except Exception as e:
        print(f"Error saving CSV: {e}")


def log_step(step_name):
    print(f"{step_name}")

def enforce_dtypes(df):
    try:
        df = df.astype(dtype_map)
        print("Converted dtypes sucessfully")
        return df
    except Exception as e:
        print(f"Error Converting to the type: {e}")
        exit(1)

def load_to_database(df):
    # rename columns to match table
    df.columns = [c.lower().replace(" ", "_") for c in df.columns]

    try:
        buffer = io.StringIO()
        df.to_csv(buffer, index=False, header=True)
        buffer.seek(0)

        with engine.begin() as conn:
            raw_conn = conn.connection
            cursor = raw_conn.cursor()

            cols = ",".join(df.columns)

            copy_sql = f"""
            COPY traffic_violations ({cols})
            FROM STDIN
            WITH (FORMAT CSV, HEADER TRUE)
            """

            cursor.copy_expert(copy_sql, buffer)

        print("COPY append successful")

    except Exception as e:
        print(f"COPY append failed: {e}")
        raise

# --------------------- Main Preprocessing --------------------- #
def main():
    # Load data
    log_step("Loading data")
    data = load_csv("Traffic_Violations.csv")

    # Apply preprocessing steps with logging
    log_step("Merging duplicates")
    data = pp.merge_duplicates(data) # Merge Duplicates, SeqID, Description, Charge

    log_step("Cleaning Date Of Stop")
    data = pp.clean_date_of_stop(data) # Date of Stop

    log_step("Cleaning Time Of Stop and Timestamp")
    data = pp.clean_time_of_stop(data) # Time Of Stop

    log_step("Cleaning Agency and SubAgency")
    data = pp.clean_agency_subagency(data)

    log_step("Cleaning Latitude, Longitude, and Geolocation")
    data = pp.clean_lat_long_geo(data)

    log_step("Cleaning Boolean Columns")
    data = pp.clean_boolean_columns(data, boolean_columns)

    log_step("Cleaning Search Columns")
    data = pp.clean_search_columns(data, search_columns)

    log_step("Cleaning State Columns")
    data = pp.clean_state(data, valid_codes, state_columns)

    log_step("Cleaning Vehicle Columns")
    data = pp.clean_vehicle_columns(data)

    log_step("Cleaning Year")
    data = pp.clean_year(data, column='Year', min_val=1960, max_val=2025)

    log_step("Cleaning Make")
    data = pp.clean_make(data, make_map)

    log_step("Cleaning Color")
    data = pp.clean_color(data, color_map)

    log_step("Cleaning Driver City")
    data = pp.clean_driver_city(data)

    log_step("Cleaning Other Columns")
    data = pp.clean_other_columns(data, other_columns)

    log_step("Coverting the datatypes")
    data = enforce_dtypes(data)

    # Save preprocessed data
    
    #save_csv(data, "Preprocessed_traffic_violations_dataset.csv")

    # Tried to_sql , it was too slow for 1M rows, so using copy

    load_to_database(data)
    

# --------------------- Entry Point --------------------- #
if __name__ == "__main__":
    main()
