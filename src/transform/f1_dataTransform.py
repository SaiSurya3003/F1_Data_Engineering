import os
import pandas as pd
import numpy as np
import re
import logging

# Setup logging for the transformation script
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


def parse_custom_format_to_timedelta(time_val):
    """
    Parses custom time strings (HH:MM:SS:SSS, mm:ss:SSS, HH:MM:SS) into pandas.Timedelta.
    Handles potential negative signs.
    """
    if pd.isna(time_val) or time_val == '' or time_val is None:
        return pd.NaT

    time_str = str(time_val).strip()
    # Handle cases where NaN was converted to string 'nan' or 'NaT'
    if time_str.lower() in ['nan', 'nat']:
        return pd.NaT

    # Regex to capture sign and parts
    # Format 1: HH:MM:SS:SSS (e.g., 01:02:03:456 or -01:02:03:456)
    # From extract.py: format_seconds_to_hhmmssms
    match_hmsms = re.fullmatch(r"(-?)(\d{2}):(\d{2}):(\d{2}):(\d{3})", time_str)
    if match_hmsms:
        sign, h, m, s, ms = match_hmsms.groups()
        # pd.to_timedelta expects HH:MM:SS.SSS
        td_str = f"{sign}{h}:{m}:{s}.{ms}"
        try:
            return pd.to_timedelta(td_str)
        except ValueError:
            logging.warning(f"ValueError converting '{td_str}' from '{time_str}'. Returning NaT.")
            return pd.NaT

    # Format 2: mm:ss:SSS (e.g., 01:23:456 or -01:23:456)
    # From extract.py: format_seconds_to_mmssms
    match_msms = re.fullmatch(r"(-?)(\d{2}):(\d{2}):(\d{3})", time_str)
    if match_msms:
        sign, m_val, s_val, ms_val = match_msms.groups()
        # pd.to_timedelta needs hours for MM:SS.SSS, so pad with 00 hours
        td_str = f"{sign}00:{m_val}:{s_val}.{ms_val}"
        try:
            return pd.to_timedelta(td_str)
        except ValueError:
            logging.warning(f"ValueError converting '{td_str}' from '{time_str}'. Returning NaT.")
            return pd.NaT

    # Format 3: HH:MM:SS (e.g., 01:02:03 or -01:02:03)
    # From extract.py: format_seconds_to_hhmmss
    match_hms = re.fullmatch(r"(-?)(\d{2}):(\d{2}):(\d{2})", time_str)
    if match_hms:
        sign, h, m, s = match_hms.groups()
        td_str = f"{sign}{h}:{m}:{s}"
        try:
            return pd.to_timedelta(td_str)
        except ValueError:
            logging.warning(f"ValueError converting '{td_str}' from '{time_str}'. Returning NaT.")
            return pd.NaT

    logging.warning(f"Time string '{time_str}' did not match expected custom formats. Returning NaT.")
    return pd.NaT


# Define column mappings for transformation
# File name -> list of columns with custom string time formats
STRING_COLUMNS_TO_TIMEDELTA = {
    'session_results.csv': ['Time', 'Q1', 'Q2', 'Q3'],
    'laps_data.csv': [
        'LapTime', 'Sector1Time', 'Sector2Time', 'Sector3Time',
        'Time', 'PitInTime', 'PitOutTime',
        'Sector1SessionTime', 'Sector2SessionTime', 'Sector3SessionTime', 'LapStartTime'
    ],
    'weather_data.csv': ['Time']
}

# File name -> list of columns with ISO date/datetime strings
ISO_STRING_COLUMNS_TO_DATETIME = {
    'event_info.csv': ['EventDate', 'SessionStartDateLocalISO', 'SessionStartDateUTCISO']
}

# File name -> list of columns with numeric seconds to be converted to timedelta
NUMERIC_SECONDS_COLUMNS_TO_TIMEDELTA = {
    'session_results.csv': ['Interval'],
    'lap_telemetry_summary.csv': ['TelemetryLapStartTime_seconds']
}


def transform_csv_file(input_file_path, output_file_path):
    """
    Reads a CSV, transforms specified time-related columns, and saves the result.
    """
    try:
        # Read all data as string initially to prevent pandas from auto-converting
        # and potentially misinterpreting our custom formats.
        df = pd.read_csv(input_file_path, dtype=str, keep_default_na=True,
                         na_values=['', '#N/A', '#N/A N/A', '#NA', '-1.#IND', '-1.#QNAN', '-NaN', '-nan', '1.#IND',
                                    '1.#QNAN', '<NA>', 'N/A', 'NULL', 'NaN', 'None', 'nan', 'null'])

        file_name = os.path.basename(input_file_path)
        transformed_cols_count = 0

        # 1. Transform custom string format columns to Timedelta
        string_td_cols_transformed = 0
        if file_name in STRING_COLUMNS_TO_TIMEDELTA:
            cols_to_parse = STRING_COLUMNS_TO_TIMEDELTA[file_name]
            for col in cols_to_parse:
                if col in df.columns:
                    df[col] = df[col].apply(parse_custom_format_to_timedelta)
                    string_td_cols_transformed += 1
            if string_td_cols_transformed > 0:
                logging.debug(
                    f"Applied custom string to timedelta transformation to {string_td_cols_transformed} column(s) in {file_name}")
                transformed_cols_count += string_td_cols_transformed

        # 2. Transform ISO string columns to Datetime
        datetime_cols_transformed = 0
        if file_name in ISO_STRING_COLUMNS_TO_DATETIME:
            cols_to_datetime = ISO_STRING_COLUMNS_TO_DATETIME[file_name]
            for col in cols_to_datetime:
                if col in df.columns:
                    df[col] = pd.to_datetime(df[col], errors='coerce')  # Coerce turns parsing errors into NaT
                    datetime_cols_transformed += 1
            if datetime_cols_transformed > 0:
                logging.debug(
                    f"Applied ISO string to datetime transformation to {datetime_cols_transformed} column(s) in {file_name}")
                transformed_cols_count += datetime_cols_transformed

        # 3. Transform numeric (read as string, then to numeric) seconds columns to Timedelta
        numeric_td_cols_transformed = 0
        if file_name in NUMERIC_SECONDS_COLUMNS_TO_TIMEDELTA:
            cols_numeric_to_td = NUMERIC_SECONDS_COLUMNS_TO_TIMEDELTA[file_name]
            for col in cols_numeric_to_td:
                if col in df.columns:
                    # Convert column to numeric first, coercing errors to NaN
                    numeric_series = pd.to_numeric(df[col], errors='coerce')
                    df[col] = pd.to_timedelta(numeric_series, unit='s', errors='coerce')
                    numeric_td_cols_transformed += 1
            if numeric_td_cols_transformed > 0:
                logging.debug(
                    f"Applied numeric seconds to timedelta transformation to {numeric_td_cols_transformed} column(s) in {file_name}")
                transformed_cols_count += numeric_td_cols_transformed

        if transformed_cols_count > 0:
            os.makedirs(os.path.dirname(output_file_path), exist_ok=True)
            df.to_csv(output_file_path, index=False)
            logging.info(f"Saved transformed file ({transformed_cols_count} cols affected): {output_file_path}")
        else:
            logging.info(f"No transformations applied or specified for {input_file_path}. Skipping save.")

    except FileNotFoundError:
        logging.error(f"Input file not found: {input_file_path}")
    except pd.errors.EmptyDataError:
        logging.warning(f"Input file is empty: {input_file_path}. Skipping.")
    except Exception as e:
        logging.error(f"Error processing file {input_file_path}: {e}", exc_info=True)


def main_transform(input_base_dir, output_base_dir):
    """
    Main function to walk through input directories and transform CSV files.
    """
    logging.info(f"Starting transformation from '{input_base_dir}' to '{output_base_dir}'")
    if not os.path.exists(input_base_dir):
        logging.error(f"Input directory '{input_base_dir}' does not exist. Exiting.")
        return

    for root, _, files in os.walk(input_base_dir):
        for file in files:
            if file.endswith('.csv'):
                input_file_path = os.path.join(root, file)
                # Construct corresponding output path
                relative_path = os.path.relpath(input_file_path, input_base_dir)
                output_file_path = os.path.join(output_base_dir, relative_path)

                logging.info(f"Processing {input_file_path} -> {output_file_path}")
                transform_csv_file(input_file_path, output_file_path)

    logging.info("--- Data transformation process finished ---")


if __name__ == '__main__':
    # Define project structure components
    # Get the directory where the script itself is located
    SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))  # This will be G:\Projects\f1DataEngineering\src\transform

    # To get the project root, we go two levels up from SCRIPT_DIR
    PROJECT_ROOT = os.path.abspath(os.path.join(SCRIPT_DIR, '..', '..'))  # This should be G:\Projects\f1DataEngineering

    # Input directory where f1_data_extract.py saves its output
    # Path relative to PROJECT_ROOT: src/extract/f1_data_output_csvs
    INPUT_DIR_PATH_PARTS = ['src', 'extract', 'f1_data_output_csvs']
    INPUT_DIRECTORY = os.path.join(PROJECT_ROOT, *INPUT_DIR_PATH_PARTS)

    # Output directory for transformed files
    # Path relative to PROJECT_ROOT: src/transform/f1_data_transformed_time_objects
    OUTPUT_DIR_PARENT_PATH_PARTS = ['src', 'transform']
    OUTPUT_SUBDIR_NAME = 'f1_data_transformed_time_objects'

    OUTPUT_DIRECTORY_BASE = os.path.join(PROJECT_ROOT, *OUTPUT_DIR_PARENT_PATH_PARTS)
    OUTPUT_DIRECTORY = os.path.join(OUTPUT_DIRECTORY_BASE, OUTPUT_SUBDIR_NAME)

    # Ensure the base output directory 'src/transform/' exists
    os.makedirs(OUTPUT_DIRECTORY_BASE, exist_ok=True)
    # Ensure the final output directory exists
    os.makedirs(OUTPUT_DIRECTORY, exist_ok=True)

    # Print paths for debugging (optional, can be removed after confirming)
    logging.info(f"Script location: {os.path.abspath(__file__)}")
    logging.info(f"Determined Project Root: {PROJECT_ROOT}")
    logging.info(f"Determined Input Directory: {INPUT_DIRECTORY}")
    logging.info(f"Determined Output Directory: {OUTPUT_DIRECTORY}")

    main_transform(input_base_dir=INPUT_DIRECTORY, output_base_dir=OUTPUT_DIRECTORY)