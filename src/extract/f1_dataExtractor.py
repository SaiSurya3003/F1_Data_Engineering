import fastf1
from fastf1.core import Laps
from fastf1 import plotting  # Not strictly used in data extraction but often imported
from fastf1 import utils  # Not strictly used in data extraction but often imported
import pandas as pd
import numpy as np
import os
import logging
from datetime import timedelta, datetime  # Added datetime for current year
import arrow  # For robust timezone handling and datetime arithmetic
import re  # For cleaning strings

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- Configuration ---
# Clear cache before enabling it to force re-download
try:
    fastf1.Cache.clear_cache()  # Add this line to clear cache
    logging.info("FastF1 cache cleared successfully.")
except Exception as e:
    logging.warning(f"Could not clear FastF1 cache: {e}")

# Enable cache
# For EC2, ensure the path is valid and writable by the user running the script
CACHE_DIR = os.path.expanduser('~/fastf1_cache')
if not os.path.exists(CACHE_DIR):
    try:
        os.makedirs(CACHE_DIR)
        logging.info(f"Created cache directory: {CACHE_DIR}")
    except OSError as e:
        logging.error(f"Error creating cache directory {CACHE_DIR}: {e}")
        CACHE_DIR = None  # Disables caching if directory creation fails

if CACHE_DIR:
    try:
        fastf1.Cache.enable_cache(CACHE_DIR)
        logging.info(f"FastF1 cache enabled at: {CACHE_DIR}")
    except Exception as e:
        logging.error(f"Error enabling FastF1 cache at {CACHE_DIR}: {e}")


def robust_string_or_td_to_seconds(val):
    """
    Robustly converts a value (timedelta, string, or numeric) to total seconds.
    """
    if pd.isna(val):
        return np.nan
    if isinstance(val, timedelta):
        return val.total_seconds()
    if isinstance(val, (int, float)):  # Assumed to be already in seconds
        return float(val)
    logging.warning(f"robust_string_or_td_to_seconds received unhandled type: {type(val)} for value {val}")
    return np.nan


def format_seconds_to_mmssms(seconds_val):
    """
    Formats a duration in seconds to a 'mm:ss:SSS' string.
    Example: 89.567 -> "01:29:567"
    """
    if pd.isna(seconds_val):
        return np.nan

    if not isinstance(seconds_val, (int, float)):
        logging.warning(f"format_seconds_to_mmssms received non-numeric value: {seconds_val}, returning as is.")
        return str(seconds_val)

    try:
        if seconds_val < 0:
            sign = "-"
            seconds_val = abs(seconds_val)
        else:
            sign = ""

        total_milliseconds = int(round(seconds_val * 1000))

        minutes = total_milliseconds // (60 * 1000)
        remainder_milliseconds_after_minutes = total_milliseconds % (60 * 1000)

        seconds_part = remainder_milliseconds_after_minutes // 1000
        milliseconds_part = remainder_milliseconds_after_minutes % 1000

        return f"{sign}{minutes:02d}:{seconds_part:02d}:{milliseconds_part:03d}"
    except Exception as e:
        logging.error(f"Error formatting seconds {seconds_val} to mm:ss:SSS: {e}")
        return np.nan


def format_seconds_to_hhmmss(seconds_val):
    """
    Formats a duration in seconds to an 'HH:mm:ss' string.
    Example: 3661 -> "01:01:01"
    """
    if pd.isna(seconds_val):
        return np.nan

    if not isinstance(seconds_val, (int, float)):
        logging.warning(f"format_seconds_to_hhmmss received non-numeric value: {seconds_val}, returning as is.")
        return str(seconds_val)

    try:
        if seconds_val < 0:
            sign = "-"
            seconds_val = abs(seconds_val)
        else:
            sign = ""

        total_seconds_int = int(round(seconds_val))

        hours = total_seconds_int // 3600
        remainder_seconds_after_hours = total_seconds_int % 3600
        minutes = remainder_seconds_after_hours // 60
        seconds_part = remainder_seconds_after_hours % 60

        return f"{sign}{hours:02d}:{minutes:02d}:{seconds_part:02d}"
    except Exception as e:
        logging.error(f"Error formatting seconds {seconds_val} to HH:mm:ss: {e}")
        return np.nan


def format_seconds_to_hhmmssms(seconds_val):
    """
    Formats a duration in seconds to an 'HH:MM:SS:MS' string.
    Example: 3690.123 -> "01:01:30:123"
    """
    if pd.isna(seconds_val):
        return np.nan
    if not isinstance(seconds_val, (int, float)):
        logging.warning(f"format_seconds_to_hhmmssms received non-numeric value: {seconds_val}, returning as is.")
        return str(seconds_val)
    try:
        if seconds_val < 0:
            sign = "-"
            seconds_val = abs(seconds_val)
        else:
            sign = ""

        total_milliseconds = int(round(seconds_val * 1000))

        hours = total_milliseconds // (3600 * 1000)
        remainder_after_hours = total_milliseconds % (3600 * 1000)

        minutes = remainder_after_hours // (60 * 1000)
        remainder_after_minutes = remainder_after_hours % (60 * 1000)

        seconds_part = remainder_after_minutes // 1000
        milliseconds_part = remainder_after_minutes % 1000

        return f"{sign}{hours:02d}:{minutes:02d}:{seconds_part:02d}:{milliseconds_part:03d}"
    except Exception as e:
        logging.error(f"Error formatting seconds {seconds_val} to HH:mm:ss:ms: {e}")
        return np.nan


def format_arrow_to_hhmmssms(arrow_obj):
    """Formats an Arrow datetime object to 'HH:MM:SS:MS' string."""
    if arrow_obj is None or pd.isna(arrow_obj):
        return np.nan
    try:
        return arrow_obj.format("HH:mm:ss:SSS")
    except Exception as e:
        logging.error(f"Error formatting Arrow object {arrow_obj} to HH:mm:ss:SSS: {e}")
        return np.nan


def format_arrow_to_hhmmss(arrow_obj):
    """Formats an Arrow datetime object to 'HH:MM:SS' string."""
    if arrow_obj is None or pd.isna(arrow_obj):
        return np.nan
    try:
        return arrow_obj.format("HH:mm:ss")
    except Exception as e:
        logging.error(f"Error formatting Arrow object {arrow_obj} to HH:mm:ss: {e}")
        return np.nan


def get_session_data(year, event_specifier, session_name_key, output_base_dir):
    """
    Fetches and saves data for a specific session.
    """
    try:
        logging.info(f"Loading session: Year={year}, Event={event_specifier}, SessionKey={session_name_key}")
        session = fastf1.get_session(year, event_specifier, session_name_key)
        session.load(laps=True, telemetry=True, weather=True, messages=True)
    except Exception as e:
        logging.error(f"Error loading session {year}, {event_specifier}, {session_name_key}: {e}")
        return

    actual_session_name_for_path = session.name.replace(" ", "_").replace("/",
                                                                          "_") if session.name else session_name_key

    if hasattr(session.event, 'EventName') and session.event.EventName:
        event_name_safe = session.event.EventName.replace(" ", "_").replace("/", "_")
    else:
        logging.warning(
            f"Could not retrieve EventName for directory path for {year}, {event_specifier}. Using specifier '{event_specifier}' as fallback.")
        event_name_safe = str(event_specifier).replace(" ", "_").replace("/", "_")

    session_output_dir = os.path.join(output_base_dir, str(year), event_name_safe, actual_session_name_for_path)
    os.makedirs(session_output_dir, exist_ok=True)

    # 1. Event Info
    local_session_start_time_arrow_obj = None
    local_session_start_time_iso = None
    utc_session_start_time_iso = None

    if session.date:
        utc_session_start_time_iso = session.date.isoformat()

    try:
        raw_date_from_event = session.event.get_session_date(session_name_key)
        if raw_date_from_event is not None:
            if isinstance(raw_date_from_event, arrow.Arrow):
                local_session_start_time_arrow_obj = raw_date_from_event
            elif isinstance(raw_date_from_event, (pd.Timestamp, datetime)):
                local_session_start_time_arrow_obj = arrow.get(raw_date_from_event)
            else:
                local_session_start_time_arrow_obj = arrow.get(str(raw_date_from_event))

            if local_session_start_time_arrow_obj:
                local_session_start_time_iso = local_session_start_time_arrow_obj.isoformat()
        else:
            logging.warning(
                f"session.event.get_session_date('{session_name_key}') returned None for {year} {event_name_safe}.")
    except Exception as e:
        logging.error(
            f"Error processing local session start time for '{session_name_key}' in {year} {event_name_safe}: {e}")

    if local_session_start_time_arrow_obj is None:
        logging.warning(
            f"Local session start Arrow object is None for {year} {event_name_safe} {actual_session_name_for_path}. Absolute time conversions will be NaN. ISO string will use UTC if available.")
        if utc_session_start_time_iso and local_session_start_time_iso is None:
            local_session_start_time_iso = utc_session_start_time_iso

    event_info = {
        'Year': session.event.year if hasattr(session.event, 'year') else year,
        'EventName': session.event.EventName if hasattr(session.event, 'EventName') else 'Unknown Event',
        'EventDate': session.event.EventDate.isoformat() if hasattr(session.event,
                                                                    'EventDate') and session.event.EventDate else None,
        'Country': session.event.Country if hasattr(session.event, 'Country') else None,
        'Location': session.event.Location if hasattr(session.event, 'Location') else None,
        'SessionKey': session_name_key,
        'SessionNameActual': session.name if session.name else None,
        'SessionStartDateLocalISO': local_session_start_time_iso,
        'SessionStartDateUTCISO': utc_session_start_time_iso
    }
    pd.DataFrame([event_info]).to_csv(os.path.join(session_output_dir, 'event_info.csv'), index=False)
    logging.info(
        f"Saved event info for {session.name if session.name else session_name_key} (Local Start ISO: {local_session_start_time_iso})")

    # 2. Session Results
    if session.results is not None and not session.results.empty:
        results_df = pd.DataFrame(session.results).copy()
        for col_name in ['Time', 'Q1', 'Q2', 'Q3', 'Interval']:
            if col_name in results_df.columns:
                seconds_value = results_df[col_name].apply(robust_string_or_td_to_seconds)
                if col_name == 'Time':
                    results_df[col_name] = seconds_value.apply(format_seconds_to_hhmmssms)
                    logging.debug(f"Results: Formatted column '{col_name}' to HH:MM:SS:MS string format.")
                elif col_name in ['Q1', 'Q2', 'Q3']:  # Format Q1, Q2, Q3 as mm:ss:SSS
                    results_df[col_name] = seconds_value.apply(format_seconds_to_mmssms)
                    logging.debug(f"Results: Formatted column '{col_name}' to mm:ss:SSS string format.")
                else:  # Interval remains as numeric seconds
                    results_df[col_name] = seconds_value
                    logging.debug(f"Results: Processed column '{col_name}' to numeric seconds.")

        results_df.to_csv(os.path.join(session_output_dir, 'session_results.csv'), index=False)
        logging.info(
            f"Saved session results for {session.name if session.name else session_name_key} ({len(results_df)} rows)")
    else:
        logging.warning(f"No results data for {session.name if session.name else session_name_key}")

    # 3. Lap Data
    if session.laps is not None and not session.laps.empty:
        laps_df = pd.DataFrame(session.laps).copy()

        duration_cols_mmssms = ['LapTime', 'Sector1Time', 'Sector2Time', 'Sector3Time']
        absolute_local_time_hhmmssms = ['Time', 'PitInTime', 'PitOutTime']
        absolute_local_time_hhmmss = ['Sector1SessionTime', 'Sector2SessionTime', 'Sector3SessionTime', 'LapStartTime']

        for col_name in duration_cols_mmssms:
            if col_name in laps_df.columns:
                seconds_series = laps_df[col_name].apply(robust_string_or_td_to_seconds)
                laps_df[col_name] = seconds_series.apply(format_seconds_to_mmssms)
                logging.debug(f"Laps: Formatted column '{col_name}' to mm:ss:SSS string format.")

        for col_name in absolute_local_time_hhmmssms:
            if col_name in laps_df.columns:
                seconds_offset_series = laps_df[col_name].apply(robust_string_or_td_to_seconds)
                if local_session_start_time_arrow_obj is not None:
                    abs_times = []
                    for offset in seconds_offset_series:
                        if pd.isna(offset):
                            abs_times.append(np.nan)
                        else:
                            try:
                                shifted_time = local_session_start_time_arrow_obj.shift(seconds=offset)
                                abs_times.append(format_arrow_to_hhmmssms(shifted_time))
                            except Exception as e_shift:
                                logging.error(
                                    f"Error shifting time for column {col_name} with offset {offset}: {e_shift}")
                                abs_times.append(np.nan)
                    laps_df[col_name] = abs_times
                else:
                    laps_df[col_name] = np.nan
                logging.debug(f"Laps: Converted column '{col_name}' to absolute local time HH:MM:SS:MS string format.")

        for col_name in absolute_local_time_hhmmss:
            if col_name in laps_df.columns:
                seconds_offset_series = laps_df[col_name].apply(robust_string_or_td_to_seconds)
                if local_session_start_time_arrow_obj is not None:
                    abs_times = []
                    for offset in seconds_offset_series:
                        if pd.isna(offset):
                            abs_times.append(np.nan)
                        else:
                            try:
                                shifted_time = local_session_start_time_arrow_obj.shift(seconds=offset)
                                abs_times.append(format_arrow_to_hhmmss(shifted_time))
                            except Exception as e_shift:
                                logging.error(
                                    f"Error shifting time for column {col_name} with offset {offset}: {e_shift}")
                                abs_times.append(np.nan)
                    laps_df[col_name] = abs_times
                else:
                    laps_df[col_name] = np.nan
                logging.debug(f"Laps: Converted column '{col_name}' to absolute local time HH:MM:SS string format.")

        laps_df.to_csv(os.path.join(session_output_dir, 'laps_data.csv'), index=False)
        logging.info(f"Saved lap data for {session.name if session.name else session_name_key} ({len(laps_df)} rows)")

        # 4. Telemetry Data Summary
        all_lap_telemetry_summary = []
        driver_id_col = 'DriverNumber' if 'DriverNumber' in laps_df.columns else (
            'Driver' if 'Driver' in laps_df.columns else None)

        if driver_id_col:
            required_cols_for_telemetry = [driver_id_col, 'LapNumber', 'LapTime']
            if not all(col in laps_df.columns for col in required_cols_for_telemetry):
                logging.warning(
                    f"Skipping telemetry summary for {session.name}: Essential columns missing in laps_df for telemetry iteration.")
            else:
                drivers = laps_df[driver_id_col].unique()
                for driver_val in drivers:
                    driver_laps_processed_df = laps_df[laps_df[driver_id_col] == driver_val]

                    for _, lap_processed_row in driver_laps_processed_df.iterrows():
                        if pd.isna(lap_processed_row['LapTime']):
                            continue

                        original_lap_obj = None
                        try:
                            lap_num_for_lookup = int(lap_processed_row['LapNumber']) if not pd.isna(
                                lap_processed_row['LapNumber']) else None
                            if lap_num_for_lookup is None:
                                logging.warning(f"Skipping telemetry for driver {driver_val} due to NaN LapNumber.")
                                continue

                            original_lap_series = \
                            session.laps.pick_driver(driver_val).pick_lap(lap_num_for_lookup).iloc[0]
                            original_lap_obj = Laps([original_lap_series]).iloc[0]
                        except Exception as e_lap_lookup:
                            logging.warning(
                                f"Could not find original lap object for driver {driver_val}, lap number {lap_processed_row.get('LapNumber', 'N/A')} to fetch telemetry: {e_lap_lookup}")
                            continue

                        if original_lap_obj is None:
                            continue

                        try:
                            lap_telemetry = original_lap_obj.get_telemetry().add_distance()
                            if not lap_telemetry.empty:
                                summary = {
                                    driver_id_col: driver_val,
                                    'LapNumber': lap_processed_row['LapNumber'],
                                    'TelemetryLapStartTime_seconds': timedelta_to_seconds(
                                        lap_telemetry['Time'].iloc[0]) if 'Time' in lap_telemetry and not lap_telemetry[
                                        'Time'].empty else np.nan,
                                    'AvgSpeed': lap_telemetry['Speed'].mean() if 'Speed' in lap_telemetry else np.nan,
                                    'MaxSpeed': lap_telemetry['Speed'].max() if 'Speed' in lap_telemetry else np.nan,
                                    'MinSpeed': lap_telemetry['Speed'].min() if 'Speed' in lap_telemetry else np.nan,
                                    'AvgRPM': lap_telemetry['RPM'].mean() if 'RPM' in lap_telemetry else np.nan,
                                    'MaxRPM': lap_telemetry['RPM'].max() if 'RPM' in lap_telemetry else np.nan,
                                    'AvgThrottle': lap_telemetry[
                                        'Throttle'].mean() if 'Throttle' in lap_telemetry else np.nan,
                                    'AvgBrake': lap_telemetry['Brake'].mean() if 'Brake' in lap_telemetry else np.nan,
                                    'MaxDistance': lap_telemetry[
                                        'Distance'].max() if 'Distance' in lap_telemetry else np.nan,
                                    'DRSActive': (lap_telemetry['DRS'] >= 8).any() if 'DRS' in lap_telemetry else False
                                }
                                all_lap_telemetry_summary.append(summary)
                        except Exception as e_telemetry:
                            logging.warning(
                                f"Could not get/process telemetry for driver {driver_val}, lap {lap_processed_row.get('LapNumber', 'N/A')}: {e_telemetry}")

                if all_lap_telemetry_summary:
                    telemetry_summary_df = pd.DataFrame(all_lap_telemetry_summary)
                    telemetry_summary_df.to_csv(os.path.join(session_output_dir, 'lap_telemetry_summary.csv'),
                                                index=False)
                    logging.info(
                        f"Saved lap telemetry summary for {session.name if session.name else session_name_key} ({len(telemetry_summary_df)} rows)")
                else:
                    logging.info(
                        f"No lap telemetry summary data generated for {session.name if session.name else session_name_key}")
        else:
            logging.warning(
                f"Cannot generate telemetry summary: No '{driver_id_col}' column in laps data for {session.name if session.name else session_name_key}")
    else:
        logging.warning(f"No lap data for {session.name if session.name else session_name_key}")

    # 5. Weather Data (Time column formatted as absolute local time HH:MM:SS:MS)
    if session.weather_data is not None and not session.weather_data.empty:
        weather_df = pd.DataFrame(session.weather_data).copy()
        if 'Time' in weather_df.columns:
            seconds_offset_series = weather_df['Time'].apply(robust_string_or_td_to_seconds)
            if local_session_start_time_arrow_obj is not None:
                abs_times = []
                for offset in seconds_offset_series:
                    if pd.isna(offset):
                        abs_times.append(np.nan)
                    else:
                        try:
                            shifted_time = local_session_start_time_arrow_obj.shift(seconds=offset)
                            abs_times.append(format_arrow_to_hhmmssms(shifted_time))
                        except Exception as e_shift:
                            logging.error(f"Error shifting time for weather data with offset {offset}: {e_shift}")
                            abs_times.append(np.nan)
                weather_df['Time'] = abs_times
            else:
                weather_df['Time'] = np.nan  # Cannot compute absolute time
            logging.debug(f"Weather: Converted column 'Time' to absolute local time HH:MM:SS:MS string format.")

        weather_df.to_csv(os.path.join(session_output_dir, 'weather_data.csv'), index=False)
        logging.info(
            f"Saved weather data for {session.name if session.name else session_name_key} ({len(weather_df)} rows)")
    else:
        logging.warning(f"No weather data for {session.name if session.name else session_name_key}")

    # 6. Tyre Stints Summary
    if session.laps is not None and not session.laps.empty and 'Driver' in session.laps.columns and 'Stint' in session.laps.columns:
        stints_list = []
        for driver_abbreviation in session.laps['Driver'].unique():
            driver_laps_original = session.laps.pick_driver(driver_abbreviation)
            for stint_num in driver_laps_original['Stint'].unique():
                if pd.isna(stint_num): continue
                stint_laps_original = driver_laps_original[driver_laps_original['Stint'] == stint_num]
                if not stint_laps_original.empty:
                    stint_info = {
                        'Driver': driver_abbreviation,
                        'StintNumber': stint_num,
                        'Compound': stint_laps_original['Compound'].iloc[
                            0] if 'Compound' in stint_laps_original and len(
                            stint_laps_original['Compound']) > 0 else None,
                        'StartLap': stint_laps_original[
                            'LapNumber'].min() if 'LapNumber' in stint_laps_original else None,
                        'EndLap': stint_laps_original[
                            'LapNumber'].max() if 'LapNumber' in stint_laps_original else None,
                        'NumLapsInStint': len(stint_laps_original)
                    }
                    stints_list.append(stint_info)
        if stints_list:
            stints_df = pd.DataFrame(stints_list)
            stints_df.to_csv(os.path.join(session_output_dir, 'tyre_stints_summary.csv'), index=False)
            logging.info(
                f"Saved tyre stints summary for {session.name if session.name else session_name_key} ({len(stints_df)} rows)")
        else:
            logging.info(f"No tyre stints summary generated for {session.name if session.name else session_name_key}")
    else:
        logging.warning(
            f"Cannot generate tyre stints summary: Missing 'Driver' or 'Stint' column in original session.laps for {session.name if session.name else session_name_key}")


def main(years_list, events_list=None, sessions_to_extract=None, output_dir='f1_data_output'):
    if sessions_to_extract is None:
        sessions_to_extract = ['R', 'Q']

    for year in years_list:
        logging.info(f"--- Processing Year: {year} ---")

        current_year_events_specifiers = []
        if events_list is None:
            try:
                schedule = fastf1.get_event_schedule(year, include_testing=False)
                if not schedule.empty:
                    current_year_events_specifiers = schedule['RoundNumber'].tolist()
                    logging.info(
                        f"Fetched schedule for {year}, {len(current_year_events_specifiers)} events found using RoundNumber.")
                else:
                    logging.warning(f"No events found in schedule for year {year}.")
                    continue
            except Exception as e:
                logging.error(f"Could not fetch schedule for year {year}: {e}")
                continue
        elif isinstance(events_list, dict):
            current_year_events_specifiers = events_list.get(year, [])
        else:
            current_year_events_specifiers = events_list

        if not current_year_events_specifiers:
            logging.warning(f"No events specified or found to process for year {year}. Skipping.")
            continue

        for event_specifier in current_year_events_specifiers:
            logging.info(f"--- Processing Event Specifier: {event_specifier} for Year: {year} ---")
            if isinstance(event_specifier, (int, float)) and event_specifier <= 0:
                logging.warning(f"Invalid event specifier '{event_specifier}' for year {year}. Skipping.")
                continue
            try:
                event_obj = fastf1.get_event(year, event_specifier)
            except Exception as e:
                logging.warning(
                    f"Could not access event details for {year}, {event_specifier} (might be in the future or data not available): {e}. Will attempt to load sessions anyway.")

            for session_key in sessions_to_extract:
                get_session_data(year, event_specifier, session_key, output_dir)

    logging.info("--- Data extraction process finished ---")


if __name__ == '__main__':
    current_year = datetime.now().year
    YEARS = [2025]
    # For 2022 to current year: YEARS = list(range(2022, current_year + 1))

    EVENTS = None
    # Example for specific events:
    # EVENTS = {2023: [1, 'Monza'], 2024: ['Silverstone']}

    SESSIONS = ['FP1', 'FP2', 'FP3', 'Q', 'R', 'Sprint', 'SS']
    OUTPUT_DIRECTORY = 'f1_data_output_csvs'
    os.makedirs(OUTPUT_DIRECTORY, exist_ok=True)

    logging.info(f"Starting F1 data extraction. Output will be in: {OUTPUT_DIRECTORY}")
    logging.info(
        f"Years: {YEARS}, Events: {'All per year (using RoundNumber)' if EVENTS is None else EVENTS}, Sessions: {SESSIONS}")

    main(years_list=YEARS, events_list=EVENTS, sessions_to_extract=SESSIONS, output_dir=OUTPUT_DIRECTORY)

    if fastf1.Cache.is_enabled and fastf1.Cache.cache_dir:
        logging.info(f"FastF1 Cache is active at: {fastf1.Cache.cache_dir}")
    else:
        logging.info("FastF1 Cache is not active or failed to initialize properly.")