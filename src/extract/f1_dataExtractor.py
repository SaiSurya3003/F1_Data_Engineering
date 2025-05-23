import fastf1
from fastf1.core import Laps
from fastf1 import plotting
from fastf1 import utils
import pandas as pd
import numpy as np
import os
from datetime import timedelta, datetime
import arrow
import re
import time

# --- Configuration ---
CACHE_DIR = os.path.expanduser('~/fastf1_cache')
if not os.path.exists(CACHE_DIR):
    try:
        os.makedirs(CACHE_DIR)
    except OSError as e:
        CACHE_DIR = None

if CACHE_DIR:
    try:
        fastf1.Cache.enable_cache(CACHE_DIR)
    except Exception:
        pass


def robust_string_or_td_to_seconds(val):
    if pd.isna(val):
        return np.nan
    if isinstance(val, timedelta):
        return val.total_seconds()
    if isinstance(val, (int, float)):
        return float(val)
    return np.nan


def format_seconds_to_mmssms(seconds_val):
    if pd.isna(seconds_val):
        return np.nan
    if not isinstance(seconds_val, (int, float)):
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
    except Exception:
        return np.nan


def format_seconds_to_hhmmss(seconds_val):
    if pd.isna(seconds_val):
        return np.nan
    if not isinstance(seconds_val, (int, float)):
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
    except Exception:
        return np.nan


def format_seconds_to_hhmmssms(seconds_val):
    if pd.isna(seconds_val):
        return np.nan
    if not isinstance(seconds_val, (int, float)):
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
    except Exception:
        return np.nan


def format_arrow_to_hhmmssms(arrow_obj):
    if arrow_obj is None or pd.isna(arrow_obj):
        return np.nan
    try:
        return arrow_obj.format("HH:mm:ss:SSS")
    except Exception:
        return np.nan


def format_arrow_to_hhmmss(arrow_obj):
    if arrow_obj is None or pd.isna(arrow_obj):
        return np.nan
    try:
        return arrow_obj.format("HH:mm:ss")
    except Exception:
        return np.nan


def get_session_data(year, event_specifier, session_name_key, output_base_dir):
    try:
        session = fastf1.get_session(year, event_specifier, session_name_key)
        session.load(laps=True, telemetry=True, weather=True, messages=True)
    except Exception as e:
        print(f"Error loading session data for {year} {event_specifier} {session_name_key}: {e}")
        return

    actual_session_name_for_path = session.name.replace(" ", "_").replace("/", "_") if session.name else session_name_key

    if hasattr(session.event, 'EventName') and session.event.EventName:
        event_name_safe = session.event.EventName.replace(" ", "_").replace("/", "_")
    else:
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
    except Exception:
        pass

    if local_session_start_time_arrow_obj is None:
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

    # 2. Session Results
    if session.results is not None and not session.results.empty:
        results_df = pd.DataFrame(session.results).copy()
        for col_name in ['Time', 'Q1', 'Q2', 'Q3', 'Interval']:
            if col_name in results_df.columns:
                seconds_value = results_df[col_name].apply(robust_string_or_td_to_seconds)
                if col_name == 'Time':
                    results_df[col_name] = seconds_value.apply(format_seconds_to_hhmmssms)
                elif col_name in ['Q1', 'Q2', 'Q3']:
                    results_df[col_name] = seconds_value.apply(format_seconds_to_mmssms)
                else:
                    results_df[col_name] = seconds_value
        results_df.to_csv(os.path.join(session_output_dir, 'session_results.csv'), index=False)
    else:
        print(f"No session results data for {year} {event_specifier} {session_name_key}")

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
                            except Exception:
                                abs_times.append(np.nan)
                    laps_df[col_name] = abs_times
                else:
                    laps_df[col_name] = np.nan

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
                            except Exception:
                                abs_times.append(np.nan)
                    laps_df[col_name] = abs_times
                else:
                    laps_df[col_name] = np.nan

        laps_df.to_csv(os.path.join(session_output_dir, 'laps_data.csv'), index=False)

        # 4. Telemetry Data Summary - REVISED LOGIC
        all_lap_telemetry_summary = []

        if not session.laps.empty:
            for idx, row in session.laps.iterrows():
                try:
                    original_lap_obj = session.laps.iloc[idx]

                    lap_telemetry = original_lap_obj.get_telemetry()

                    if lap_telemetry is None or lap_telemetry.empty:
                        continue

                    lap_telemetry = lap_telemetry.add_distance()

                    summary = {
                        'Driver': original_lap_obj.get('Driver', np.nan),
                        'Team': original_lap_obj.get('Team', np.nan),
                        'TeamName': original_lap_obj.get('TeamName', np.nan),
                        'LapNumber': original_lap_obj.get('LapNumber', np.nan),
                        'TelemetryLapStartTime_seconds': robust_string_or_td_to_seconds(lap_telemetry['Time'].iloc[0]) if 'Time' in lap_telemetry and not lap_telemetry['Time'].empty else np.nan,
                        'AvgSpeed': lap_telemetry['Speed'].mean() if 'Speed' in lap_telemetry else np.nan,
                        'MaxSpeed': lap_telemetry['Speed'].max() if 'Speed' in lap_telemetry else np.nan,
                        'MinSpeed': lap_telemetry['Speed'].min() if 'Speed' in lap_telemetry else np.nan,
                        'AvgRPM': lap_telemetry['RPM'].mean() if 'RPM' in lap_telemetry else np.nan,
                        'MaxRPM': lap_telemetry['RPM'].max() if 'RPM' in lap_telemetry else np.nan,
                        'AvgThrottle': lap_telemetry['Throttle'].mean() if 'Throttle' in lap_telemetry else np.nan,
                        'AvgBrake': lap_telemetry['Brake'].mean() if 'Brake' in lap_telemetry else np.nan,
                        'MaxDistance': lap_telemetry['Distance'].max() if 'Distance' in lap_telemetry else np.nan,
                        'DRSActive': (lap_telemetry['DRS'] >= 8).any() if 'DRS' in lap_telemetry else False
                    }

                    # Calculate Total Gear Changes
                    if 'Gear' in lap_telemetry and not lap_telemetry['Gear'].empty:
                        # Calculate the difference between consecutive gear values
                        # Use .astype(float) to handle potential NaN values correctly in diff
                        gear_changes = lap_telemetry['Gear'].astype(float).diff()
                        # Count where the gear actually changed (difference is not zero and not NaN)
                        summary['TotalGearChanges'] = gear_changes.fillna(0).abs().astype(bool).sum()
                    else:
                        summary['TotalGearChanges'] = np.nan

                    all_lap_telemetry_summary.append(summary)
                except Exception as e:
                    print(f"Error processing telemetry for Driver {row.get('Driver', 'N/A')} Lap {row.get('LapNumber', 'N/A')}: {e}")
                    pass

            if all_lap_telemetry_summary:
                telemetry_summary_df = pd.DataFrame(all_lap_telemetry_summary)
                if 'Driver' in telemetry_summary_df.columns:
                    # Adjust column order to include 'TotalGearChanges'
                    cols = ['Driver', 'Team', 'TeamName', 'LapNumber', 'TotalGearChanges'] + \
                           [col for col in telemetry_summary_df.columns if col not in ['Driver', 'Team', 'TeamName', 'LapNumber', 'TotalGearChanges']]
                    telemetry_summary_df = telemetry_summary_df[cols]
                telemetry_summary_df.to_csv(os.path.join(session_output_dir, 'lap_telemetry_summary.csv'),
                                            index=False)
            else:
                print(f"No lap telemetry summary data generated for {year} {event_specifier} {session_name_key}")
        else:
            print(f"No laps available to process telemetry for {year} {event_specifier} {session_name_key}")
    else:
        print(f"No lap data available for {year} {event_specifier} {session_name_key}")

    # 5. Weather Data
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
                        except Exception:
                            abs_times.append(np.nan)
                weather_df['Time'] = abs_times
            else:
                weather_df['Time'] = np.nan
        weather_df.to_csv(os.path.join(session_output_dir, 'weather_data.csv'), index=False)
    else:
        print(f"No weather data for {year} {event_specifier} {session_name_key}")

    # 6. Tyre Stints Summary
    if session.laps is not None and not session.laps.empty and 'Driver' in session.laps.columns and 'Stint' in session.laps.columns:
        stints_list = []
        for driver_abbreviation in session.laps['Driver'].unique():
            driver_laps_collection = session.laps.pick_drivers(driver_abbreviation)
            if driver_laps_collection.empty:
                continue

            for stint_num in driver_laps_collection['Stint'].unique():
                if pd.isna(stint_num): continue
                stint_laps_original = driver_laps_collection[driver_laps_collection['Stint'] == stint_num]
                if not stint_laps_original.empty:
                    stint_info = {
                        'Driver': driver_abbreviation,
                        'StintNumber': int(stint_num),
                        'Compound': stint_laps_original['Compound'].iloc[0] if 'Compound' in stint_laps_original and not stint_laps_original['Compound'].empty else None,
                        'StartLap': int(stint_laps_original['LapNumber'].min()) if 'LapNumber' in stint_laps_original and not stint_laps_original['LapNumber'].empty else None,
                        'EndLap': int(stint_laps_original['LapNumber'].max()) if 'LapNumber' in stint_laps_original and not stint_laps_original['LapNumber'].empty else None,
                        'NumLapsInStint': len(stint_laps_original)
                    }
                    stints_list.append(stint_info)
        if stints_list:
            stints_df = pd.DataFrame(stints_list)
            stints_df.to_csv(os.path.join(session_output_dir, 'tyre_stints_summary.csv'), index=False)
        else:
            print(f"No tyre stints summary generated for {year} {event_specifier} {session_name_key}")
    else:
        print(f"Cannot generate tyre stints summary for {year} {event_specifier} {session_name_key} (missing laps, Driver, or Stint data)")


def main(years_list, events_list=None, sessions_to_extract=None, output_dir='f1_data_output'):
    if sessions_to_extract is None:
        sessions_to_extract = ['R', 'Q']

    for year in years_list:
        print(f"\n--- Processing Year: {year} ---")
        current_year_events_specifiers = []
        if events_list is None:
            try:
                schedule = fastf1.get_event_schedule(year, include_testing=False)
                if not schedule.empty:
                    current_year_events_specifiers = schedule['RoundNumber'].tolist()
                else:
                    print(f"No event schedule found for {year}.")
                    continue
            except Exception as e:
                print(f"Error fetching event schedule for {year}: {e}")
                continue
        elif isinstance(events_list, dict):
            current_year_events_specifiers = events_list.get(year, [])
        else:
            current_year_events_specifiers = events_list

        if not current_year_events_specifiers:
            print(f"No events to process for year {year}.")
            continue

        for event_specifier in current_year_events_specifiers:
            print(f"  Processing Event: {event_specifier} (Year: {year})")
            if isinstance(event_specifier, (int, float)) and event_specifier <= 0:
                print(f"    Skipping invalid event specifier: {event_specifier}")
                continue
            try:
                event_info = fastf1.get_event(year, event_specifier)
                print(f"    Event Name: {event_info.EventName}")
            except Exception as e:
                print(f"    Could not retrieve event info for {year} {event_specifier}: {e}. Skipping event.")
                continue

            for session_key in sessions_to_extract:
                print(f"    Attempting to get data for Session: {session_key}")
                get_session_data(year, event_specifier, session_key, output_dir)
                time.sleep(5)


if __name__ == '__main__':
    # Set YEARS to a past year for testing, e.g., 2024, if you want to see telemetry files generated.
    # The 2025 season data is not yet available, hence the errors.
    YEARS = [2024] # Changed to 2024 for demonstration
    current_year = datetime.now().year
    # YEARS = list(range(2022, current_year + 1))

    EVENTS = None
    SESSIONS = ['Q', 'R']
    OUTPUT_DIRECTORY = 'f1_raw_data_output'
    os.makedirs(OUTPUT_DIRECTORY, exist_ok=True)

    main(years_list=YEARS, events_list=EVENTS, sessions_to_extract=SESSIONS, output_dir=OUTPUT_DIRECTORY)