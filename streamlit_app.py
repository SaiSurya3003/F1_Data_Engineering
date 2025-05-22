import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go  # For more custom plots if needed
import os
import numpy as np  # For handling potential NaN in numeric conversions

# --- Configuration ---
BASE_DATA_PATH = os.path.join('src', 'transform',
                              'f1_data_transformed_timedelta_no_dh')  # From your last transform script


# --- Helper Functions ---
@st.cache_data  # Cache data loading
def load_dataframe(file_path, file_identifier="Data"):
    """Loads a CSV file into a pandas DataFrame."""
    if os.path.exists(file_path):
        try:
            df = pd.read_csv(file_path)
            if df.empty:
                st.info(f"The {file_identifier} file ({os.path.basename(file_path)}) is empty or contains no data.")
                return pd.DataFrame()
            return df
        except pd.errors.EmptyDataError:
            st.info(f"The {file_identifier} file ({os.path.basename(file_path)}) is empty.")
            return pd.DataFrame()
        except Exception as e:
            st.error(f"Error loading {file_identifier} file ({os.path.basename(file_path)}): {e}")
            return pd.DataFrame()
    st.warning(f"{file_identifier} file not found: {file_path}")
    return pd.DataFrame()


def get_subdirectories(path):
    """Gets a sorted list of subdirectories in a given path."""
    if not os.path.exists(path) or not os.path.isdir(path):
        return []
    return sorted([d for d in os.listdir(path) if os.path.isdir(os.path.join(path, d))])


def safe_to_timedelta_seconds(series, column_name=""):
    """Safely converts a series of timedelta strings to total seconds."""
    if series is None:
        return pd.Series(dtype='float64')
    try:
        td_series = pd.to_timedelta(series, errors='coerce')
        return td_series.dt.total_seconds()
    except Exception as e:
        st.warning(f"Could not convert column '{column_name}' to timedelta seconds: {e}. Some values might be invalid.")
        # Attempt conversion for valid parts if possible, else return NaNs
        return pd.to_timedelta(series, errors='coerce').dt.total_seconds()


# --- Preprocessing Functions for Each Dataset ---
def preprocess_laps_data(df):
    if df.empty: return pd.DataFrame()
    df['LapTimeSeconds'] = safe_to_timedelta_seconds(df.get('LapTime'), 'LapTime')
    df['LapNumber'] = pd.to_numeric(df.get('LapNumber'), errors='coerce')
    df['Driver'] = df.get('Driver', pd.Series(dtype='object')).astype(str)  # Ensure Driver is string

    # Handle 'IsAccurate' robustly
    if 'IsAccurate' in df.columns:
        # Convert to string, lower, then map to boolean
        df['IsAccurate'] = df['IsAccurate'].astype(str).str.lower().map({
            'true': True, '1': True, '1.0': True,
            'false': False, '0': False, '0.0': False,
            'nan': False, 'none': False, '': False  # Handle string versions of missing
        }).fillna(False)  # Default to False if mapping fails or original was NaN

    df.dropna(subset=['LapNumber', 'LapTimeSeconds', 'Driver'], inplace=True)
    df = df[df['LapTimeSeconds'] > 0]
    return df


def preprocess_session_results(df):
    if df.empty: return pd.DataFrame()
    time_cols = ['Time', 'Q1', 'Q2', 'Q3', 'Interval']
    for col in time_cols:
        if col in df.columns:
            # 'Interval' might be numeric seconds, others are timedelta strings
            if col == 'Interval' and pd.api.types.is_numeric_dtype(df[col]):
                df[f'{col}Seconds'] = pd.to_numeric(df[col], errors='coerce')
            else:
                df[f'{col}Seconds'] = safe_to_timedelta_seconds(df[col], col)
    if 'Position' in df.columns:
        df['Position'] = pd.to_numeric(df.get('Position'), errors='coerce', downcast='integer')
    if 'Laps' in df.columns:
        df['Laps'] = pd.to_numeric(df.get('Laps'), errors='coerce', downcast='integer')
    return df


def preprocess_weather_data(df):
    if df.empty: return pd.DataFrame()
    # 'Time' in weather_data is a timedelta string from session start
    df['SessionTimeSeconds'] = safe_to_timedelta_seconds(df.get('Time'), 'Time')
    numeric_cols = ['AirTemp', 'TrackTemp', 'Humidity', 'Pressure', 'WindSpeed', 'Rainfall']
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')
    return df


def preprocess_telemetry_summary(df):
    if df.empty: return pd.DataFrame()
    # TelemetryLapStartTime_seconds is already numeric seconds from transform script
    if 'TelemetryLapStartTime_seconds' in df.columns:
        df['TelemetryLapStartTime_seconds'] = pd.to_numeric(df['TelemetryLapStartTime_seconds'], errors='coerce')

    numeric_cols = ['AvgSpeed', 'MaxSpeed', 'MinSpeed', 'AvgRPM', 'MaxRPM', 'AvgThrottle', 'AvgBrake', 'MaxDistance']
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')
    if 'LapNumber' in df.columns:
        df['LapNumber'] = pd.to_numeric(df.get('LapNumber'), errors='coerce', downcast='integer')
    return df


def preprocess_tyre_stints(df):
    if df.empty: return pd.DataFrame()
    numeric_cols = ['StintNumber', 'StartLap', 'EndLap', 'NumLapsInStint']
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce', downcast='integer')
    return df


# --- Visualization Functions ---
def display_event_info(df):
    st.subheader("Event Information")
    if df.empty: return
    # Display as a more readable format, transposing it
    st.table(df.iloc[0].T.rename("Value"))


def display_session_results(df):
    st.subheader("Session Results")
    if df.empty: return

    df_processed = preprocess_session_results(df.copy())
    if df_processed.empty:
        st.info("No processable session results data.")
        return

    st.dataframe(df_processed, hide_index=True)

    # Example: Bar chart of Race Times if 'TimeSeconds' and 'Driver' exist
    if 'TimeSeconds' in df_processed.columns and 'Driver' in df_processed.columns and not df_processed[
        'TimeSeconds'].isnull().all():
        st.markdown("---")
        st.write("#### Final Times (if applicable)")
        # Filter out drivers with no time, sort by position if available or time
        plot_df = df_processed.dropna(subset=['TimeSeconds', 'Driver'])
        if 'Position' in plot_df.columns:
            plot_df = plot_df.sort_values(by='Position')
        else:
            plot_df = plot_df.sort_values(by='TimeSeconds')

        fig = px.bar(plot_df, x='Driver', y='TimeSeconds',
                     labels={'TimeSeconds': 'Total Time (seconds)', 'Driver': 'Driver'},
                     title="Session Times by Driver")
        st.plotly_chart(fig, use_container_width=True)


def display_lap_times(df):
    st.subheader("Lap Time Progression by Driver")
    if df.empty: return

    laps_df_processed = preprocess_laps_data(df.copy())
    if laps_df_processed.empty:
        st.info("No processable lap data.")
        return

    plot_df = laps_df_processed
    if 'IsAccurate' in laps_df_processed.columns:
        filter_accurate_laps = st.checkbox("Show only 'accurate' laps", value=True, key="lap_time_accurate_filter")
        if filter_accurate_laps:
            plot_df = laps_df_processed[laps_df_processed['IsAccurate'] == True]

    if plot_df.empty:
        st.info("No data to display after filtering.")
        return

    all_drivers = sorted(plot_df['Driver'].unique())
    if not all_drivers:
        st.warning("No drivers found in the processed lap data.")
        return

    default_drivers = all_drivers[:min(len(all_drivers), 5)]  # Default to first 5 or all
    selected_drivers = st.multiselect(
        "Select drivers to display:", all_drivers, default=default_drivers, key="lap_time_driver_select"
    )

    if selected_drivers:
        plot_df_filtered = plot_df[plot_df['Driver'].isin(selected_drivers)].sort_values(by=['Driver', 'LapNumber'])
        if not plot_df_filtered.empty:
            fig = px.line(
                plot_df_filtered, x='LapNumber', y='LapTimeSeconds', color='Driver',
                labels={'LapNumber': 'Lap Number', 'LapTimeSeconds': 'Lap Time (seconds)'},
                markers=True,
                hover_data={
                    'LapTime': True,  # Show original timedelta string from CSV
                    'Compound': True,
                    'TyreLife': True,
                    'Stint': True
                }
            )
            fig.update_layout(legend_title_text='Driver')
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No data for selected drivers after filtering.")
    else:
        st.info("Please select at least one driver.")


def display_weather_data(df):
    st.subheader("Weather Conditions Over Session")
    if df.empty: return

    weather_df_processed = preprocess_weather_data(df.copy())
    if weather_df_processed.empty or 'SessionTimeSeconds' not in weather_df_processed.columns or weather_df_processed[
        'SessionTimeSeconds'].isnull().all():
        st.info("No processable weather data or session time.")
        return

    weather_vars = {
        'AirTemp': 'Air Temperature (°C)',
        'TrackTemp': 'Track Temperature (°C)',
        'Humidity': 'Humidity (%)',
        'Pressure': 'Air Pressure (hPa)',
        'WindSpeed': 'Wind Speed (km/h)',
        'Rainfall': 'Rainfall (mm or True/False)'  # Rainfall might be boolean or numeric
    }

    for var, label in weather_vars.items():
        if var in weather_df_processed.columns and not weather_df_processed[var].isnull().all():
            st.markdown(f"#### {label}")
            if var == 'Rainfall' and weather_df_processed[var].dtype == 'bool':  # Special handling for boolean rainfall
                fig = px.scatter(weather_df_processed, x='SessionTimeSeconds', y=var,
                                 labels={'SessionTimeSeconds': 'Session Time (seconds)', var: label},
                                 title=f"{label} over Session")
                fig.update_yaxes(tickvals=[0, 1], ticktext=['No Rain', 'Rain'])
            else:
                fig = px.line(weather_df_processed, x='SessionTimeSeconds', y=var,
                              labels={'SessionTimeSeconds': 'Session Time (seconds)', var: label},
                              title=f"{label} over Session", markers=True)
            st.plotly_chart(fig, use_container_width=True)


def display_tyre_stints(df):
    st.subheader("Tyre Stint Summary")
    if df.empty: return

    stints_df_processed = preprocess_tyre_stints(df.copy())
    if stints_df_processed.empty:
        st.info("No processable tyre stint data.")
        return

    st.dataframe(stints_df_processed, hide_index=True)

    if 'Driver' in stints_df_processed.columns and \
            'NumLapsInStint' in stints_df_processed.columns and \
            'Compound' in stints_df_processed.columns:
        st.markdown("---")
        st.write("#### Laps per Stint by Driver and Compound")

        # Create a unique identifier for each stint for plotting if needed
        stints_df_processed['StintIdentifier'] = stints_df_processed['Driver'] + " - Stint " + stints_df_processed[
            'StintNumber'].astype(str)

        fig = px.bar(stints_df_processed, x='Driver', y='NumLapsInStint', color='Compound',
                     barmode='stack',  # or 'group'
                     labels={'NumLapsInStint': 'Number of Laps in Stint'},
                     title="Laps per Stint by Driver (Colored by Compound)",
                     hover_data=['StintNumber', 'StartLap', 'EndLap'])
        st.plotly_chart(fig, use_container_width=True)


def display_telemetry_summary(df):
    st.subheader("Lap Telemetry Summary (Averages/Max per Lap)")
    if df.empty: return

    telemetry_df_processed = preprocess_telemetry_summary(df.copy())
    if telemetry_df_processed.empty:
        st.info("No processable telemetry summary data.")
        return

    st.dataframe(telemetry_df_processed.head(20), hide_index=True)  # Show a sample
    if st.checkbox("Show full telemetry summary table?", key="show_full_telemetry_table"):
        st.dataframe(telemetry_df_processed, hide_index=True)

    # Example: AvgSpeed vs. LapNumber for selected driver
    if 'Driver' in telemetry_df_processed.columns and \
            'LapNumber' in telemetry_df_processed.columns and \
            'AvgSpeed' in telemetry_df_processed.columns:

        st.markdown("---")
        st.write("#### Average Speed per Lap")
        drivers_telemetry = sorted(telemetry_df_processed['Driver'].unique())
        if drivers_telemetry:
            selected_driver_telemetry = st.selectbox("Select Driver for Telemetry Detail:", drivers_telemetry,
                                                     key="telemetry_driver_select")
            driver_telemetry_df = telemetry_df_processed[
                telemetry_df_processed['Driver'] == selected_driver_telemetry].sort_values(by='LapNumber')

            if not driver_telemetry_df.empty:
                fig = px.line(driver_telemetry_df, x='LapNumber', y='AvgSpeed',
                              title=f"Average Speed per Lap for {selected_driver_telemetry}",
                              labels={'AvgSpeed': 'Average Speed (km/h)'}, markers=True)
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.info(f"No telemetry data for {selected_driver_telemetry}.")


# --- Streamlit App UI and Logic ---
st.set_page_config(layout="wide", page_title="F1 Data Dashboard")
st.title("🏎️ Formula 1 Data Dashboard")

# --- Sidebar for Selections ---
st.sidebar.header("Session Selector")

years = get_subdirectories(BASE_DATA_PATH)
if not years:
    st.error(f"No data found in the base data path: '{BASE_DATA_PATH}'. "
             "Ensure scripts have run and populated this directory.")
    st.stop()

selected_year = st.sidebar.selectbox("Select Year:", years, key="year_select")

selected_event_folder_name = None
if selected_year:
    year_path = os.path.join(BASE_DATA_PATH, selected_year)
    events = get_subdirectories(year_path)
    if not events:
        st.sidebar.warning(f"No events found for {selected_year}.")
    else:
        selected_event_folder_name = st.sidebar.selectbox("Select Event:", events, key="event_select")

selected_session_folder_name = None
if selected_event_folder_name:
    event_path = os.path.join(year_path, selected_event_folder_name)
    session_folders = get_subdirectories(event_path)
    if not session_folders:
        st.sidebar.warning(f"No sessions found for {selected_event_folder_name}.")
    else:
        selected_session_folder_name = st.sidebar.selectbox("Select Session:", session_folders, key="session_select")

# Data View Selector
st.sidebar.markdown("---")
st.sidebar.header("Data View")
data_views = [
    "Event Info",
    "Session Results",
    "Lap Times",
    "Tyre Stints",
    "Weather Data",
    "Lap Telemetry Summary"
]
selected_data_view = st.sidebar.radio("Select data to visualize:", data_views, key="data_view_select")

# --- Main Area for Display ---
if selected_year and selected_event_folder_name and selected_session_folder_name:
    session_path = os.path.join(event_path, selected_session_folder_name)

    # Attempt to get prettier names from event_info.csv
    event_info_df_for_header = load_dataframe(os.path.join(session_path, 'event_info.csv'), "Header Info")
    header_event_name = selected_event_folder_name
    header_session_name = selected_session_folder_name
    if not event_info_df_for_header.empty:
        try:
            header_event_name = event_info_df_for_header['EventName'].iloc[
                0] if 'EventName' in event_info_df_for_header.columns else selected_event_folder_name
            header_session_name = event_info_df_for_header['SessionNameActual'].iloc[
                0] if 'SessionNameActual' in event_info_df_for_header.columns else selected_session_folder_name
        except Exception:
            pass  # Silently use folder names if issues with event_info content

    st.header(f"{header_event_name} - {header_session_name} ({selected_year})")
    st.markdown("---")

    if selected_data_view == "Event Info":
        df = load_dataframe(os.path.join(session_path, 'event_info.csv'), "Event Info")
        display_event_info(df)
    elif selected_data_view == "Session Results":
        df = load_dataframe(os.path.join(session_path, 'session_results.csv'), "Session Results")
        display_session_results(df)
    elif selected_data_view == "Lap Times":
        df = load_dataframe(os.path.join(session_path, 'laps_data.csv'), "Lap Times")
        display_lap_times(df)
    elif selected_data_view == "Tyre Stints":
        df = load_dataframe(os.path.join(session_path, 'tyre_stints_summary.csv'), "Tyre Stints")
        display_tyre_stints(df)
    elif selected_data_view == "Weather Data":
        df = load_dataframe(os.path.join(session_path, 'weather_data.csv'), "Weather Data")
        display_weather_data(df)
    elif selected_data_view == "Lap Telemetry Summary":
        df = load_dataframe(os.path.join(session_path, 'lap_telemetry_summary.csv'), "Lap Telemetry Summary")
        display_telemetry_summary(df)
else:
    st.info("⬅️ Please select a Year, Event, and Session from the sidebar to load visualizations.")

st.sidebar.markdown("---")
st.sidebar.info("This app visualizes F1 data processed from FastF1.")