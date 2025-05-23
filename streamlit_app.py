import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import os
import numpy as np

# --- Configuration ---
BASE_DATA_PATH = os.path.join('src', 'transform',
                              'f1_transformed_data_output')  # From your last transform script


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
    """Safely converts a series of timedelta strings to total seconds.
    Handles 'HH:MM:SS:ms' format by converting to 'HH:MM:SS.ms'.
    """
    if series is None or series.empty:
        return pd.Series(dtype='float64')

    # Convert to string to ensure .str accessor works, then replace last ':' with '.' for milliseconds
    # This specifically addresses the 'HH:MM:SS:ms' format in your weather data
    processed_series = series.astype(str).apply(
        lambda x: x.rsplit(':', 1)[0] + '.' + x.rsplit(':', 1)[1] if ':' in x and x.count(':') == 3 else x
    )

    try:
        td_series = pd.to_timedelta(processed_series, errors='coerce')
        return td_series.dt.total_seconds()
    except Exception as e:
        st.warning(f"Could not convert column '{column_name}' to timedelta seconds: {e}. Some values might be invalid.")
        return pd.to_timedelta(processed_series, errors='coerce').dt.total_seconds()

def format_seconds_to_hms_ms(seconds):
    """Converts a duration in seconds (float) to HH:MM:ss:SSS format string."""
    if pd.isna(seconds):
        return None
    total_milliseconds = int(seconds * 1000)
    hours = total_milliseconds // (1000 * 60 * 60)
    total_milliseconds %= (1000 * 60 * 60)
    minutes = total_milliseconds // (1000 * 60)
    total_milliseconds %= (1000 * 60)
    seconds = total_milliseconds // 1000
    milliseconds = total_milliseconds % 1000
    return f"{hours:02}:{minutes:02}:{seconds:02}:{milliseconds:03}"


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

    # Try converting other columns to numeric, coercing errors
    numeric_cols = ['AirTemp', 'TrackTemp', 'Humidity', 'Pressure', 'WindSpeed']
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')

    # Robust handling for Rainfall:
    if 'Rainfall' in df.columns:
        # Attempt to convert to boolean first (handles 'True', 'False', '0', '1')
        rainfall_mapped_bool = df['Rainfall'].astype(str).str.lower().map({
            'true': True, '1': True, '1.0': True,
            'false': False, '0': False, '0.0': False
        }).copy()

        if not rainfall_mapped_bool.isnull().all() and \
           (True in rainfall_mapped_bool.unique() or False in rainfall_mapped_bool.unique()):
            df['Rainfall'] = rainfall_mapped_bool.fillna(False)
        else:
            df['Rainfall'] = pd.to_numeric(df['Rainfall'], errors='coerce')
            if df['Rainfall'].isnull().all():
                df['Rainfall'] = False

    # Add the formatted time column
    df['SessionTimeFormatted'] = df['SessionTimeSeconds'].apply(format_seconds_to_hms_ms)

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
    if 'Driver' in df.columns:
        df['Driver'] = df['Driver'].astype(str) # Ensure driver column is string type
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
    if df.empty:
        st.info("No event information available.")
        return
    # Display as a more readable format, transposing it
    st.table(df.iloc[0].T.rename("Value"))


def display_session_results(df):
    st.subheader("Session Results")
    if df.empty:
        st.info("No session results data available.")
        return

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
    if df.empty:
        st.info("No lap times data available.")
        return

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
                    'Stint': True,
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
    if df.empty:
        st.info("No weather data available from the source file.")
        return

    weather_df_processed = preprocess_weather_data(df.copy())

    if weather_df_processed.empty or 'SessionTimeSeconds' not in weather_df_processed.columns or weather_df_processed[
        'SessionTimeSeconds'].isnull().all():
        st.info("No processable weather data or session time for plotting after preprocessing.")
        return

    # Display the DataFrame with the new formatted column
    st.dataframe(weather_df_processed[['Time', 'SessionTimeFormatted', 'AirTemp', 'TrackTemp', 'Humidity', 'Pressure', 'WindSpeed', 'Rainfall']], hide_index=True)


    weather_vars = {
        'AirTemp': 'Air Temperature (°C)',
        'TrackTemp': 'Track Temperature (°C)',
        'Humidity': 'Humidity (%)',
        'Pressure': 'Air Pressure (hPa)',
        'WindSpeed': 'Wind Speed (km/h)',
        'Rainfall': 'Rainfall'
    }

    st.markdown("---")
    st.write("#### Weather Trends")

    # Determine tick values and labels for the x-axis to represent time delta
    # Select a subset of data points for ticks to prevent overcrowding
    num_ticks = 10
    if len(weather_df_processed) > 0:
        indices_for_ticks = np.linspace(0, len(weather_df_processed) - 1, num_ticks, dtype=int)
        tick_vals_seconds = weather_df_processed['SessionTimeSeconds'].iloc[indices_for_ticks].tolist()
        tick_texts_formatted = weather_df_processed['SessionTimeFormatted'].iloc[indices_for_ticks].tolist()
    else:
        tick_vals_seconds = []
        tick_texts_formatted = []


    for var, label in weather_vars.items():
        if var in weather_df_processed.columns and not weather_df_processed[var].isnull().all():
            st.markdown(f"##### {label}")

            fig = go.Figure()

            # Determine if Rainfall should be treated as boolean (1/0) or numeric
            if var == 'Rainfall' and weather_df_processed[var].dtype == 'bool':
                # For boolean rainfall, use scatter to clearly show discrete states
                # Map True/False to numeric 1/0 for plotting
                plot_y = weather_df_processed[var].astype(int)
                fig.add_trace(go.Scatter(
                    x=weather_df_processed['SessionTimeSeconds'],
                    y=plot_y,
                    mode='markers',
                    name=label,
                    marker=dict(
                        color=np.where(weather_df_processed[var], 'blue', 'grey'),
                        size=8
                    ),
                    hoverinfo='text',
                    hovertext=[f"Time: {t}<br>{label}: {'Rain' if r else 'No Rain'}"
                               for t, r in zip(weather_df_processed['SessionTimeFormatted'], weather_df_processed[var])]
                ))
                fig.update_yaxes(tickvals=[0, 1], ticktext=['No Rain', 'Rain'], title=label)
            else:
                # For other numeric variables, use line plot
                fig.add_trace(go.Scatter(
                    x=weather_df_processed['SessionTimeSeconds'],
                    y=weather_df_processed[var],
                    mode='lines+markers',
                    name=label,
                    hoverinfo='text',
                    hovertext=[f"Time: {t}<br>{label}: {val:.2f}" # Format numeric values to 2 decimal places
                               for t, val in zip(weather_df_processed['SessionTimeFormatted'], weather_df_processed[var])]
                ))
                fig.update_yaxes(title=label) # Set y-axis title for numeric plots

            fig.update_layout(
                title=f"{label} over Session",
                xaxis=dict(
                    title='Session Time',
                    tickmode='array',
                    tickvals=tick_vals_seconds,
                    ticktext=tick_texts_formatted,
                    type='linear' # Ensure axis is linear for numeric seconds
                ),
                hovermode='x unified' # Ensures hover shows all relevant data at a given x
            )
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info(f"No valid data to plot for '{label}'.")


def display_tyre_stints(df):
    st.subheader("Tyre Stint Summary")
    if df.empty:
        st.info("No tyre stints data available.")
        return

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
    if df.empty:
        st.info("No telemetry summary data available.")
        return

    telemetry_df_processed = preprocess_telemetry_summary(df.copy())
    if telemetry_df_processed.empty:
        st.info("No processable telemetry summary data.")
        return

    st.dataframe(telemetry_df_processed.head(20), hide_index=True)  # Show a sample
    if st.checkbox("Show full telemetry summary table?", key="show_full_telemetry_table"):
        st.dataframe(telemetry_df_processed, hide_index=True)

    # Example: AvgSpeed vs. LapNumber for a single selected driver
    if 'Driver' in telemetry_df_processed.columns and \
            'LapNumber' in telemetry_df_processed.columns and \
            'AvgSpeed' in telemetry_df_processed.columns:

        st.markdown("---")
        st.write("#### Average Speed per Lap (Single Driver)")
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

        # New: Compare Avg Speed Plot among multiple drivers
        st.markdown("---")
        st.write("#### Average Speed Comparison Among Drivers")
        if drivers_telemetry:
            default_drivers_comparison = drivers_telemetry[:min(len(drivers_telemetry), 3)] # Default to 3 for comparison
            selected_drivers_comparison = st.multiselect(
                "Select Drivers for Average Speed Comparison:", drivers_telemetry,
                default=default_drivers_comparison,
                key="telemetry_drivers_compare_select"
            )

            if selected_drivers_comparison:
                compare_df = telemetry_df_processed[
                    telemetry_df_processed['Driver'].isin(selected_drivers_comparison)
                ].sort_values(by=['Driver', 'LapNumber'])

                if not compare_df.empty:
                    fig_compare = px.line(compare_df, x='LapNumber', y='AvgSpeed', color='Driver',
                                          title="Average Speed Comparison by Driver",
                                          labels={'AvgSpeed': 'Average Speed (km/h)'},
                                          markers=True)
                    fig_compare.update_layout(legend_title_text='Driver')
                    st.plotly_chart(fig_compare, use_container_width=True)
                else:
                    st.info("No data for selected drivers for comparison.")
            else:
                st.info("Select at least one driver to compare average speed.")


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