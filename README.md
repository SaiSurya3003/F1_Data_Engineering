# F1 Data Engineering Project

A comprehensive Formula 1 data pipeline that extracts, transforms, and visualizes F1 session data using the FastF1 library and Streamlit.

## 🏎️ Overview

This project provides an end-to-end solution for F1 data analysis, featuring:
- **Data Extraction**: Automated retrieval of F1 session data (lap times, telemetry, weather, results)
- **Data Transformation**: Processing and standardization of time-based data formats
- **Interactive Dashboard**: Web-based visualization of F1 session data

## ✨ Features

### Data Extraction
- **Session Data**: Race results, qualifying times, practice sessions
- **Lap Data**: Individual lap times, sector times, tire information
- **Telemetry**: Speed, RPM, throttle, brake data summaries
- **Weather Data**: Track conditions throughout sessions
- **Tire Stint Analysis**: Compound usage and stint summaries

### Data Transformation
- **Time Format Standardization**: Converts custom time formats to pandas timedelta objects
- **DateTime Processing**: Handles ISO datetime strings and session timestamps
- **Data Validation**: Robust error handling and data cleaning

### Interactive Dashboard
- **Session Selector**: Browse by year, event, and session type
- **Multiple Visualizations**:
  - Lap time progression by driver
  - Session results and standings
  - Weather condition trends
  - Tire stint analysis
  - Telemetry summaries

## 🛠️ Technologies Used

- **Python 3.11+**
- **FastF1**: Official F1 data API library
- **Pandas**: Data manipulation and analysis
- **Streamlit**: Web dashboard framework
- **Plotly**: Interactive visualizations
- **Arrow**: Advanced datetime handling
- **NumPy**: Numerical computing

## 📁 Project Structure

```
f1DataEngineering/
├── src/
│   ├── extract/
│   │   ├── f1_dataExtractor.py      # Main data extraction script
│   │   └── f1_data_output_csvs/     # Raw extracted data
│   └── transform/
│       ├── f1_dataTransform.py      # Data transformation pipeline
│       └── f1_data_transformed_time_objects/  # Processed data
├── streamlit_app.py                 # Dashboard application
├── .idea/                          # IntelliJ/PyCharm configuration
├── .venv/                          # Virtual environment
└── README.md
```

## 🚀 Getting Started

### Prerequisites

```bash
pip install fastf1 pandas streamlit plotly arrow numpy
```

### Installation

1. **Clone the repository**
   ```bash
   git clone https://github.com/yourusername/f1DataEngineering.git
   cd f1DataEngineering
   ```

2. **Set up virtual environment** (recommended)
   ```bash
   python -m venv .venv
   source .venv/bin/activate  # On Windows: .venv\Scripts\activate
   pip install fastf1 pandas streamlit plotly arrow numpy
   ```

3. **Create requirements.txt** (optional)
   ```bash
   pip freeze > requirements.txt
   ```

## 📊 Usage

### 1. Data Extraction

Run the extraction script to download F1 data:

```bash
python src/extract/f1_dataExtractor.py
```

**Configuration options in the script:**
- `YEARS`: List of years to process (default: [2025])
- `EVENTS`: Specific events or None for all events
- `SESSIONS`: Session types to extract (FP1, FP2, FP3, Q, R, Sprint, SS)
- `OUTPUT_DIRECTORY`: Where to save CSV files

### 2. Data Transformation

Process the extracted data:

```bash
python src/transform/f1_dataTransform.py
```

This converts:
- Custom time strings → pandas Timedelta objects
- ISO datetime strings → pandas Datetime objects
- Raw numeric seconds → Timedelta objects

### 3. Launch Dashboard

Start the Streamlit dashboard:

```bash
streamlit run streamlit_app.py
```

Access the dashboard at `http://localhost:8501`

## 📈 Data Flow

```
FastF1 API → Data Extractor → Raw CSV Files → Data Transformer → Processed CSV Files → Streamlit Dashboard → Interactive Visualizations
```

## 🔧 Configuration

### Data Extraction Settings

In `f1_dataExtractor.py`, modify these variables:

```python
# Years to process
YEARS = [2025]  # or list(range(2022, 2026))

# Specific events (optional)
EVENTS = None  # or {2023: [1, 'Monza'], 2024: ['Silverstone']}

# Session types
SESSIONS = ['FP1', 'FP2', 'FP3', 'Q', 'R', 'Sprint', 'SS']

# Output directory
OUTPUT_DIRECTORY = 'f1_data_output_csvs'
```

### Cache Configuration

The extractor uses FastF1's caching system:
- **Cache Location**: `~/fastf1_cache`
- **Auto-clear**: Cache is cleared on startup to ensure fresh data
- **Error Handling**: Falls back gracefully if cache setup fails

### Dashboard Data Path

In `streamlit_app.py`, update the data path if needed:

```python
BASE_DATA_PATH = os.path.join('src', 'transform', 'f1_data_transformed_timedelta_no_dh')
```

## 📋 Data Output

### Generated Files (per session)

- **`event_info.csv`** - Event metadata and session details
- **`session_results.csv`** - Final standings and times
- **`laps_data.csv`** - Individual lap information
- **`lap_telemetry_summary.csv`** - Aggregated telemetry data
- **`weather_data.csv`** - Session weather conditions
- **`tyre_stints_summary.csv`** - Tire strategy analysis

### Time Format Standards

- **Duration Times**: `mm:ss:SSS` (lap times, sector times)
- **Session Times**: `HH:MM:SS:MS` (absolute session timestamps)
- **Race Times**: `HH:MM:SS:MS` (total race duration)
- **Weather Times**: `HH:MM:SS:MS` (absolute local time)

### Sample Data Structure

#### Lap Data Columns
- `Driver`, `LapNumber`, `LapTime`
- `Sector1Time`, `Sector2Time`, `Sector3Time`
- `Compound`, `TyreLife`, `Stint`
- `IsAccurate`, `Time` (session timestamp)

#### Telemetry Summary Columns
- `DriverNumber`, `LapNumber`
- `AvgSpeed`, `MaxSpeed`, `MinSpeed`
- `AvgRPM`, `MaxRPM`
- `AvgThrottle`, `AvgBrake`
- `DRSActive`, `MaxDistance`

## 🎯 Dashboard Features

### Session Navigation
- **Year Selection**: Browse available seasons
- **Event Selection**: Choose specific Grand Prix
- **Session Selection**: Filter by session type (Practice, Qualifying, Race, Sprint)

### Visualization Types

#### 1. Event Information
- Event metadata, location, and session details
- Local and UTC session start times

#### 2. Session Results
- Final standings and times
- Bar charts of session completion times
- Qualifying sector times (Q1, Q2, Q3)

#### 3. Lap Time Analysis
- Line charts showing lap time progression
- Driver comparison and filtering
- Option to show only "accurate" laps
- Hover data includes tire compound and stint info

#### 4. Weather Tracking
- Air and track temperature trends
- Humidity, pressure, and wind speed
- Rainfall indicators throughout the session

#### 5. Tire Strategy Analysis
- Stint summaries by driver
- Compound usage visualization
- Laps per stint analysis

#### 6. Telemetry Insights
- Average speed progression per lap
- RPM, throttle, and brake data summaries
- DRS usage indicators

## 🎨 Dashboard Interface

### Sidebar Controls
- **Session Selector**: Dropdown menus for year, event, and session
- **Data View Selector**: Radio buttons for different visualization types
- **Filter Options**: Driver selection, accurate laps only, etc.

### Main Display Area
- **Dynamic Headers**: Shows selected event and session names
- **Interactive Charts**: Plotly-powered visualizations with hover data
- **Data Tables**: Sortable and scrollable data displays
- **Responsive Layout**: Adapts to different screen sizes

## 🔄 Data Processing Pipeline

### 1. Extraction Phase
```python
# Key functions in f1_dataExtractor.py
get_session_data()  # Main extraction function
format_seconds_to_mmssms()  # Time formatting
format_arrow_to_hhmmssms()  # Absolute time conversion
```

### 2. Transformation Phase
```python
# Key functions in f1_dataTransform.py
parse_custom_format_to_timedelta()  # Custom time string parsing
transform_csv_file()  # File-by-file transformation
main_transform()  # Batch processing
```

### 3. Visualization Phase
```python
# Key functions in streamlit_app.py
preprocess_laps_data()  # Data preparation for charts
display_lap_times()  # Interactive lap time visualization
safe_to_timedelta_seconds()  # Robust time conversion
```

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

### Development Guidelines

- Follow PEP 8 style guidelines
- Add logging for new functions
- Include error handling for data processing
- Update documentation for new features
- Test with multiple seasons and events

## ⚠️ Known Limitations

- **Data Availability**: Limited by FastF1 API coverage and F1's data release schedule
- **Cache Size**: FastF1 cache can grow large over time (several GB)
- **Rate Limits**: API requests may be throttled for large date ranges
- **Future Events**: Cannot extract data for events that haven't occurred
- **Historical Data**: Some older seasons may have incomplete telemetry data
- **Memory Usage**: Large datasets may require significant RAM for processing

## 🐛 Troubleshooting

### Common Issues

1. **Cache Errors**
   ```bash
   # Clear cache manually
   rm -rf ~/fastf1_cache
   ```

2. **Missing Data**
   - Some sessions may not have complete telemetry data
   - Check FastF1 API status and data availability

3. **Time Format Issues**
   - Ensure system timezone is properly configured
   - Check for NaN values in time columns

4. **Dashboard Loading**
   - Verify transformed CSV files exist in expected directory
   - Check file permissions and paths

5. **Import Errors**
   ```bash
   # Reinstall dependencies
   pip install --upgrade fastf1 pandas streamlit plotly arrow numpy
   ```

### Debug Mode

Enable detailed logging in any script:
```python
import logging
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')
```

### Performance Tips

- **Selective Data Extraction**: Limit years and sessions for faster processing
- **Cache Management**: Periodically clear cache to free disk space
- **Memory Management**: Process one year at a time for large datasets

## 📚 Additional Resources

- [FastF1 Documentation](https://docs.fastf1.dev/)
- [Streamlit Documentation](https://docs.streamlit.io/)
- [Plotly Documentation](https://plotly.com/python/)
- [Formula 1 Technical Regulations](https://www.fia.com/regulation/category/110)

## 🔮 Future Enhancements

- [ ] Real-time data updates during live sessions
- [ ] Advanced statistical analysis (correlation, regression)
- [ ] Driver performance comparison tools
- [ ] Export functionality for processed data
- [ ] API endpoints for external access
- [ ] Machine learning predictions
- [ ] Mobile-responsive design improvements

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## 🙏 Acknowledgments

- **FastF1**: Amazing library for F1 data access created by [@theOehrly](https://github.com/theOehrly)
- **Formula 1**: For making telemetry data available through official channels
- **Streamlit Community**: For excellent visualization and dashboard tools
- **Python Community**: For the incredible ecosystem of data science libraries

---

**Note**: This project is for educational and analysis purposes. All F1 data is sourced through the official FastF1 library and is subject to their terms of use. No real-time or proprietary data is accessed directly from Formula 1 systems.
