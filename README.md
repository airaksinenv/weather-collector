# Weather Collector

Collects historical weather data from the **Finnish Meteorological Institute (FMI)** using an **Azure Function App**, outputting the data as a pandas DataFrame.

---

Author:
Valtteri Airaksinen

Version:
0.1.0

Release Date:
28.8.2025


## Overview
This project provides an **Azure Function App** that:

- Fetches **daily historical weather data** from FMI’s API  
- Uses a Python-based function with a modular structure for easier maintenance  
- Is designed for **GitHub → Azure Function** deployment, installing dependencies during the build phase

## Repository Structure
```
weather-collector/
│
├── DataCollectorFunc/ # Azure Function entrypoint
│ ├── init.py # Function entry script
│ └── function.json # Azure Function configuration
│
├── shared/ # Core logic and utilities
│ ├── main.py # Main data collection logic
│ └── utils.py # Helper functions
│
└── .github/workflows/ # Deployment workflow
```

- **`DataCollectorFunc`**: Contains the function definition and configuration  
- **`shared`**: Reusable Python modules used by the function  
- **Workflow**: Dependencies are installed during the GitHub Actions build phase due to Azure Function deployment constraints  


## Installation & Usage

### Prerequisites
- Python 3.10
- Azure Functions Core Tools (for local testing)  
- GitHub Actions enabled for CI/CD (if deploying automatically)

### Local Development
1. Clone the repository
2. Create a virtual environment and install required packages from `requirements.txt`
    ```bash
    python -m venv .venv
    source .venv/Scripts/activate
    pip install -r requirements.txt
3. Start the function app locally:
   ```bash
   python shared/main.py
   ```
## Data sources

### Daily weather data

| Required field | Source | Parameter name |
|:-:|:-:|:-:|
| temp_avg | kriging_suomi_daily | DailyMeanTemperature |
| temp_min | kriging_suomi_daily | MinimumTemperature24h |
| temp_max | kriging_suomi_daily | MaximumTemperature24h |
| prec | kriging_suomi_daily | Precipitation24h |
| wind_speed_avg | kriging_suomi_hourly | WindSpeedMS |
| wind_speed_max | kriging_suomi_daily | MaximumWind |
| wind_dir_avg | - | - |
| rel_humid_avg | kriging_suomi_hourly | Humidity |
| rel_humid_max | kriging_suomi_hourly | Humidity |
| rel_humid_min | kriging_suomi_hourly | Humidity |
| global_rad | krging_suomi_daily | DailyGlobalRadiation |
| vapour_press | kriging_suomi_hourly | Temperature & Humidity ([Formula 1](https://www.vaisala.com/fi/expert-article/relative-humidity-how-is-it-defined-and-calculated) & [Formula 2](https://www.vaisala.com/fi/lp/make-your-job-easier-humidity-conversion-formulas))  |
| snow_depth | - | - |

---

### 3h weather data
| Required field | Source | Parameter name |
|:-:|:-:|:-:|
| temp | kriging_suomi_synop | Temperature |
| prec | - | - |
| wind_speed | kriging_suomi_synop | WindSpeedMS |
| rel_humid | kriging_suomi_synop | Humidity |

---

### 1h weather data
| Required field | Source | Parameter name |
|:-:|:-:|:-:|
| - | kriging_suomi_hourly | Precipitation1h |
| - | kriging_suomi_hourly | Humidity |
| - | kriging_suomi_hourly | WindSpeedMS |
| - | kriging_suomi_hourly | Temperature |

---

## TBA

- Need to add a logic for collecting monthly data.