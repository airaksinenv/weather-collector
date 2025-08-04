# Weather Collector

Collects historical weather data from the **Finnish Meteorological Institute (FMI)** using an **Azure Function App**, outputting the data as a pandas DataFrame.

---

Author:
Valtteri Airaksinen

Version:
0.0.1

Release Date:
TBD


## Overview
This project provides an **Azure Function App** that:

- Fetches **daily historical weather data** from FMI’s API  
- Uses a Python-based function with a modular structure for easier maintenance  
- Is designed for **GitHub → Azure Function** deployment, installing dependencies during the build phase  

---

## Repository Structure

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


- **`DataCollectorFunc`**: Contains the function definition and configuration  
- **`shared`**: Reusable Python modules used by the function  
- **Workflow**: Dependencies are installed during the GitHub Actions build phase due to Azure Function deployment constraints  


## Installation & Usage

### Prerequisites
- Python 3.9+  
- Azure Functions Core Tools (for local testing)  
- GitHub Actions enabled for CI/CD (if deploying automatically)

### Local Development
1. Clone the repository
2. Create a virtual environment and install required packages from `requirements.txt`
3. Start the function app locally:
   ```bash
   func start