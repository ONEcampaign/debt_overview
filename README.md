# Debt Overview

This repository powers the [Sovereign debt overview page](https://data.one.org/analysis/sovereign-debt)

## Installation

This project uses [uv](https://docs.astral.sh/uv/) for dependency management.

```bash
# Install dependencies
uv sync
```

## Usage

Scripts are located in the `scripts/` directory.

To run the analysis, run the `get_raw_data.py` script to fetch the latest data. 
This module will save the data in the `raw_data/` directory. Raw data is
not tracked in version control.

Once the raw data is fetched, run the `charts.py` script to generate the analysis outputs
stored in the `output/` directory.

For any issues or requests please open an issue on the GitHub repository.

## License

This project is licensed under the mit License - see the LICENSE file for details.

