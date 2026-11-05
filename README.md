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
not tracked in version control, except for the IMF list of debt sustainability
assessments (`raw_data/imf_dsa.csv`).

The IMF currently blocks scripted downloads of that list. When the download fails,
`get_raw_data.py` logs a warning and the charts use the saved copy. To update it by
hand, save https://www.imf.org/external/Pubs/ft/dsa/DSAlist.pdf from a browser and run:

```bash
uv run python -m scripts.analysis.update_dsa path/to/DSAlist.pdf
```

Once the raw data is fetched, run the `charts.py` script to generate the analysis outputs
stored in the `output/` directory.

For any issues or requests please open an issue on the GitHub repository.

## License

This project is licensed under the mit License - see the LICENSE file for details.

