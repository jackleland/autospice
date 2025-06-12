# AutoSpice

AutoSpice is a utility for generating job submission scripts for simulation codes such as SPICE and submitting them to various job schedulers.

## Installation

Create a Python environment and install the dependencies listed in `requirements.txt`:

```bash
pip install -r requirements.txt
```

`gspread` and `oauth2client` are optional dependencies used for logging job submissions to Google Sheets. If they are not installed, logging will be disabled gracefully.

## Usage

The main entry point is the `autospice.py` script which can be invoked via `python` or by installing it as a console script. A typical call looks like:

```bash
python autospice.py path/to/config.yml --log_fl
```

Example configuration files are provided in the `samples/` directory.

