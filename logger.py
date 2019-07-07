import gspread
from oauth2client.service_account import ServiceAccountCredentials
import datetime
from utils import find_next_available_filename
from warnings import warn
import pathlib as p
import pickle


class Logger(object):
    """
    Object for logging job submissions, using a google spreadsheet as the database and a google API for interacting
    with google docs.

    A guide for setting up the google drive API (GDAPI) using google's developer console is here:
    https://www.twilio.com/blog/2017/02/an-easy-way-to-read-and-write-to-a-google-spreadsheet-in-python.html

    The minimum requirement for this to work is that a .json file with the proper API key in it is present in the
    same folder as logger.py (or able to be pointed to using api_json_filename). The google sheet must also have given
    edit access to the account associated with the GDAPI - this is all information available in the linked guide above.

    Format for the logging is customisable through the log_format kwarg, which must be a dictionary of
    {column_name (str): column_type (type)}. An id and timestamp are always present.
    """
    SCOPE = [
        'https://spreadsheets.google.com/feeds',
        'https://www.googleapis.com/auth/drive'
    ]
    DEFAULT_LOG_FORMAT = {
        'machine': str,
        'job_number': str,
        'job_name': str,
        'input_file': str,
        'masala_config': str,
        'nodes': int,
        'total_cores': int,
        'memory_req': int,
        'wtime_req': str,
        'notes': str
    }
    BACKUP_FILENAME = 'offline_row_update.txt'

    def __init__(self, api_json_filename='client_secret.json', gsheet_name='spice-submission-db', log_format=None):
        """

        :param api_json_filename:
        :param gsheet_name:
        :param log_format:
        """
        self.creds = ServiceAccountCredentials.from_json_keyfile_name(api_json_filename, self.SCOPE)
        self.client = gspread.authorize(self.creds)
        self.gsheet_name = gsheet_name

        # Verify that passed log_format is valid
        if log_format is not None and self.verify_log_format(log_format):
            self.log_format = log_format
        else:
            print(f"Using default log format {self.DEFAULT_LOG_FORMAT}")
            self.log_format = self.DEFAULT_LOG_FORMAT

        # Attempt to set up client connection
        try:
            self.sheet = self.client.open(gsheet_name).sheet1
        except gspread.GSpreadException as e:
            print("Unable to connect to google spreadsheet, logging not possible with "
                  "current settings. "
                  f"{e}")
            self.sheet = None

    def update_log(self, log_data, backup_data=True, dry_run_fl=False):
        index = len(self.sheet.get_all_values())
        timestamp = str(datetime.datetime.now())

        # Every row starts with an id and timestamp
        row = [index, timestamp]

        # Verify log data conforms to stored log format
        if not self.verify_log_data(log_data):
            print('Log data malformed, cannot update database.')
            return

        # Append the checked log data to row and update database if possible
        row.extend(list(log_data.values()))
        if self.sheet is not None and not dry_run_fl:
            self.sheet.insert_row(row, index + 1)
            print(f'Database updated successfully.')
        elif dry_run_fl:
            print(f'DRY RUN RESULT: {row}')
        else:
            print(f'{self.gsheet_name} could not be accessed right now, the row '
                  f'\n {row} \n'
                  f'will need to be added to the database manually. ')
            if backup_data:
                # Write the log_data dictionary to file via pickle to load and update db with later.
                backup_filename = find_next_available_filename(p.Path(self.BACKUP_FILENAME))
                print(f'Also pickling this to file {backup_filename}, which can be loaded'
                      f'and added to the database later.')
                with open(backup_filename, 'wb') as fp:
                    pickle.dump(log_data, fp)

    def update_log_from_backup(self, backup_filename):
        if not p.Path(backup_filename).exists():
            raise ValueError('Passed filename does not exist.')

        # Load log_data db from previous attempt. This does not retain the id or timestamp
        with open(backup_filename, 'rb') as fp:
            log_data = pickle.load(fp)
        self.update_log(log_data)

    @staticmethod
    def verify_log_format(log_format):
        assert isinstance(log_format, dict)
        for col_name, col_type in log_format.items():
            if not isinstance(col_name, str) or not isinstance(col_type, type):
                print(f'Column name & type combination of {col_name, col_type} is not valid.'
                      f'log_format must be a dictionary with the keys representing column names, '
                      f'which must be strings, and the values being the type to be stored in that'
                      f'column.')
                return False
            if col_type.__str__ is object.__str__:
                warn(f'{col_name} is of type {col_type} which does not have an implemented'
                     f'__str__ method, so it will not look pretty in your database.')
        return True

    # noinspection PyTypeHints
    def verify_log_data(self, log_data):
        assert isinstance(log_data, dict)
        assert isinstance(self.log_format, dict)

        for col_name, col_value in log_data.items():
            if col_name not in self.log_format:
                print(f'{col_name} was not found in log_format. Log data must consist only '
                      f'of these columns {list(self.log_format.keys())}')
                return False
            required_col_type = self.log_format[col_name]
            if not isinstance(col_value, required_col_type):
                print(f'Value of [{col_name}, {col_value}], is not of the required type '
                      f'{required_col_type}. Was found to be of {type(col_value)}.')
                return False

        for col_name in self.log_format.keys():
            if col_name not in log_data:
                print(f'{col_name} was found to be missing from log_data. Log data requires all'
                      f'of the following column names to be present: '
                      f'{list(self.log_format.keys())}')
                return False
        return True
