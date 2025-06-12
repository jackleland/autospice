import abc
import datetime
import shutil
from pathlib import Path

from utils import find_next_available_dir


class SimulationCode(abc.ABC):
    """
    Abstract base class for storing code specific options and any necessary
    verification methods
    """

    SUBCLASS_COUNT = 0
    LOG_PREFIX = "log"

    EXE_COPY_SUBFOLDER = "localbin"
    EXE_FILES_TO_COPY = tuple()
    EXE_FOLDERS_TO_COPY = tuple()

    def __init__(
        self,
        name,
        mandatory_config_labels,
        optional_config_labels=None,
        boolean_config_labels=None,
    ):
        self.name = name
        self.mandatory_config_labels = mandatory_config_labels

        if optional_config_labels is not None:
            self.optional_config_labels = set(optional_config_labels)
        else:
            self.optional_config_labels = set()

        self.all_config_labels = (
            set(self.mandatory_config_labels) | self.optional_config_labels
        )

        if boolean_config_labels and self.all_config_labels.issuperset(
            set(boolean_config_labels)
        ):
            self.boolean_labels = boolean_config_labels
        else:
            self.boolean_labels = set()
        SimulationCode.increment_counter()

    def process_config_options(self, config_opts):
        all_config_labels = (
            set(self.mandatory_config_labels) | self.optional_config_labels
        )

        # Verify that all mandatory options are present
        if not set(self.mandatory_config_labels).issubset(config_opts.keys()):
            raise ValueError(
                "The options in the config file do not match those specified in the code's definition. \n"
                f"The config file should contain all mandatory options ({self.mandatory_config_labels}) "
                f'under the heading "{self.name}". \nMissing params: '
                f"{set(self.mandatory_config_labels) - set(config_opts)}"
            )

        # Verify that no undefined options were added in
        for label in config_opts:
            if label not in all_config_labels:
                raise ValueError(
                    f"An interloper option ({label}) was found in the config file. \n"
                    f"The config file should contain only these options: "
                    f'{all_config_labels} under the heading "{self.name}"'
                )

        # Set strings to bools if appropriate
        if self.boolean_labels:
            for boolean_label in self.boolean_labels:
                try:
                    config_opts.getboolean(boolean_label)
                except ValueError:
                    raise ValueError(
                        f'The boolean flag "{boolean_label}" is not set to a valid boolean value. \n'
                        f"The current value is {config_opts[boolean_label]}."
                    )
        return config_opts

    def copy_on_restart(self, output_dir, dryrun_fl, mode):
        # If restarting, copy files depending on the copy mode passed.
        if mode in ["0", "none"]:
            # Make no backup, run in the original directory
            print(f"You've opted not to backup the restart directory")
            return output_dir
        elif mode in ["1", "new"]:
            # Make a backup of the original directory and run from the backup
            restart_dir = find_next_available_dir(Path(f"{output_dir}_restart"))
            print(
                f"Restarting {self.name} run in directory {restart_dir}, leaving a backup of start files in "
                f"{output_dir} \n"
            )

            if not dryrun_fl:
                shutil.copytree(
                    output_dir, restart_dir, ignore=shutil.ignore_patterns("*backup*")
                )
            return restart_dir
        elif mode in ["2", "stay_out"]:
            # Make a backup of the original directory and run from the original directory
            restart_dir = find_next_available_dir(Path(f"{output_dir}_at_restart"))
            print(
                f"Restarting {self.name} run in directory {output_dir}, making a backup of start files in "
                f"{restart_dir} \n"
            )

            if not dryrun_fl:
                shutil.copytree(
                    output_dir, restart_dir, ignore=shutil.ignore_patterns("*backup*")
                )
            return output_dir
        elif mode in ["3", "stay_in"]:
            # Make a backup of the original directory inside the original directory and run from original directory
            datetime_str = datetime.datetime.today().strftime("%Y%m%d-%H%M")
            restart_dir = find_next_available_dir(
                output_dir / f"backup_at_restart_{datetime_str}"
            )
            print(
                f"Restarting {self.name} run in directory {output_dir}, making a backup of start files in "
                f"{restart_dir} \n"
            )

            if not dryrun_fl:
                shutil.copytree(
                    output_dir, restart_dir, ignore=shutil.ignore_patterns("*backup*")
                )
            return output_dir
        else:
            raise ValueError(
                "Invalid restart copy mode selected, see documentation for proper usage."
            )

    def copy_executable(self, output_dir, call_params, dryrun_fl):
        executable = Path(call_params["executable"].name)
        new_executable_dir = output_dir / self.EXE_COPY_SUBFOLDER
        if not dryrun_fl:
            new_executable_dir.mkdir(parents=True)
            shutil.copy(executable, new_executable_dir / executable)
            for exe_file in self.EXE_FILES_TO_COPY:
                shutil.copy(exe_file, new_executable_dir / exe_file)
            for exe_folder in self.EXE_FOLDERS_TO_COPY:
                shutil.copytree(exe_folder, new_executable_dir / exe_folder)
        call_params["executable"] = new_executable_dir / executable
        return call_params

    @abc.abstractmethod
    def print_config_options(self, config_opts):
        pass

    @abc.abstractmethod
    def get_command_line_args(self, config_opts):
        pass

    @abc.abstractmethod
    def get_submission_script_body(
        self,
        machine,
        call_params,
        multi_submission=False,
        safe_job_time_fl=False,
        backup_fl=False,
    ):
        pass

    @abc.abstractmethod
    def verify_input_file(self, input_file, config_opts):
        pass

    @abc.abstractmethod
    def is_parameter_scan(self, input_file):
        pass

    @classmethod
    @abc.abstractmethod
    def is_restart(cls, config_opts):
        pass

    @staticmethod
    @abc.abstractmethod
    def is_code_output_dir(directory):
        pass

    @abc.abstractmethod
    def directory_io(
        self, output_dir, config_opts, dryrun_fl, restart_copy_mode=1, print_fl=True
    ):
        pass

    @classmethod
    def increment_counter(cls):
        cls.SUBCLASS_COUNT += 1
