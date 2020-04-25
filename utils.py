from pathlib import Path


def find_next_available_filename(filename):
    filename_ext = filename.suffix
    return find_next_available_dir(filename).with_suffix(filename_ext)


def find_next_available_dir(directory):
    i = 0
    dummy_dir = directory
    while dummy_dir.exists():
        i += 1
        dummy_dir = dummy_dir.parent / Path(f"{directory.stem}_{i}_")
    return dummy_dir

# TODO: This has been temporarily removed due to the git repo object not being present.
#
# from warnings import warn
# import git.repository as git
#
# def git_check(executable_path):
#     # Check if latest changes to executable have been committed
#     repo = git.Repository(executable_path, search_parent_directories=True)
#     if repo.is_dirty():
#         warn("There are uncommitted changes to the executable code's git "
#              "repository")
#
#     # TODO: Check if the executable has been compiled since the last commit
#     # TODO: Other repos e.g. mercurial etc.?
def full(path):
    return str(path.resolve())
