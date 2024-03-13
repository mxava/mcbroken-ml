### HERE BE DRAGONS
from pathlib import Path
import os
#########################
# Not necessary for now #
#########################
#import pyspark
#from pyspark.sql import SparkSession, DataFrameReader
#from pyspark.sql.functions import col, min, max
#from pyspark.sql.types import *
#from pyspark.rdd import *
#import pandas as pd
import json
# import sqlalchemy
import git


# extract mcbroken.json archives

# this will all change once we have a real place to retrieve and store data from
mcbroken_archive_repo_path = Path(os.environ.get('HOME')) / 'code' / 'mcbroken-archive'
os.chdir(mcbroken_archive_repo_path)


def do_the_thing(repo_path: Path = Path.cwd(),
                    dump_dir:  Path = None
                    ) -> list:
    """Extracts and transforms archived datasets from a git repo,
    sequencing them in a directory from oldest to newest.

    Args:
        repo (Path): git repo path
        dump_dir (str): target directory to dump dataset

    Returns:
        output : itemized list of files added
    """
    # Ensure we're in the right place.
    os.chdir(repo_path) 
    # If does not exist, create target dir
    if dump_dir == None:
        dump_dir = repo_path / 'DATA_DUMP'
    dump_dir.mkdir(parents=True, exist_ok=True)
    generate_dump_data(repo_path, dump_dir)
    # TODO: Add the next thing
    return
                

def generate_dump_data(repo_path: Path=Path.cwd(),
                      dump_dir: Path=None
                      ) -> int:
    """Stores archive dump to dataframe

    Args:
        repo_path (Path, optional): Path for archive repo. Defaults to Path.cwd().
        dump_dir (Path, optional): Path for dump directory. Defaults to (Path.cwd() / 'DATA_DUMP').

    Returns:
        None
    """
    repo = git.Repo(repo_path)
    repo.remotes.origin.fetch()
    commit_list = repo.git.log('--pretty=%h', '--all', '--', './mcbroken.json')
    #convert string output to list
    commit_list = commit_list.split('\n')
    commit_times_json_path = dump_dir / '000_COMMIT_TIMES.json'
    for each in commit_list:
        current_path = dump_dir / f'{each}.json'
        if current_path.exists():
            continue
        else:
            print(f'Fetching data for \'{each}\'...')
            current_contents = repo.git.show(f'{each}:mcbroken.json')
            with open(current_path, 'w+') as f:
                f.write(current_contents)
            if commit_times_json_path.is_file() != True:
                with open(commit_times_json_path, 'w+') as f:
                    commit_times = dict()
                    commit_times[each] = repo.git.show('--no-patch', '--pretty=format:%ct', each)
                    commit_times_as_json = json.dumps(commit_times)
                    f.write(commit_times_as_json)
            else:
                with open(commit_times_json_path, 'r+') as f:
                    commit_times = json.load(f)
                    commit_times[each] = repo.git.show('--no-patch', '--pretty=format:%ct', each)
                    f.seek(0)
                    commit_times_as_json = json.dumps(commit_times)
                    print(len(commit_times))
                    f.write(commit_times_as_json)

            

# TODO: Figure out SQL DB schema/desired data

# TODO: Convert mcbroken "last_checked" time to a time stamp

# TODO: Transform mcbroken.json data according to sql db schema

# TODO: Throw transformed mcbroken data into mcflurry table

# TODO: Fetch locations of weather data based on location and timestamp

# TODO: Transform weather data

# TODO: Throw into weather table


### Scratchpad
if __name__ == '__main__':
    computer_on_fire = do_the_thing(mcbroken_archive_repo_path)
