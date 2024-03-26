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
import requests
import json
# import sqlalchemy
import git


class DataAggregate:
    
    def __init__(self, name):
        self.name = name
        self.df # empty dataframe

    ### Save / Load Methods

    # TODO: Save DataAggregate to file
        
    # TODO: Load DataAggregate from file
        
    # TODO: Write to db
        
    # MAYBE TODO: Load from db query (probably won't need this)
    
    # TODO: Ingest transformed McBroken data
    
    # TODO: Ingest transformed weather data
    
    # TODO: Ingest transformed local purchasing power data


class McBrokenData:
    
    def __init__(self,
                 repo_url:str = 'https://github.com/rashiq/mcbroken-archive.git',
                 name:str = None,
                 working_tree_dir: Path = None,
                 dump_dir: Path = None):
        self.name = name
        self.source_url = repo_url
        self.repo_dir = working_tree_dir if working_tree_dir else (Path.cwd() / 'mcbroken-archive')
        self.dump_dir = dump_dir if dump_dir else (self.repo_dir / 'DATA_DUMP')
        git_repo_init = self.initialize_git_repo()
        self.repo = git_repo_init[0]
        self.is_initialized = git_repo_init[1]
    
    def initialize_git_repo(
            self,
            mcbroken_archive_url_git: str = None,
            working_tree_dir: Path = None
            ) -> tuple:
        """Initializes git repo.

        Args:
            mcbroken_archive_url_git (str, optional): git origin source.
            working_tree_dir (Path, optional): Path to mcbroken-archive repo directory.

        Raises:
            RuntimeError: Indicates git repo initialization failed.

        Returns:
            tuple[0]: git repo object
            tuple[1]: bool indicating it successfully initialized
        """
        source_url = mcbroken_archive_url_git if mcbroken_archive_url_git is not None else self.source_url
        repo_dir = working_tree_dir if working_tree_dir is not None else self.repo_dir
        # Attempt to initialize repo
        try:
            repo = git.Repo(repo_dir)
            # Assumes that any valid repo at this location is the correct one
            assert repo.git_dir
        except:
            print(f'Repo not found at {repo_dir} - cloning from {source_url}...\n This may take a while!')
            try:
                repo = git.Repo.clone_from(source_url,
                           str(repo_dir),
                           branch='main')
                assert repo.git_dir
            except:
                is_initialized = False
                raise RuntimeError(f'Unable to locate or clone valid git repo at path "{repo_dir}"')
        repo.remotes.origin.fetch()
        is_initialized = True
        return repo, is_initialized
    
    def et_phone_home(self):
        # Generate dump data
        repo = self.repo
        repo.remotes.origin.fetch()
        if not self.dump_dir.exists():
            self.dump_dir.mkdir(parents=True, exist_ok=True)
        commit_list = repo.git.log('--pretty=%h', '--all', '--', './mcbroken.json')
        # Convert string output to list
        commit_list = commit_list.split('\n')
        commit_times_json_path = self.dump_dir / '000_COMMIT_TIMES.json'
        commit_times = {}
        try:
            with open(commit_times_json_path, "r+") as f:
                commit_times = json.load(f)
        except:
            print(f'\'000_COMMIT_TIMES.json\' not found in {self.dump_dir}. Generating new - this may take a while!')
            pass
        for each in commit_list:
            current_file_path = self.dump_dir / f'{each}.json'
            # Ensure we do not duplicate work
            if each not in commit_times:
                commit_times[each] = {}
                commit_times[each]['commit_time'] = repo.git.show('--no-patch', '--pretty=format:%ct', each)
                commit_times[each]['time_fixed'] = False
                commit_times[each]['processed'] = False
            if commit_times[each]['time_fixed'] and commit_times[each]['processed'] == True:
                continue
            # Try to read from already-existing data in case time has already been fixed
            if current_file_path.exists():
                with open(current_file_path, 'r+') as f:
                    # TODO: only check mcbroken contents once
                    try:
                        try:
                            mcbroken_json_contents = json.load(f)
                        except:
                            print(f"Generating {each}.json...")
                            mcbroken_json_contents = json.loads(str(repo.git.show(f'{each}:mcbroken.json')))
                    except:
                        print(f'Something went wrong with \'{each}.json\', and I don\'t feel like debugging it anymore - skipping! ayy lmao 👽')
                        commit_times[each]['time_fixed'] = True
                        commit_times[each]['processed'] = True
                        continue
                    # Checks if last_checked has already been converted to epoch time.
                    if not isinstance(mcbroken_json_contents[len(mcbroken_json_contents) - 1]['properties']['last_checked'], int):
                        print(f"Fixing times in {each}.json...")
                        mcbroken_json_contents = self.fix_times(mcbroken_json_contents, commit_times[each]['commit_time'])
                    mcbroken_json_contents = json.dumps(mcbroken_json_contents)
                    f.seek(0)
                    f.write(mcbroken_json_contents)
                    commit_times[each]['time_fixed'] = True
                    continue
            with open(commit_times_json_path, 'w+') as f:
                commit_times = json.dumps(commit_times, sort_keys=True, indent=4)
                f.seek(0)
                f.write(commit_times)
                commit_times = json.loads(commit_times)


    def fix_times(self, input_mcbroken_json: dict, commit_timestamp: int, commit_hash: str) -> dict:
        output_mcbroken_json = []
        print(f"Fixing {commit_hash}.json...")
        output_mcbroken_json = input_mcbroken_json
        for each_entry in output_mcbroken_json:
            try:
                assert isinstance(each_entry['properties']['last_checked'], int)
                continue
            except:
                value_to_subtract = each_entry['properties']['last_checked'].strip(' Checkedminutesago')
                # Convert to seconds
                value_to_subtract = int(value_to_subtract) * 60
                each_entry['properties']['last_checked'] = int(commit_timestamp) - value_to_subtract
        #output_mcbroken_json = json.dumps(output_mcbroken_json, sort_keys=True, indent=4)
        return output_mcbroken_json





        


class WeatherData:

    # TODO: Think about how to structure McBroken db,
    # followed by how best to fetch, parse, and load weather data
    def __init__(self):
        return


# TODO: Figure out SQL DB schema/desired data

# TODO: Convert mcbroken "last_checked" time to a time stamp

# TODO: Transform mcbroken.json data according to sql db schema

# TODO: Throw transformed mcbroken data into mcflurry table

# TODO: Fetch locations of weather data based on location and timestamp

# TODO: Transform weather data

# TODO: Throw into weather table


### Scratchpad
if __name__ == '__main__':
    mcbroken_data = McBrokenData(name='mcbroken')
    mcbroken_data.et_phone_home()
    mcbroken_data.fix_times()
