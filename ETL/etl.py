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
    
    def extract(self):
        commit_list = self.repo.git.log('--pretty=%h', '--all', '--', './mcbroken.json')
        # Generate dump data
        repo = self.repo
        repo.remotes.origin.fetch()
        if not self.dump_dir.exists():
            self.dump_dir.mkdir(parents=True, exist_ok=True)
        commit_list = repo.git.log('--pretty=%h', '--all', '--', './mcbroken.json')
        #convert string output to list
        commit_list = commit_list.split('\n')
        commit_times_json_path = self.dump_dir / '000_COMMIT_TIMES.json'
        for each in commit_list[0:10]:
            current_file_path = self.dump_dir / f'{each}.json'
            try:
                assert current_file_path.exists()
                continue
            except:
                print(f'Fetching data for \'{each}\'...')
                current_contents = repo.git.show(f'{each}:mcbroken.json')
                with open(current_file_path, 'w+') as f:
                    f.write(current_contents)
                if commit_times_json_path.is_file() != True:
                    with open(commit_times_json_path, 'w+') as f:
                        commit_times = dict()
                        commit_times[each] = {}
                        commit_times[each]['commit_time'] = repo.git.show('--no-patch', '--pretty=format:%ct', each)
                        commit_times[each]['time_fixed'] = False
                        commit_times_as_json = json.dumps(commit_times, sort_keys=True, indent=4)
                        f.write(commit_times_as_json)
                else:
                    with open(commit_times_json_path, 'r+') as f:
                        commit_times = json.load(f)
                        commit_times[each] = {}
                        commit_times[each]['commit_time'] = repo.git.show('--no-patch', '--pretty=format:%ct', each)
                        commit_times[each]['time_fixed'] = False
                        commit_times[each]['processed'] = False
                        f.seek(0)
                        commit_times_as_json = json.dumps(commit_times, sort_keys=True, indent=4)
                        f.write(commit_times_as_json)

    def fix_times(self):
        commit_times_json_path = self.dump_dir / '000_COMMIT_TIMES.json'
        with open(commit_times_json_path, 'r+') as commit_times:
            commit_times_dict = json.load(commit_times)
            for each_commit in list(commit_times_dict.keys()):
                if commit_times_dict[each_commit]['time_fixed'] is not True:
                    print(f'Adjusting \'last_checked\' values for {each_commit}.json...')
                    current_mcbroken_json = self.dump_dir / (str(each_commit)+'.json')
                    with open(current_mcbroken_json, 'r+') as f:
                        current_mcbroken_contents = json.load(f)
                        for each_entry in current_mcbroken_contents:
                            if isinstance(each_entry['properties']['last_checked'], int):
                                continue
                            else:
                                value_to_subtract_in_seconds = int(each_entry['properties']['last_checked'].strip(' Checkedminutesago')) * int(60)
                                each_entry['properties']['last_checked'] = int(commit_times_dict[each_commit]['commit_time']) - value_to_subtract_in_seconds
                                commit_times_dict[each_commit]['time_fixed'] = True
                        f.seek(0)
                        new_mcbroken_contents = json.dumps(current_mcbroken_contents, sort_keys=True, indent=4)
                        f.write(new_mcbroken_contents)
                    commit_times_dict[each_commit]['time_fixed'] = True
                    continue
            commit_times.seek(0)
            commit_times_as_json = json.dumps(commit_times_dict, sort_keys=True, indent=4)
            commit_times.write(commit_times_as_json)




        


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
    mcbroken_data.extract()
    mcbroken_data.fix_times()
