### THAR BE DRAGONS AFOOT.
from pathlib import Path
import os
from pyspark.sql import SparkSession, DataFrameReader
from pyspark.sql.functions import col, min, max
import pandas as pd
import json
import sqlalchemy
import git

# sample mcbroken.json file
FILE_PATH = Path.cwd() / 'ETL' / 'mcbroken.json'

# extract mcbroken.json archives
MCBROKEN_ARCHIVE_REPO_PATH = Path('../mcbroken-archive')
os.chdir(MCBROKEN_ARCHIVE_REPO_PATH)
MCBROKEN_ARCHIVE_REPO_PATH = Path.cwd()

def jack_the_ripper(repo_path: Path=Path.cwd(),
                    dump_dir: Path=(Path.cwd() / 'DATA_DUMP')
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
    dump_dir.mkdir(parents=True, exist_ok=True)
    generate_dump_json(repo_path, dump_dir)
    print('dump.json update complete.')
    return
                
def generate_dump_json(repo_path: Path=Path.cwd(),
                      dump_dir: Path=(Path.cwd() / 'DATA_DUMP')
                      ) -> int:
    """Generates log of archive dump as a json file.

    Args:
        repo_path (Path, optional): Path for archive repo. Defaults to Path.cwd().
        dump_dir (Path, optional): Path for dump directory. Defaults to (Path.cwd() / 'DATA_DUMP').

    Returns:
        None
    """
    # Saving this for later
    mcbrokenjson_path = repo_path / 'mcbroken.json'
    # Creates a logfile if it doesn't exist
    dump_json_path = dump_dir / 'dump.json'
    #if dump_json_path.is_file() != True:
    #    with open(dump_json_path, 'w') as f:
    #        f.write(json.dump(dict()))
    #        pass
    repo = git.Repo(repo_path)
    # list every commit in which mcbroken.json has been
    # modified in reverse chronological order
    rev_list = repo.git.rev_list('--all', '--objects', '--', mcbrokenjson_path)
    # remove everything in line after space
    rev_list = rev_list.split(' ')[0]
    # create list from string
    rev_list = rev_list.split('\n')
    with open(dump_dir / 'dump.json', 'r+') as dumper:
        # check if file already has data
        data = json.load(dumper)
        # TODO: Fix dumpsterfire
        for each in rev_list:
            print(f'Recording data for \'{each}\'...')
            data[each] = {'properties': {}}
            data[each]['properties']['git_commit_time'] = 'dummy_time_value'
            #repo.git.show(each, mcbrokenjson_path, 's --format="%ci')
            data[each]['properties']['extracted_flag'] = 'False'
            data[each]['properties']['transformed_flag'] = 'False'
        data = json.dumps(data, sort_keys=True, indent=4)
        dumper.write(data)
    print(f'Dump log generation completed at \'{dump_json_path}\'.')
    return None

def extract_mcbroken_archive(repo_path: Path=Path.cwd(),
                             data_dump: Path=(Path.cwd() / 'DATA_DUMP')
                             ) -> None:
    repo = git.Repo(repo_path)
    dump_json = data_dump / 'dump.json'
    with open(dump_json) as dump_processor:
        data = json.load(dump_processor)

        for each in data:
            print(each)
            print(each.keys()[0])
            #print(f'Processing {data[each].keys()}')
            #if each['properties']['extracted_flag'] == False:
            #    with open(each.keys()[0], 'w+') as f:
            #        f.write(repo.commit(each.keys()[0]).tree['mcbroken.json'].data_stream.read())
            return None
                

    

#file_last_modified = time.gmtime(os.path.getmtime(FILE_PATH))

# load json as pandas dataframe
#pd_df = pd.read_json(FILE_PATH)

# initialize spark session
#spark = SparkSession.builder \
#    .appName("Pandas to Spark") \
#    .getOrCreate()

# load pd_df as spark dataframe
#spark_df = spark.createDataFrame(pd_df)

### transform data in spark dataframe

# restructure mcbroken.json data according to sql db schema

# convert mcbroken "last_checked" time to a time stamp


# throw transformed mcbroken data into mcflurry table

# fetch locations of weather data based on location and timestamp



### Scratchpad
if __name__ == '__main__':
    set_cpu_ablaze = jack_the_ripper(MCBROKEN_ARCHIVE_REPO_PATH, 'mcbroken.json')
