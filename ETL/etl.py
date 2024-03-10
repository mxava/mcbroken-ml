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

# sample mcbroken.json file
FILE_PATH = Path.cwd() / 'ETL' / 'mcbroken.json'

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
    #extract_mcbroken_archive(repo_path, dump_dir)


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

            




#def generate_dump_data(repo_path: Path=Path.cwd(),
#                      dump_dir: Path=None
#                      ) -> int:
#    """Stores archive dump to dataframe
#
#    Args:
#        repo_path (Path, optional): Path for archive repo. Defaults to Path.cwd().
#        dump_dir (Path, optional): Path for dump directory. Defaults to (Path.cwd() / 'DATA_DUMP').
#
#    Returns:
#        None
#    """
#    repo = git.Repo(repo_path)
#    if dump_dir == None:
#        dump_dir = repo_path / 'DATA_DUMP'
#    dump_parquet = dump_dir / 'dump.parquet'
#    df_schema = StructType([
#            StructField('commit_hash', StringType(), True),
#            StructField('extracted', StringType(), True),
#            StructField('transformed', StringType(), True),
#            StructField('loaded', StringType(), True),
#            StructField('git_commit_time', StringType(), True),
#            StructField('contents', StringType(), True)
#        ])
#    #check if file can be loaded
#    if dump_parquet.is_file():
#        df = spark_mb.read.load(str(dump_parquet))
#    #otherwise, create empty:
#    else:
#        empty_RDD = spark_mb.sparkContext.emptyRDD()
#        df = spark_mb.createDataFrame(data = empty_RDD,
#                                schema = df_schema)
#    commit_list = repo.git.log('--pretty=%h', '--all', '--', './mcbroken.json')
#    #convert string output to list
#    commit_list = commit_list.split('\n')
#    df_temp = df.toPandas()
#    df_temp.set_index('commit_hash')
#    for each in commit_list:
#        if each in df_temp.index:
#            continue
#        else:
#            print(f'Processing \'{each}\'...')
#            current_commit_time = repo.git.show('--no-patch', '--pretty=format:%ct', each)
#            current_contents = repo.git.show(f'{each}:mcbroken.json')
#            df_temp._append([[each, 'True', 'False', 'False', current_commit_time, current_contents]])
#    df = spark_mb.createDataFrame(data = df_temp,
#                                  schema = df_schema)
#    print(f'Done processing! Writing to \'{str(dump_parquet)}...\'')
#    df.write.mode('overwrite').format('parquet').save(str(dump_parquet))

#def extract_mcbroken_archive(repo_path: Path=Path.cwd(),
#                             data_dump: Path=(Path.cwd() / 'DATA_DUMP')
#                             ) -> None:
#    repo = git.Repo(repo_path)
#    dump_json = data_dump / 'dump.json'
#    with open(dump_json, 'r+') as f:
        # TODO: use pd instead of json to see if that fixes memory leak
        # TODO: possibly flatten json a bit
#        df = spark_mb.read
#        df.show()
        #temp_file = data_dump / 'TEMP.json'
        #k = data.keys()
        #for each in iter(k):
        #    if data[each]['properties']['extracted_flag'] == 'True':
        #        continue
        #    elif data[each]['properties']['extracted_flag'] == 'True':
        #        continue
        #    elif data[each]['contents']:
        #        continue
        #    else:
        #        print(f'Extracting data for commit \'{each}\'...')
    
    #with open(dump_json, "r+b") as f:
    #    for each in ijson.items(f, "item"):
    #        each['contents'] = repo.git.show(f'{each}:mcbroken.json')
    #        each['properties']['extracted_flag'] = 'True'  
            #with open(temp_file, 'w+') as f:
            #    temp_data = dict()
            #    temp_data[each] = data[each]
            #    temp_data[each]['contents'] = repo.git.show(f'{each}:mcbroken.json')
            #    f.write(json.dumps(temp_data))
#        return None
                

    

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
    computer_on_fire = do_the_thing(mcbroken_archive_repo_path)
