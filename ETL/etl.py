### HERE BE DRAGONS
from pathlib import Path
import os
import pyspark
from pyspark.sql import SparkSession, DataFrameReader
from pyspark.sql.functions import col, min, max
from pyspark.sql.types import *
from pyspark.rdd import *
import pandas as pd
import json
import ijson
import sqlalchemy
import git
#???
os.environ["HADOOP_HOME"] = 'C:\\hadoop'
# sample mcbroken.json file
FILE_PATH = Path.cwd() / 'ETL' / 'mcbroken.json'

# extract mcbroken.json archives
MCBROKEN_ARCHIVE_REPO_PATH = Path('../mcbroken-archive')
os.chdir(MCBROKEN_ARCHIVE_REPO_PATH)
MCBROKEN_ARCHIVE_REPO_PATH = Path.cwd()

spark_mb = SparkSession \
    .builder \
    .appName('mcbroken-archive data') \
    .getOrCreate()
pyspark.SparkContext.setSystemProperty('hadoop.home.dir', 'C:\\hadoop\\bin')

def jack_the_ripper(repo_path: Path = Path.cwd(),
                    dump_dir:  Path = Path('./DATA_DUMP')
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
    generate_dump_data(repo_path, dump_dir)
    #extract_mcbroken_archive(repo_path, dump_dir)


    return
                

def generate_dump_data(repo_path: Path=Path.cwd(),
                      dump_dir: Path=(Path.cwd() / 'DATA_DUMP')
                      ) -> int:
    """Stores archive dump to dataframe

    Args:
        repo_path (Path, optional): Path for archive repo. Defaults to Path.cwd().
        dump_dir (Path, optional): Path for dump directory. Defaults to (Path.cwd() / 'DATA_DUMP').

    Returns:
        None
    """
    repo = git.Repo(repo_path)
    data_dump = dump_dir / 'dump.pkl'
    #check if file can be loaded
    if data_dump.is_file():
        df = spark_mb.SparkContext.pickleFile('DATA_DUMP/dump.pkl')
    #otherwise, create empty:
    else:
        empty_RDD = spark_mb.sparkContext.emptyRDD()
        df_schema = StructType([
            StructField('commit_hash', StringType(), True),
            StructField('extracted', StringType(), True),
            StructField('transformed', StringType(), True),
            StructField('loaded', StringType(), True),
            StructField('git_commit_time', StringType(), True),
            StructField('contents', StringType(), True)
        ])
        df = spark_mb.createDataFrame(data = empty_RDD,
                                schema = df_schema)
    commit_list = repo.git.log('--pretty=%h', '--all', '--', './mcbroken.json')
    #convert string output to list
    commit_list = commit_list.split('\n')
    df = df.toPandas()
    for each in commit_list[0:5]:
        #if df.lookup(each):
        #    continue
        #else:
            print(f'Processing \'{each}\'...')
            current_commit_time = repo.git.show('--no-patch', '--pretty=format:%ct', each)
            current_contents = repo.git.show(f'{each}:mcbroken.json')
            df._append([[each, 'True', 'False', 'False', current_commit_time, current_contents]])
    df = spark_mb.createDataFrame(data = df,
                                  schema = df_schema)
    df.rdd.saveAsPickleFile('DATA_DUMP/dump.pkl')


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
    if dump_json_path.is_file() != True:
        with open(dump_json_path, 'w') as f:
            empty_dict = {}
            f.write(json.dump(empty_dict, 'w+'))
            pass
    repo = git.Repo(repo_path)
    # list every commit in which mcbroken.json has been
    # modified in reverse chronological order
    #rev_list = repo.git.log('--all', '--objects', '--', './mcbroken.json')
    commit_list = repo.git.log('--pretty=%h', '--all', '--', './mcbroken.json')
    #convert string output to list
    commit_list = commit_list.split('\n')
    with open(dump_dir / 'dump.json', 'r+') as dumper:
        #check if file already has data
        try:
            data = json.load(dumper)
        except ValueError:
            print(f'File {dump_json_path} is empty. Generating data - this may take a while!')
            data = dict()
        print('Checking and updating dump.json')
        for each in commit_list:
            if each in data:
               pass
            else:
                data[each] = {
                    'properties': {},
                    'contents': {}
                    }
                print(f'Adding entry for \'{each}\'...')
            if 'properties' in data[each]:
                pass
            else:
                raise ValueError(f'\'{each}\' says, "Properties? In this economy?"')
            if 'contents' in data[each]:
                pass
            else:
                raise ValueError(f'\'{each}\' wants to know if this data is ethically sourced!')
            if 'git_commit_time' in data[each]['properties']:
                pass
            else:
                data[each]['properties']['git_commit_time'] = repo.git.show('--no-patch', '--pretty=format:%ct', each)
            if 'extracted_flag' in data[each]['properties']:
                pass
            else:
                data[each]['properties']['extracted_flag'] = 'False'
            if 'transformed_flag' in data[each]['properties']:
                pass
            else:
                data[each]['properties']['transformed_flag'] = 'False'
        data = json.dumps(data, sort_keys=True, indent=4)
        dumper.seek(0)
        dumper.truncate()
        dumper.write(data)
    print(f'Finished processing \'{dump_json_path}\'.')
    return None

def extract_mcbroken_archive(repo_path: Path=Path.cwd(),
                             data_dump: Path=(Path.cwd() / 'DATA_DUMP')
                             ) -> None:
    repo = git.Repo(repo_path)
    dump_json = data_dump / 'dump.json'
    with open(dump_json, 'r+') as f:
        # TODO: use pd instead of json to see if that fixes memory leak
        # TODO: possibly flatten json a bit
        df = spark.read.json('DATA_DUMP/dump.json')
        df.show()
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
    set_cpu_ablaze = jack_the_ripper(MCBROKEN_ARCHIVE_REPO_PATH)
