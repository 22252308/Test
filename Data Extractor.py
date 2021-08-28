import pandas as pd
import numpy as np
import requests
from datetime import date, timedelta
import os
import re
import tarfile
import zipfile
import bz2
import glob
import logging
from unittest.mock import patch
from typing import List, Set, Dict, Tuple, Optional
from itertools import zip_longest
import plotly.express as px

import betfairlightweight
from betfairlightweight import StreamListener
from betfairlightweight.resources.bettingresources import (
    PriceSize,
    MarketBook
)

# %config IPCompleter.greedy=True

# Current wd
print("Current Working Directory " , os.getcwd())

try:
    # Change the current working Directory    
    os.chdir("D:\\test")
    print("Directory changed")
    print("Current Working Directory " , os.getcwd())
except OSError:
    print("Can't change the Current Working Directory")

# Log in and create listener
trading = betfairlightweight.APIClient("username", "password")
listener = StreamListener(max_latency=None)

def load_markets(file_paths: List[str]):
    # loops over each of the strings in the list
    for file_path in file_paths:
        # Prints each of the stirngs in the list
        print(file_path)
        # If the string exists then
        if os.path.isdir(file_path):
            # https://docs.python.org/3/library/glob.html
            # Return a of path names that match pathname
            # glob.iglob(pathname, *, recursive=False)
            # If recursive is true, the pattern “**” will match any files and zero or more directories, subdirectories and symbolic links to directories.
            # basically it finds all files in the path that ends with .bz2 within the file path and returns it
            for path in glob.iglob(file_path + '**/**/*.bz2', recursive=True):
                f = bz2.BZ2File(path, 'rb')
                yield f
                f.close()
        # if the string path doesn't exist, check if it is a file
        elif os.path.isfile(file_path):
            # if it is a file then takes its path go up one step and then basically iterate throught everything in that upper file
            ext = os.path.splitext(file_path)[1]
            # iterate through a tar archive
            if ext == '.tar':
                with tarfile.TarFile(file_path) as archive:
                    for file in archive:
                        yield bz2.open(archive.extractfile(file))
            # or a zip archive
            elif ext == '.zip':
                with zipfile.ZipFile(file_path) as archive:
                    for file in archive.namelist():
                        yield bz2.open(archive.open(file))

    return None

def ladder_traded_volume(ladder):
    return(sum([rung.size for rung in ladder]))

market_path = ['This is a test file']

def test(market_path):
    for file_obj in load_markets(market_path):
        # Create trading.streaming but this time we are using .create_historical_generator_stream
        # Assuming this is the version for treading.streaming but for historical data
        # Instantiate a "stream" object
        stream = trading.streaming.create_historical_generator_stream(
            file_path=file_obj,
            listener=listener)
        print(stream)

        with patch("builtins.open", lambda f, _: f):   

            # Create generator
            gen = stream.get_generator()

            # For each group of market books in the generator
            for market_books in gen():
                # For each individual market book in the group of market books
                for market_book in market_books:
                    seconds_to_start = (market_book.market_definition.market_time - market_book.publish_time).total_seconds()
                    if (seconds_to_start < 0 or seconds_to_start > 300):
                        continue
                    market_id = market_book.market_id
                    for runner in market_book.runners:
                        lst = []
                        selection_id = runner.selection_id
                        selection_name = next((rd.name for rd in market_book.market_definition.runners if rd.selection_id == runner.selection_id), None)
                        selection_status = runner.status
                #         sp = runner.sp.actual_sp This seems useless

                        selection_traded_volume = ladder_traded_volume(runner.ex.traded_volume)
                        try:
                            # atb_ladder_3m = [top_3_ladder(runner.ex.available_to_back)]
                            # atl_ladder_3m = [top_3_ladder(runner.ex.available_to_lay)]
                            best_back_price = runner.ex.available_to_back[0].price
                            best_back_size = runner.ex.available_to_back[0].size
                            best_lay_price = runner.ex.available_to_lay[0].price
                            best_lay_size = runner.ex.available_to_lay[0].size
                            # best_back = [runner.ex.available_to_back[0].price,runner.ex.available_to_back[0].volume]
                            # best_lay = [runner.ex.available_to_lay[0]]
                            # selection_traded_volume = runner.ex.traded_volume
                        except:
                            best_back_price = []
                            best_back_size = []
                            best_lay_price = []
                            best_lay_size = []

                        # lastPriceTraded: The price of the most recent bet matched on this selection    
                        try:
                            last_price_traded = runner.last_price_traded
                        except:
                            last_price_traded = None

                        yield [market_id,seconds_to_start,selection_id,selection_name,selection_status,best_back_price, best_back_size,best_lay_price,best_lay_size,selection_traded_volume,last_price_traded]

df = test(market_path)
df = list(df)
df = pd.DataFrame(list(test(market_path)), columns = ['market_id','seconds_to_start','selection_id','selection_name','selection_status','best_back_price', 'best_back_volume','best_lay_price','best_lay_size','selection_traded_volume','last_price_traded'])
df.to_csv('test.csv')
df.head()
# print('This is a test')


df = pd.read_csv('test.csv')
df.head()
