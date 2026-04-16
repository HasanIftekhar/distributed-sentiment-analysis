#!/usr/bin/python
import csv
import json
import re
import os
from io import StringIO

from mrjob.job import MRJob
import mrjob

class project(MRJob):

    OUTPUT_PROTOCOL = mrjob.protocol.RawValueProtocol
    
    def mapper(self, _, tweet):
    
            row = tweet.split(',')

            created_at = row[2]
            text = row[4]
            favourites_count = row[11]
            retweet_count = row[12]
        
            yield None, f"{created_at}, {text}, {favourites_count}, {retweet_count}"
        

    '''
    def reducer(self, key, values):
        yield None, values
    '''

if __name__ == '__main__':
    project.run()