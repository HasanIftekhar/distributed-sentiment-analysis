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

            tweet = tweet.strip()
            regex = r'"created_at":"(?P<created_at>[^"]+)".*?"text":"(?P<text>[^"]+)".*?"favourites_count":(?P<favourites_count>\d+).*?"quote_count":(?P<quote_count>\d+).*?"reply_count":(?P<reply_count>\d+).*?"retweet_count":(?P<retweet_count>\d+)'

            match = re.search(regex, tweet)
                
            if match:
                created_at = match.group('created_at')
                text = match.group('text')
                text = text.replace(',', '.')
                favourites_count = int(match.group('favourites_count'))
                quote_count = int(match.group('quote_count'))
                reply_count = int(match.group('reply_count'))
                retweet_count = int(match.group('retweet_count'))
            else:
                return

            yield None, f"{created_at}, {text}, {favourites_count}, {quote_count}, {reply_count}, {retweet_count}"
        

    '''
    def reducer(self, values, _):
        yield None, values
    '''

if __name__ == '__main__':
    project.run()