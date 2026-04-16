import csv
import re

def removeNewLine():
    
    csv_in = open('csvDatasets/2020-04-04CoronavirusTweets.csv', newline='')
    csv_out = open('csvDatasetsProcessed/CoronaTweetDataset04-04.csv', mode='w')

    writer = csv.writer(csv_out)

    for row in csv.reader(csv_in):
        row[4] = re.sub('\n', ' ', row[4])
        row[4] = re.sub(',', '.', row[4])
        writer.writerow(row)
    print('Done')

removeNewLine()