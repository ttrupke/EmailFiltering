# coding: utf-8

import pandas as pd
import csv
import os

# only need this part if you have not already created a dataframe
# columns = {'spam':[],'non-spam':[],'quarantined':[],'domain-blacklist':[],'domain-whitelist':[],'sender-whitelist':[],
#           'invalid-recipient':[],'virus':[],'sender-blacklist':[],'bogus-mx':[],'host-blacklist':[],'bad-ext':[],
#          'host-whitelist':[],'datekey':[]}
# df = pd.DataFrame(columns)

# read in current .csv file
df = pd.read_csv('', index_col=0)

# get list of all files and the archives
listdir = os.listdir('')
archivelist = os.listdir('')

#iterate over each file in the directory, get the datekey from the name of the file
for object in listdir:
    openfilepath = '' + object
    datekeyval = object[:8]
    filematch = datekeyval + 'test.csv'
    found = 0
    
    #if the file is in the archives it shouldn't be added to the dataframe
    for object in archivelist:
        if filematch == object:
            found = 1
    if found==1:
        print('records for', datekeyval, 'already exist')
    else:
        print('records for', datekeyval, 'do not exist')
        
        # if it's not in the archives, open the file and write only the relevant rows to a separate csv in the archives
        with open(openfilepath, 'r') as csvfile:
            reader = csv.reader(csvfile)
            i=0
            newfilepath = '' + datekeyval + 'test.csv'
            with open(newfilepath, 'w') as newfile:
                writer = csv.writer(newfile)
                for row in reader:
                    if i > 7:
                        writer.writerow(row)
                    i=i+1
                writer.writerow(['datekey', datekeyval])    

        # read the new csv into pandas dataframe
        df2 = pd.read_csv(newfilepath)

        # put the datekey into the master dataframe to create the new entry row
        numrows = len(df2)
        numentries = len(df)
        df.at[numentries,'datekey'] = datekeyval

        # match the correct email types and insert values into master dataframe
        for i in range(numrows):
            colname = df2.iat[i,0]
            for j in range(14):
                if colname==df.columns[j]:
                    df.iat[numentries,j]=df2.iat[i,1]


# write to master output file
df.to_csv('')









