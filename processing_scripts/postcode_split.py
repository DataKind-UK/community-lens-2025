import csv
import json
import numpy as np

postcodeFile = 'data/ONSPD_MAY_2025/Data/ONSPD_MAY_2025_UK.csv'
imdFile = 'data/Indices_of_Deprivation-2025-data_download-file-postcode_join copy.csv'

postcode_imd = {}
processedData = {}

#build a look of the IMD based on LSOA value
t_imd_num = 0

print('Building IMD lookup')
with open(imdFile, newline='') as csvfile:
    csvreader = csv.reader(csvfile, delimiter=',', quotechar='"')
    next(csvreader) # skip the header row
    for row in csvreader:
        # # {postcode: [LAD code (2024), LAD name (2024),IMD Rank, IMD Decile, Income Decile, Employment Decile, Education, Skills and Training Decile, Health Deprivation and Disability Decile, Crime Decile, Barriers to Housing and Services Decile, Living Environment Decile]}
        # postcode_imd[row[0]] = [row[2],row[4],row[5],row[15],row[16],row[17],row[18],row[19],row[20],row[21],row[22]]

        # {postcode: [LAD code (2024), LAD name (2024),IMD Rank, Income Rank, Employment Rank, Education, Skills and Training Rank, Health Deprivation and Disability Rank, Crime Rank, Barriers to Housing and Services Rank, Living Environment Rank]}
        postcode_imd[row[0]] = [row[2],row[4],row[5],row[6],row[7],row[8],row[9],row[10],row[11],row[12]]

with open('data/intermediate/imd_postcode.json', 'w') as outfile:
    json.dump(postcode_imd, outfile)

#process each postcode in csv and join IMD data

print('Processing Postcodes')
with open(postcodeFile, newline='') as csvfile:
    csvreader = csv.reader(csvfile, delimiter=',', quotechar='"')
    next(csvreader) # skip the header row

    for row in csvreader:

        #get the start of the postcode (3 or 4 character before space) and lsoa value for join

        #check if in England or not
        if(row[43] not in ['N99999999','S99999999','W99999999','stp','','L99999999','M99999999', np.nan]):
            postcodeStart = row[2].split(' ')[0] + row[2].split(' ')[1][0]
            lsoa = row[34]

            #add postcodestart to processed data if not already there
            if postcodeStart not in processedData:
                processedData[postcodeStart] = {}

            # clean postcodes by removing spaces - QUESTION - SHOULD THIS BE THE SAME VERSION OF POSTCODE?
            fullCode = row[0].replace(' ','')

            #retrieve IMD data for the postcode
            imd_postcode_data = postcode_imd[fullCode]

            processedData[postcodeStart][fullCode] = [row[41],row[42],row[8]] + imd_postcode_data
                    

print('Saving New Files')
#save one files for each postcode start value
for key in processedData:
    file = key+'.json'
    print(file)
    with open('../processed_data/'+file, 'w') as outfile:
        json.dump(processedData[key], outfile)

