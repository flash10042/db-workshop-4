import csv
import re
import os
import time
from pymongo import MongoClient
from config import Config


BATCH_SIZE = 1000


while True:
    try:
        client = MongoClient(Config.MONGO_URI)

        db = client.cp_database
        collection = db.zno_collection

        start_time = time.time()

        for file_name in os.listdir('data'):
            year = re.findall(r'Odata(\d{4})File.csv', file_name)
            if year:
                with open(os.path.join('data', file_name), encoding='cp1251') as csvfile:
                    csv_reader = csv.reader(csvfile, delimiter=';')
                    next(csv_reader)

                    row_num = 0
                    batch = list()

                    res = collection.find_one({'year' : year[0]}, sort=[('rows', -1)])

                    if res:
                        if 'rows' not in res:
                            print(f'File {file_name} has been already processed. Going to next...')
                            continue
                        for i in range(res['rows'] + 1):
                            next(csv_reader)
                            row_num += 1
                    

                    print(f'Start exporting data from {file_name}')
                    for row in csv_reader:
                        # PREPROCESS
                        for i in range(len(row)):
                            if row[i] == 'null':
                                row[i] = None
                            else:
                                if i in Config.NUMERIC_COLS:
                                    row[i] = row[i].replace(',', '.')
                                    row[i] = float(row[i])
                        
                        batch.append(dict(zip(Config.COL_NAMES, [row_num] + year + row)))
                        row_num += 1

                        if not row_num % BATCH_SIZE:
                            collection.insert_many(batch)
                            #helper.update_one({'_id':year[0]}, {'$set': {'rows': row_num}})
                            batch = list()
                    if batch:
                        collection.insert_many(batch)
                        collection.update_many({}, {'$unset': {'rows': 1}})
                        #helper.update_one({'_id':year[0]}, {'$set': {'rows': row_num, 'done': True}})
                        batch = list()

        total = time.time() - start_time
        print(f'Total exec time - {total} secs')
        with open(os.path.join('data', 'time.txt'), 'w') as time_file:
            time_file.write(f'Total exec time - {total} secs')


        with open(os.path.join('data', 'result.csv'), 'w', newline='') as csvfile:
            csv_writer = csv.writer(csvfile, dialect='excel')
            csv_writer.writerow(Config.ANSWER_COLUMNS)
            for year in ['2019', '2020']:
                res = collection.find_one({'physTestStatus': 'Зараховано', 'year': year}, sort=[('physBall100', -1)])
                csv_writer.writerow([year, res['physBall100'], res['physBall12'], res['physBall']])
        print('Select operation finished.')


        break
    except Exception as err:
        print(err)
        time.sleep(5)
