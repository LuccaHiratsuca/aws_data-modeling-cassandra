"""Reproduce the notebook's Part I ETL and derive the three query results.

Used to validate the data model without a live Cassandra cluster: each query is
resolved using the same partition-key / clustering-column semantics that the
corresponding Cassandra table enforces.
"""

import csv
import glob
import os

RAW_COLUMNS = """0=artist 1=auth 2=firstName 3=gender 4=itemInSession 5=lastName
6=length 7=level 8=location 9=method 10=page 11=registration
12=sessionId 13=song 14=status 15=ts 16=userId"""

NEW_HEADER = ['artist', 'firstName', 'gender', 'itemInSession', 'lastName',
              'length', 'level', 'location', 'sessionId', 'song', 'userId']


def part_one():
    file_path_list = sorted(glob.glob(os.path.join(os.getcwd(), 'event_data', '*.csv')))
    print(f"event files found: {len(file_path_list)}")

    full_data_rows_list = []
    for f in file_path_list:
        with open(f, 'r', encoding='utf8', newline='') as csvfile:
            csvreader = csv.reader(csvfile)
            next(csvreader)
            for line in csvreader:
                full_data_rows_list.append(line)

    print(f"raw event rows collected: {len(full_data_rows_list)}")
    blanks = [r for r in full_data_rows_list if len(r) < 17]
    print(f"malformed/short rows: {len(blanks)}")

    csv.register_dialect('myDialect', quoting=csv.QUOTE_ALL, skipinitialspace=True)
    written = 0
    with open('event_datafile_new.csv', 'w', encoding='utf8', newline='') as f:
        writer = csv.writer(f, dialect='myDialect')
        writer.writerow(NEW_HEADER)
        for row in full_data_rows_list:
            if len(row) >= 17 and row[10] == 'NextSong':
                writer.writerow((row[0], row[2], row[3], row[4], row[5],
                                 row[6], row[7], row[8], row[12], row[13], row[16]))
                written += 1

    print(f"NextSong rows written to event_datafile_new.csv: {written}")

    # Cross-check against the alternative filter used by the course template.
    alt = sum(1 for r in full_data_rows_list if len(r) >= 17 and r[0] != '')
    print(f"rows under 'artist != empty' filter (course template): {alt}")
    return written


def load_new_file():
    with open('event_datafile_new.csv', 'r', encoding='utf8') as f:
        return list(csv.DictReader(f))


def queries(rows):
    # --- Query 1: song_plays_by_session, PRIMARY KEY (sessionId, itemInSession)
    t1 = {}
    for r in rows:
        t1[(int(r['sessionId']), int(r['itemInSession']))] = (
            r['artist'], r['song'], float(r['length']))
    print("\nQuery 1 -- sessionId=338 AND itemInSession=4")
    print(" ", t1.get((338, 4)))

    # --- Query 2: song_plays_by_user_session, PRIMARY KEY ((userId, sessionId), itemInSession)
    t2 = {}
    for r in rows:
        t2[((int(r['userId']), int(r['sessionId'])), int(r['itemInSession']))] = (
            r['artist'], r['song'], r['firstName'], r['lastName'])
    part = sorted((ck, v) for (pk, ck), v in t2.items() if pk == (10, 182))
    print("\nQuery 2 -- userId=10 AND sessionId=182 (clustering order by itemInSession)")
    for ck, v in part:
        print(f"  itemInSession={ck}: {v}")

    # --- Query 3: users_by_song, PRIMARY KEY (song, userId)
    t3 = {}
    for r in rows:
        t3[(r['song'], int(r['userId']))] = (r['firstName'], r['lastName'])
    target = 'All Hands Against His Own'
    listeners = sorted((uid, v) for (s, uid), v in t3.items() if s == target)
    print(f"\nQuery 3 -- song='{target}' (clustering order by userId)")
    for uid, v in listeners:
        print(f"  userId={uid}: {v}")

    # Show that the upsert on (song, userId) actually deduplicates.
    raw_plays = sum(1 for r in rows if r['song'] == target)
    print(f"  raw play events for this song: {raw_plays} -> distinct listener rows: {len(listeners)}")

    print(f"\nusers_by_song total rows (distinct song+user): {len(t3)}")
    print(f"song_plays_by_session total rows: {len(t1)}")
    print(f"song_plays_by_user_session total rows: {len(t2)}")


if __name__ == '__main__':
    part_one()
    queries(load_new_file())
