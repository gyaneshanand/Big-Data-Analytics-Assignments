#!/usr/bin/env python
# coding: utf-8

# # Assignment 1
# 

# ### Data

# INFO, 2017-03-22T20:11:49+00:00, ghtorrent-31 -- ghtorrent.rb: Added pullreq_commit 244eeac28bf419642d5d5c3b388bd2999c8c72e6 to tgstation/tgstation -> 25341
# 
# DEBUG, 2017-03-23T11:15:14+00:00, ghtorrent-30 -- retriever.rb: Commit mzvast/FlappyFrog -> 80bf5c5fde7be6274a2721422f4d9a773583f73c exists
# 
# DEBUG, 2017-03-22T20:15:48+00:00, ghtorrent-35 -- ghtorrent.rb: Parent af8451e16e077f7c6cae3f98bf43bffaca562f88 for commit 2ef393531a3cfbecc69f17d2cedcc95662fae1e6 exists

# In[ ]:


import psycopg2


# In[ ]:


with open('ghtorrent-logs.txt') as f:
    c=0
    for line in f:
        c+=1
    print(c)


# Total No. of records in file = **9,669,788**

# In[ ]:


def parse(line):
    line = line.replace(' -- ', ', ')
    line = line.replace('.rb: ', ', ')
    line = line.replace(', ghtorrent-', ', ')
    return line.split(', ', 4)


# ### Connecting to DataBase

# In[ ]:


conn = psycopg2.connect(database="assignment1_bda", user = "postgres", password = "12345678", host = "127.0.0.1", port = "5432")
cur = conn.cursor()


# In[ ]:




cur.execute('''CREATE TABLE Log
      (id BIGINT primary key NOT NULL,
      logging_level TEXT NOT NULL,
      timestamp TIMESTAMP NOT NULL,
      downloader_id INT NOT NULL,
      retrieval_stage TEXT NOT NULL,
      descreption TEXT NOT NULL);''')
conn.commit()


# In[ ]:


with open('ghtorrent-logs.txt') as f:
    c=0
    length = 0
    for line in f:
        desc = parse(line)
        c+=1
        try:
            # getting data contained in the line
            logging_level = desc[0]
            timestamp = desc[1].replace('T',' ')
            downloader_id = desc[2]
            retrieval_stage = desc[3]
            descreption = desc[4]
            #Inserting data in database
            values = str(str(c)+","+'\''+str(desc[0])+'\'' + "," +'\'' + desc[1] + '\'' + "," + desc[2] + "," + '\'' + desc[3] + '\'' + "," + '\'' + desc[4] + '\'')
            query = "INSERT INTO Log (id,logging_level,timestamp,downloader_id,retrieval_stage,descreption) VALUES (" + values +");"
            cur.execute(query);
            length += 1
            print(c)
        except:
            conn.commit()
            conn = psycopg2.connect(database="assignment1_bda", user = "postgres", password = "12345678", host = "127.0.0.1", port = "5432")
            cur = conn.cursor()
            print("WRONG QUERY :-",line)
            c-=1
        if length%100000 == 0:
            conn.commit()
            conn = psycopg2.connect(database="assignment1_bda", user = "postgres", password = "12345678", host = "127.0.0.1", port = "5432")
            cur = conn.cursor()
    conn.commit()


# In[ ]:




