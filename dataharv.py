from googleapiclient.discovery import build
import pymongo
import psycopg2
import pandas as pd
import streamlit as st


#API key connection
def API_connect():
    api_id = 'AIzaSyAU_VBuGfakF0EC-Q_8opS-NVO3s_vZxuE'
    
    api_service_name = 'youtube'
    api_version = 'v3'
    youtube = build(api_service_name,api_version,developerKey=api_id)    
    
    return youtube

youtube = API_connect()

    

#to get channel information(channel id's)
def channel_info(channel_id):
    request = youtube.channels().list(
            
        part = 'snippet,ContentDetails,statistics',
        id=channel_id
        
    )
    response = request.execute()

    for i in response['items']:
        data=dict(channel_name=i['snippet']['title'],
                channel_id=i['id'],
                channel_sub=i['statistics']['subscriberCount'],
                channel_views=i['statistics']['viewCount'],
                total_videos=i['statistics']['videoCount'],
                channel_desc=i['snippet']['description'],
                Playlist_id=i['contentDetails']['relatedPlaylists']['uploads'])
    
    return data
    
    
#to get video id'sget_video_ids(channel_id)
def get_video_ids(channel_id):
    Video_ids = []
    response = youtube.channels().list(id=channel_id,
                                    part='contentDetails').execute()

    next_page_token = None

    playlist_id=response['items'][0]['contentDetails']['relatedPlaylists']['uploads']

    while True:
        response1 = youtube.playlistItems().list(
                                part='snippet',
                                playlistId=playlist_id,
                                maxResults=50,pageToken=next_page_token).execute()

        for i in range(len(response1['items'])):
            Video_ids.append(response1['items'][i]['snippet']['resourceId']['videoId'])
        next_page_token=response1.get('nextPageToken')
        
        if next_page_token is None:
            break
        
    return Video_ids


def get_video_info(video_ids):
    video_data=[]
    for video_id in video_ids:
        request = youtube.videos().list(
            part = 'snippet,contentDetails,statistics',
            id=video_id
        )
        response = request.execute()
        
        for item in response['items']:
            data=dict(Channel_Name=item['snippet']['channelTitle'],
                    Channel_Id=item['snippet']['channelId'],
                    Video_id=item['id'],
                    Title=item['snippet']['title'],
                    Tags=item['snippet'].get('tags'),
                    Thumbnail=item['snippet']['thumbnails']['default']['url'],
                    Description=item['snippet'].get('description'),
                    Published_Date=item['snippet']['publishedAt'],
                    Duration=item['contentDetails']['duration'],
                    Views=item['statistics'].get('viewCount'),
                    Likes=item['statistics'].get('likeCount'),
                    Comments=item['statistics'].get('commentCount'),
                    Favorite_count=item['statistics']['favoriteCount'],
                    Definition=item['contentDetails']['definition'],
                    Caption_status=item['contentDetails']['caption']
                    )
            video_data.append(data)
        
    return video_data
                                        
                                        

def get_comment_info(video_ids):
    Comment_data=[]
    try:
        for video_id in video_ids:
            request = youtube.commentThreads().list(
                part='snippet',
                videoId=video_id,
                maxResults=50,
                
            )
            response = request.execute()

            for item in response['items']:
                data = dict(Comment_Id=item['snippet']['topLevelComment']['id'],
                            Video_Id=item['snippet']['topLevelComment']['snippet']['videoId'],
                            Comment_Text=item['snippet']['topLevelComment']['snippet']['textDisplay'],
                            Comment_Author=item['snippet']['topLevelComment']['snippet'][ 'authorDisplayName'],
                            Comment_Published=item['snippet']['topLevelComment']['snippet']['publishedAt'])
                
                Comment_data.append(data)
    except:
        pass
    
    return Comment_data


client = pymongo.MongoClient("mongodb+srv://keyboard_123:keyboard123@capstone-project.znlk8pw.mongodb.net/?retryWrites=true&w=majority&appName=capstone-project")
db = client["Youtube_data"]


def channel_details(channel_id):
    ch_details = channel_info(channel_id)
    ch_vidids = get_video_ids(channel_id)  # Retrieve video IDs
    ch_vidinfo = get_video_info(ch_vidids)  # Pass video IDs to get_video_info
    ch_comments = get_comment_info(ch_vidids)
    
    coll1 = db["channel_details"]
    coll1.insert_one({
        "channel_information": ch_details,
        "video_information": ch_vidinfo,
        "comment_information": ch_comments
    })
    
    return "Upload successfully completed"



def channels_table():
    mydb = psycopg2.connect(host="localhost",
                            user="postgres",
                            password="Hyperspeed3#",
                            database="youtube_database",
                            port="5432")
    cursor=mydb.cursor()
    
    
    drop_query = '''drop table if exists channels'''
    cursor.execute(drop_query)
    mydb.commit()
    
    try:
        create_query = '''create table if not exists channels(channel_name varchar(100),
                                                              channel_id varchar(80) primary key,
                                                              channel_sub bigint,
                                                              channel_views bigint,
                                                              total_videos int,
                                                              channel_desc text,
                                                              Playlist_id varchar(80))'''
        cursor.execute(create_query)
        mydb.commit()
        
    except:
       print("channels table already created")     
        
    
    ch_list = []
    db = client["Youtube_data"]
    coll1=db["channel_details"]
    for ch_data in coll1.find({},{"_id":0,"channel_information":1}):
        ch_list.append(ch_data["channel_information"])
    df = pd.DataFrame(ch_list)
    
    
    for index,rows in df.iterrows():
        insert_query ='''insert into channels(channel_name,
                                              channel_id,
                                              channel_sub,
                                              channel_views,
                                              total_videos,
                                              channel_desc,
                                              Playlist_id)
                                              
                                              values(%s,%s,%s,%s,%s,%s,%s)'''
                                              
        values = (rows['channel_name'],
                  rows['channel_id'],
                  rows['channel_sub'],
                  rows['channel_views'],
                  rows['total_videos'],
                  rows['channel_desc'],
                  rows['Playlist_id'])
        
        try:
            cursor.execute(insert_query,values)
            mydb.commit()
            
        except:
            print("Channels values are already inserted")                                         
            
            
def videos_table():
      mydb = psycopg2.connect(host="localhost",
                          user="postgres",
                          password="Hyperspeed3#",
                          database="youtube_database",
                          port="5432")
     
      cursor=mydb.cursor()


      drop_query = '''drop table if exists videos'''
      cursor.execute(drop_query)
      mydb.commit()


      create_query = '''create table if not exists videos(Channel_Name varchar(100),
                                                          Channel_Id varchar(100) primary key,
                                                          Video_id varchar(30),
                                                          Title varchar(150),
                                                          Tags text,
                                                          Thumbnail varchar(200),
                                                          Description text,
                                                          Published_Date timestamp,
                                                          Duration interval,
                                                          Views bigint,
                                                          Likes bigint,
                                                          Comments int,
                                                          Favorite_count int,
                                                          Definition varchar(10),
                                                          Caption_status varchar(50)
                                                         )'''

      cursor.execute(create_query)
      mydb.commit()
      
      vid_list = []
      db = client["Youtube_data"]
      coll1=db["channel_details"]
      for vid_data in coll1.find({},{"_id":0,"video_information":1}):
       for i in range(len(vid_data["video_information"])):
        vid_list.append(vid_data["video_information"][i])
       df2 = pd.DataFrame(vid_list)

      for index,rows in df2.iterrows():
        insert_query ='''insert into videos(Channel_Name,
                                              Channel_Id,
                                              Video_id,
                                              Title,
                                              Tags,
                                              Thumbnail,
                                              Description,
                                              Published_Date,
                                              Duration,
                                              Views,
                                              Likes,
                                              Comments,
                                              Favorite_count,
                                              Definition,
                                              Caption_status
                                       )
                                           
                                       values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''
                                                  
      values = (rows['Channel_Name'],
                rows['Channel_Id'],
                rows['Video_id'],
                rows['Title'],
                rows['Tags'],
                rows['Thumbnail'],
                rows['Description'],
                rows['Published_Date'],
                rows['Duration'],
                rows['Views'],
                rows['Likes'],
                rows['Comments'],
                rows['Favorite_count'],
                rows['Definition'],
                rows['Caption_status']
              )


      cursor.execute(insert_query,values)
      mydb.commit()
      
      
def comments_table():
      mydb = psycopg2.connect(host="localhost",
                          user="postgres",
                          password="Hyperspeed3#",
                          database="youtube_database",
                          port="5432")
      cursor=mydb.cursor()
 

      drop_query = '''drop table if exists comments'''
      cursor.execute(drop_query)
      mydb.commit()


      create_query = '''create table if not exists comments(Comment_Id varchar(100),
                                                        Video_Id varchar(50),
                                                        Comment_Text text,
                                                        Comment_Author varchar(150),
                                                        Comment_Published timestamp
                                                      )'''

      cursor.execute(create_query)
      mydb.commit()


      com_list = []
      db = client["Youtube_data"]
      coll1=db["channel_details"]
      for com_data in coll1.find({},{"_id":0,"comment_information":1}):
        for i in range(len(com_data["comment_information"])):
           com_list.append(com_data["comment_information"][i])
      df2 = pd.DataFrame(com_list)

      for index,rows in df2.iterrows():
         insert_query ='''insert into comments(Comment_Id,
                                          Video_Id,
                                          Comment_Text,
                                          Comment_Author,
                                          Comment_Published 
                                      )
                                          
                                      values(%s,%s,%s,%s,%s)'''
                                                  
      values = (rows['Comment_Id'],
              rows['Video_Id'],
              rows['Comment_Text'],
              rows['Comment_Author'],
              rows['Comment_Published']
              )


      cursor.execute(insert_query,values)
      mydb.commit()
      
      
def tables():
    channels_table()
    videos_table()
    comments_table()
    
    return "Tables are created sucessfully"


def show_channels_table():
    ch_list = []
    db = client["Youtube_data"]
    coll1=db["channel_details"]
    for ch_data in coll1.find({},{"_id":0,"channel_information":1}):
        ch_list.append(ch_data["channel_information"])
    df = st.dataframe(ch_list)
    
    return df


def show_videos_table():
    vid_list = []
    db = client["Youtube_data"]
    coll1=db["channel_details"]
    for vid_data in coll1.find({},{"_id":0,"video_information":1}):
        for i in range(len(vid_data["video_information"])):
            vid_list.append(vid_data["video_information"][i])
    df1 = st.dataframe(vid_list)
    
    return df1


def show_comments_table():
    com_list = []
    db = client["Youtube_data"]
    coll1=db["channel_details"]
    for com_data in coll1.find({},{"_id":0,"comment_information":1}):
        for i in range(len(com_data["comment_information"])):
          com_list.append(com_data["comment_information"][i])
    df2 = st.dataframe(com_list)
    
    return df2


#streamlit part 

with st.sidebar:
   st.title(":red[YOUTUBE DATA HARVESTING AND WAREHOUSING]")
   st.header("Skills Implied")
   st.caption("Python Scripting")
   st.caption("Data Collection")
   st.caption("MongoDB")
   st.caption("API Integration")
   st.caption("Data Managing using MongoDB and SQL")
   
   
channel_id = st.text_input("Enter the channel ID")

if st.button("collect and store data"):
    ch_ids=[]
    db=client["Youtube_data"]
    coll1=db["channel_details"]
    for ch_data in coll1.find({},{'_id':0,'channel_information':1}):
        ch_ids.append(ch_data["channel_information"]["channel_id"])
        
    if channel_id in ch_ids:
        st.success("Channel details for given channel id exists")
        
    else:
        insert = channel_details(channel_id)
        st.success(insert)
        
if st.button("migrate to SQL"):
    Table=tables()
    st.success(Table)
    
show_table = st.radio('select the table for view',("CHANNELS","VIDEOS","COMMENTS"))

if show_table == 'CHANNELS':
    show_channels_table()
    
elif show_table == "VIDEOS":
    show_videos_table()

elif show_table == "COMMENTS":
    show_comments_table()    
        
    
#SQL CONNECTION 
mydb = psycopg2.connect(host="localhost",
                        user="postgres",
                        password="Hyperspeed3#",
                        database="youtube_database",
                        port="5432")
cursor=mydb.cursor()

question = st.selectbox("select1 your question",("1. All the videos and channel name",
                                                 "2. channels with most number of videos",
                                                 "3. 10 most viewed videos",
                                                 "4. Comments in each videos",
                                                 "5. Videos with highest likes",
                                                 "6. Likes of all videos",
                                                 "7. Views of each channels",
                                                 "8. Videos publishes in the year of 2022",
                                                 "9. Average duration of all videos in each channels",
                                                 "10. Videos with highest number of comments"))

if question=="1. All the videos and channel name":
    query1 = '''select title as videos,channel_name as channelname from videos'''
    cursor.execute(query1)
    mydb.commit()
    t1 = cursor.fetchall()
    df = pd.DataFrame(t1,columns=["video title","channel name"])
    st.write(df)

    
elif question=="2. channels with most number of videos":
    query2 = '''select channel_name as channelname,total_videos as no_videos from channels order by total_videos desc'''
    cursor.execute(query2)
    mydb.commit()
    t2 = cursor.fetchall()
    df1 = pd.DataFrame(t2,columns=["channel name","Number of videos"])
    st.write(df1)


elif question=="3. 10 most viewed videos":
    query3 = '''select views as views,channel_name as channelname,title as videotitle from videos where videos is not null order by views desc limit 10'''
    cursor.execute(query3)
    mydb.commit()
    t3 = cursor.fetchall()
    df2 = pd.DataFrame(t3,columns=["videos","channel name","video title"])
    st.write(df2)
    
    
elif question=="4. Comments in each videos":
    query4 = '''select comments as no_comments,title as video_title from videos where comments is not null'''
    cursor.execute(query4)
    mydb.commit()
    t4 = cursor.fetchall()
    df3 = pd.DataFrame(t4,columns=["Number of comments","video title"])
    st.write(df3)
    
    
elif question=="5. Videos with highest likes":
    query5 = '''select title as video_title,channel_name as channel_name,likes as likecount from videos where likes is not null order by likes desc'''
    cursor.execute(query5)
    mydb.commit()
    t5 = cursor.fetchall()
    df4 = pd.DataFrame(t5,columns=["Video title","channel name","like count"])
    st.write(df4)
    
    
elif question=="6. Likes of all videos":
    query6 = '''select likes as likecount,title as videotitle from videos'''
    cursor.execute(query6)
    mydb.commit()
    t6 = cursor.fetchall()
    df5 = pd.DataFrame(t6,columns=["Like Count","Video Title"])
    st.write(df5)
    
    
elif question=="7. Views of each channels":
    query7 = '''select channel_name as channelname ,channel_views as totalviews from channels'''
    cursor.execute(query7)
    mydb.commit()
    t7 = cursor.fetchall()
    df6 = pd.DataFrame(t7,columns=["Channel Name","Total Views"])
    st.write(df6)
    
    
elif question=="8. Videos published in the year of 2022":
    query8 = '''select title as video_title,published_date as videorelease,channel_name as channelname from videos where extract(year from published_date)=2022'''
    cursor.execute(query8)
    mydb.commit()
    t8 = cursor.fetchall()
    df7 = pd.DataFrame(t8,columns=["Video title","Published date","Channel Name"])
    st.write(df7)
    
    
elif question=="9. Average duration of all videos in each channels":
    query9 = '''select channel_name as channelname,AVG(duration) as averageduration from videos group by channel_name'''
    cursor.execute(query9)
    mydb.commit()
    t9 = cursor.fetchall()
    df8 = pd.DataFrame(t9,columns=["Channel Name","Average Duration"])

    T9 = []
    for index,rows in df8.iterrows():
        channel_title=rows["Channel Name"]
        average_duration=rows["Average Duration"]
        average_duration_str = str(average_duration)
        T9.append(dict(channeltitle=channel_title,avgduration=average_duration_str))
    df0 = pd.DataFrame(T9)
    st.write(df0)
    
    
elif question=="10. Videos with highest number of comments":
    query10 = '''select title as videotitle,channel_name as channelname,Comments as comments from videos where comments is not null order by comments desc'''
    cursor.execute(query10)
    mydb.commit()
    t10 = cursor.fetchall()
    df9 = pd.DataFrame(t10,columns=["Video Title","Channel Name","Comments"])
    st.write(df9)
    

    

    

    
    


    
