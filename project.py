from googleapiclient.discovery import build
import pymongo
import psycopg2
import pandas as pd
import streamlit as st

# Define the api function
def api_connect():
    api_key="AIzaSyCOewWqSvt_LptKkJ9dg8co6HF-8RahQ7c"
    api_service_name = "youtube"
    api_version = "v3"

    youtube=build(api_service_name,api_version,developerKey=api_key)

    return youtube

# Assign the function to the variable youtube
youtube = api_connect()

#channel info

def get_channel_info(channel_id):
    request = youtube.channels().list(
                    part="snippet,ContentDetails,statistics",
                    id=channel_id

        )
    response = request.execute()

    for i in response['items']:
        data=dict(channel_name=i['snippet']['title'],
                channel_id=i['id'],
                subscriber=i['statistics']['subscriberCount'],
                viewers=i['statistics']['viewCount'],
                total_videos=i['statistics']['videoCount'],
                description=i['snippet']['description'],
                playlist=i['contentDetails']['relatedPlaylists']['uploads'])
    return data

def get_videos_id(channel_id):
    video_ids = []
    # Get the uploads playlist ID
    response = youtube.channels().list(
        id=channel_id,
        part='contentDetails'
    ).execute()
    Playlist_ID = response['items'][0]['contentDetails']['relatedPlaylists']['uploads']

    next_page_token = None

    # Loop through all pages of the playlist
    while True:
        response1 = youtube.playlistItems().list(
            part=['snippet'],
            playlistId=Playlist_ID,
            maxResults=50,
            pageToken=next_page_token
        ).execute()

        # Extract video IDs from all items in the current page
        for item in response1['items']:
            video_ids.append(item['snippet']['resourceId']['videoId'])

        # Check if there is another page
        next_page_token = response1.get('nextPageToken')

        if next_page_token is None:
            break

    return video_ids

#get video information
def get_video_info(video_ids):
    video_data=[]
    for video_id in video_ids:
        request=youtube.videos().list(
            part="snippet,ContentDetails,statistics",
            id=video_id
        )
        response=request.execute()
        
        for item in response["items"]:
            data=dict(Channel_Name=item["snippet"]["channelTitle"],
                    Channel_Id=item["snippet"]["channelId"],
                    Video_Id=item["id"],
                    Title=item["snippet"]["title"],
                    Tags=item["snippet"].get("tags"),
                    Thumbnail=item["snippet"]["thumbnails"]["default"]["url"],
                    Description=item["snippet"].get("description"),
                    Published_Date=item["snippet"]["publishedAt"],
                    Duration=item["contentDetails"]["duration"],
                    Views=item["statistics"].get("viewCount"),
                    Likes=item["statistics"].get("likeCount"),
                    Comments=item["statistics"].get("commentCount"),
                    Favorite_Count=item["statistics"]["favoriteCount"],
                    Definition=item["contentDetails"]["definition"],
                    Caption_Status=item["contentDetails"]["caption"],
                    )
            video_data.append(data)
    return video_data

#comment details
def get_comment_info(video_ids):
    Comment_data=[]
    try:
        for video_id in video_ids:
            request=youtube.commentThreads().list(
                part="snippet",
                videoId=video_id,
                maxResults=50,
            )
            response=request.execute()

            for item in response["items"]:
                data=dict(Comment_Id=item["snippet"]["topLevelComment"]["id"],
                        Video_Id=item["snippet"]["topLevelComment"]["snippet"]["videoId"],
                        Comment_Text=item["snippet"]["topLevelComment"]["snippet"]["textDisplay"],
                        Comment_Author=item["snippet"]["topLevelComment"]["snippet"]["authorDisplayName"],
                        Comment_Published=item["snippet"]["topLevelComment"]["snippet"]["publishedAt"])
                
                Comment_data.append(data)
    except:
        pass
    return Comment_data

#Playlist details

def get_playlist_details(channel_id):

        next_page_token=None
        All_data=[]
        while True:
                request=youtube.playlists().list(
                        part="snippet,contentDetails",
                        channelId=channel_id,
                        maxResults=50
                )
                response=request.execute()

                for item in response["items"]:
                        data=dict(Playlist_Id=item["id"],
                                Playlist_Title=item["snippet"]["title"],
                                Channel_Id=item["snippet"]["channelId"],
                                Channel_Name=item["snippet"]["channelTitle"],
                                PublishedAt=item["snippet"]["publishedAt"],
                                Video_count=item["contentDetails"]["itemCount"])
                        All_data.append(data)

                next_page_token=response.get("nextPageToken")
                if next_page_token is None:
                        break

        return All_data

#Upload to MongoDB

client=pymongo.MongoClient("mongodb+srv://nirmal163308:1234@cluster0.ssvad5f.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0")
db=client["Youtube_data"]

def channel_details(channel_id):
    ch_details=get_channel_info(channel_id)
    pl_details=get_playlist_details(channel_id)
    vi_ids=get_videos_id(channel_id)
    vi_details=get_video_info(vi_ids)
    com_details=get_comment_info(vi_ids)

    coll1=db["channel_details"]
    coll1.insert_one({"channel_information":ch_details,"playlist_information":pl_details,
                      "video_details":vi_details,"comment_details":com_details})
    
    return "upload completed sucessfully"

#Table creation
def channels_table(channel_name_s):
    mydb=psycopg2.connect(host="localhost",
                        user="postgres",
                        password="1234",
                        database="youtube_datas",
                        port="5432")
    cursor=mydb.cursor()

    create_query="""create table if not exists channels(channel_name varchar(100),
                                                        channel_id varchar(80) primary key,
                                                        subscriber bigint,
                                                        viewers bigint,
                                                        total_videos int,
                                                        description text,
                                                        playlist varchar(80))"""
    cursor.execute(create_query)
    mydb.commit()

    single_channel_detail=[]
    db=client["Youtube_data"]
    coll1=db["channel_details"]
    for ch_data in coll1.find({"channel_information.channel_name":channel_name_s},{"_id":0}):
        single_channel_detail.append(ch_data["channel_information"])

    df_single_channel_detail=pd.DataFrame(single_channel_detail)

    for index,row in df_single_channel_detail.iterrows():
        insert_query="""insert into channels(channel_name,
                                            channel_id,
                                            subscriber,
                                            viewers,
                                            total_videos,
                                            description,
                                            playlist)
                                            
                                            values(%s,%s,%s,%s,%s,%s,%s)"""
        values=(row["channel_name"],
                row["channel_id"],
                row["subscriber"],
                row["viewers"],
                row["total_videos"],
                row["description"],
                row["playlist"])

        try:
            cursor.execute(insert_query,values)
            mydb.commit()
        except:
            news=f"The Channel name '{channel_name_s}' you provided already exists"
            return news
        
def playlist_table(channel_name_s):
    mydb=psycopg2.connect(host="localhost",
                        user="postgres",
                        password="1234",
                        database="youtube_datas",
                        port="5432")
    cursor=mydb.cursor()

    create_query="""create table if not exists playlists(Playlist_Id varchar(100) primary key,
                                                        Playlist_Title varchar(80),
                                                        Channel_Id varchar(100),
                                                        Channel_Name varchar(100),
                                                        PublishedAt timestamp,
                                                        Video_count int)"""
                    
    cursor.execute(create_query)
    mydb.commit()

    single_playlist_details=[]
    db=client["Youtube_data"]
    coll1=db["channel_details"]
    for ch_data in coll1.find({"channel_information.channel_name":channel_name_s},{"_id":0}):
        single_playlist_details.append(ch_data["playlist_information"])

    df_single_playlist_details=pd.DataFrame(single_playlist_details[0])

    for index,row in df_single_playlist_details.iterrows():
        insert_query="""insert into playlists(Playlist_Id,
                                            Playlist_Title,
                                            Channel_Id,
                                            Channel_Name,
                                            PublishedAt,
                                            Video_count)
                                            
                                            values(%s,%s,%s,%s,%s,%s)"""
        values=(row["Playlist_Id"],
                row["Playlist_Title"],
                row["Channel_Id"],
                row["Channel_Name"],
                row["PublishedAt"],
                row["Video_count"])        
        
        cursor.execute(insert_query,values)
        mydb.commit()

def videos_table(channel_name_s):
    mydb = psycopg2.connect(
        host="localhost",
        user="postgres",
        password="1234",
        database="youtube_datas",
        port="5432"
    )
    cursor = mydb.cursor()

    # Create table query
    create_query = """create table if not exists videos(
        Channel_Name varchar(100),
        Channel_Id varchar(80),
        Video_Id varchar(40) primary key,
        Title varchar(150),
        Tags text,
        Thumbnail varchar(200),
        Description text,
        Published_Date timestamp,
        Duration interval,
        Views bigint,
        Likes bigint,
        Comments int,
        Favorite_Count int,
        Definition varchar(10),
        Caption_Status varchar(50)
    )"""
                    
    cursor.execute(create_query)
    mydb.commit()

    single_videos_details=[]
    db=client["Youtube_data"]
    coll1=db["channel_details"]
    for ch_data in coll1.find({"channel_information.channel_name":channel_name_s},{"_id":0}):
        single_videos_details.append(ch_data["video_details"])

    df_single_videos_details=pd.DataFrame(single_videos_details[0])

    for index, row in df_single_videos_details.iterrows():
            insert_query = """
                INSERT INTO videos(
                    Channel_Name,
                    Channel_Id,
                    Video_Id,
                    Title,
                    Tags,
                    Thumbnail,
                    Description,
                    Published_Date,
                    Duration,
                    Views,
                    Likes,
                    Comments,
                    Favorite_Count,
                    Definition,
                    Caption_Status
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            values = (
                row["Channel_Name"],
                row["Channel_Id"],
                row["Video_Id"],
                row["Title"],
                row["Tags"],
                row["Thumbnail"],
                row["Description"],
                row["Published_Date"],
                row["Duration"],
                row["Views"],
                row["Likes"],
                row["Comments"],
                row["Favorite_Count"],
                row["Definition"],
                row["Caption_Status"]
            )
            cursor.execute(insert_query, values)
            mydb.commit()

def comment_table(channel_name_s):
    mydb=psycopg2.connect(host="localhost",
                        user="postgres",
                        password="1234",
                        database="youtube_datas",
                        port="5432")
    cursor=mydb.cursor()

    create_query="""create table if not exists comments(Comment_Id varchar(100) primary key,
                                                        Video_Id varchar(80),
                                                        Comment_Text text,
                                                        Comment_Author varchar(150),
                                                        Comment_Published timestamp
                                                        )"""                
    cursor.execute(create_query)
    mydb.commit()

    single_comments_details=[]
    db=client["Youtube_data"]
    coll1=db["channel_details"]
    for ch_data in coll1.find({"channel_information.channel_name":channel_name_s},{"_id":0}):
        single_comments_details.append(ch_data["comment_details"])

    df_single_comments_details=pd.DataFrame(single_comments_details[0])

    for index,row in df_single_comments_details.iterrows():
            insert_query="""insert into comments(Comment_Id,
                                                    Video_Id,
                                                    Comment_Text,
                                                    Comment_Author,
                                                    Comment_Published)
                                                                                        
                                                values(%s,%s,%s,%s,%s)"""
            values=(row["Comment_Id"],
                    row["Video_Id"],
                    row["Comment_Text"],
                    row["Comment_Author"],
                    row["Comment_Published"])        
            
            cursor.execute(insert_query,values)
            mydb.commit()

def tables(single_channel):
    news=channels_table(single_channel)
    if news:
        return news
    else:
        playlist_table(single_channel)
        videos_table(single_channel)
        comment_table(single_channel)

        return "Table Created Sucessfully"

def show_channels_table():
    ch_list=[]
    db=client["Youtube_data"]
    coll1=db["channel_details"]
    for ch_data in coll1.find({},{"_id":0,"channel_information":1}):
        ch_list.append(ch_data["channel_information"])
    df=st.dataframe(ch_list)

    return df

def show_playlist_table():
    pl_list=[]
    db=client["Youtube_data"]
    coll1=db["channel_details"]
    for pl_data in coll1.find({},{"_id":0,"playlist_information":1}):
        for i in range(len(pl_data["playlist_information"])):
            pl_list.append(pl_data["playlist_information"][i])
    df1=st.dataframe(pl_list)

    return df1

def show_videos_table():
    vi_list=[]
    db=client["Youtube_data"]
    coll1=db["channel_details"]
    for vi_data in coll1.find({},{"_id":0,"video_details":1}):
        for i in range(len(vi_data["video_details"])):
            vi_list.append(vi_data["video_details"][i])
    df2=st.dataframe(vi_list)

    return df2

def show_comments_table():
    com_list=[]
    db=client["Youtube_data"]
    coll1=db["channel_details"]
    for com_data in coll1.find({},{"_id":0,"comment_details":1}):
        for i in range(len(com_data["comment_details"])):
            com_list.append(com_data["comment_details"][i])
    df3=st.dataframe(com_list)

    return df3

import streamlit as st

# Inject custom CSS with higher specificity to override Streamlit defaults
st.markdown(
    """
    <style>
    /* Apply the pink background to the entire page */
    body {
        background-color: #dcdcdc  !important;  /* Peach background */
    }
    .block-container {
        background-color: #dcdcdc  !important;  /* Ensures Peach background within main content */
    }
    .stApp {
        background-color: #dcdcdc  !important;  /* Entire Streamlit app container */
    }
    /* Centered title styling */
    .title {
        text-align: center;
        font-size: 50px;
        color: green;
        font-weight: bold;  /* Makes the title bold */
    }
    </style>
    """,
    unsafe_allow_html=True
)
#ffe5b4

# Centered title
st.markdown('<p class="title">YOUTUBE ANALYSIS</p>', unsafe_allow_html=True)

# Content examples
st.header(":blue[Data Collection and Warehousing Using SQL and Streamlit]")
col1, col2 = st.columns(2)

with col1:
    st.caption(":red[👉 Transforming YouTube Data into Insights]")
    st.caption(":red[👉 Simplifying YouTube Analytics]")
    st.caption(":red[👉 Harvest, Store, Visualize]")
    st.caption(":red[👉 Insights from YouTube, Simplified]")
    st.caption(":red[👉 Your YouTube Data, Optimized]")

with col2:
    st.caption(":red[👉 Analyze YouTube with Ease]")
    st.caption(":red[👉 From Data to Insights]")
    st.caption(":red[👉 Streamlined YouTube Analytics]")
    st.caption(":red[👉 Unlock YouTube Insights]")
    st.caption(":red[👉 Data-Driven YouTube Analysis]")

# Channel ID input at the top of the main layout
st.subheader(":blue[Enter the YouTube Channel ID]")
channel_id = st.text_input(":green[Channel ID Required]", "")

# Data grabbing and storage button
if st.button("Grab and Store Data"):
    ch_ids = []
    db = client["Youtube_data"]
    coll1 = db["channel_details"]
    for ch_data in coll1.find({}, {"_id": 0, "channel_information": 1}):
        ch_ids.append(ch_data["channel_information"]["channel_id"])

    if channel_id in ch_ids:
        st.success("Channel details for this ID already exist")
    else:
        insert = channel_details(channel_id)
        st.success(insert)

# Channel selection for transferring to SQL
st.subheader(":blue[Select Channel to Transfer Data to SQL]")
All_channels = []
db = client["Youtube_data"]
coll1 = db["channel_details"]
for ch_data in coll1.find({}, {"_id": 0, "channel_information": 1}):
    All_channels.append(ch_data["channel_information"]["channel_name"])

unique_channel = st.selectbox(":green[Select the channel]", All_channels)

if st.button("Transfer to SQL"):
    Table = tables(unique_channel)
    st.success(Table)

# Table selection for viewing data
st.subheader(":blue[Pick a Table to View]")
show_table = st.radio(":green[Choose your table to explore:]", ("CHANNELS", "PLAYLISTS", "VIDEOS", "COMMENTS"))

if show_table == "CHANNELS":
    show_channels_table()
elif show_table == "PLAYLISTS":
    show_playlist_table()
elif show_table == "VIDEOS":
    show_videos_table()
elif show_table == "COMMENTS":
    show_comments_table()

# SQL connection and questions
st.subheader(":blue[SQL Query Options]")
mydb = psycopg2.connect(
    host="localhost",
    user="postgres",
    password="1234",
    database="youtube_datas",
    port="5432"
)
cursor = mydb.cursor()

Questions = st.selectbox(":green[Pick a question to query:]", (
    "1. All videos and their channels",
    "2. Channels with most videos",
    "3. Top 10 most viewed videos",
    "4. Comments on each video",
    "5. Videos with most likes",
    "6. Total likes and dislikes per video",
    "7. Total views per channel",
    "8. Channels with videos published in 2022",
    "9. Average video duration per channel",
    "10. Videos with most comments"
))

if Questions=="1. All videos and their channels":
    query1="""select title as videos, channel_name as channelname from videos"""
    cursor.execute(query1)
    mydb.commit()
    t1=cursor.fetchall()
    df1=pd.DataFrame(t1,columns=["video title","channel name"])
    st.write(df1)


elif Questions=="2. Channels with most videos":
    query2="""select channel_name as channelname, total_videos as n0_videos from channels
                order by total_videos desc"""
    cursor.execute(query2)
    mydb.commit()
    t2=cursor.fetchall()
    df2=pd.DataFrame(t2,columns=["channel name","No of videos"])
    st.write(df2)

elif Questions=="3. Top 10 most viewed videos":
    query3="""select views as views,channel_name as channelname,title as videotitle from videos 
                where views is not null order by views desc limit 10"""
    cursor.execute(query3)
    mydb.commit()
    t3=cursor.fetchall()
    df3=pd.DataFrame(t3,columns=["views","channel name","videotitle"])
    st.write(df3)

elif Questions=="4. Comments on each video":
    query4="""select comments as total_comments,title as videotitle from videos where comments is not null"""
    cursor.execute(query4)
    mydb.commit()
    t4=cursor.fetchall()
    df4=pd.DataFrame(t4,columns=["total comments","videotitle"])
    st.write(df4)

elif Questions=="5. Videos with most likes":
    query5="""select title as videotitle,channel_name as channelname, likes as likescount
                from videos where likes is not null order by likes desc"""
    cursor.execute(query5)
    mydb.commit()
    t5=cursor.fetchall()
    df5=pd.DataFrame(t5,columns=["videotitle","channelname","likescount"])
    st.write(df5)

elif Questions=="6. Total likes and dislikes per video":
    query6="""select likes as likescount, title as videotitle from videos"""
    cursor.execute(query6)
    mydb.commit()
    t6=cursor.fetchall()
    df6=pd.DataFrame(t6,columns=["likescount","videotitle"])
    st.write(df6)

elif Questions=="7. Total views per channel":
    query7="""select channel_name as channelname ,viewers as totalviews from channels"""
    cursor.execute(query7)
    mydb.commit()
    t7=cursor.fetchall()
    df7=pd.DataFrame(t7,columns=["channel name","total views"])
    st.write(df7)

elif Questions=="8. Channels with videos published in 2022":
    query8="""select title as video_title,published_date as videorelease,channel_name as channelname from videos 
                where extract(year from published_date)=2022"""
    cursor.execute(query8)
    mydb.commit()
    t8=cursor.fetchall()
    df8=pd.DataFrame(t8,columns=["video title","published date","channel name"])
    st.write(df8)

elif Questions=="9. Average video duration per channel":
    query9="""select channel_name as channelname, avg(duration) as averageduration from videos group by channel_name"""
    cursor.execute(query9)
    mydb.commit()
    t9=cursor.fetchall()
    df9=pd.DataFrame(t9,columns=["channel name","average duration"])
    T9=[]
    for index,row in df9.iterrows():
        channel_title=row["channel name"]
        average_duration=row["average duration"]
        average_duration_str=str(average_duration)
        T9.append(dict(channeltitle=channel_title,avgduration=average_duration_str))
    df=pd.DataFrame(T9)
    st.write(df)

elif Questions=="10. Videos with most comments":
    query10="""select title as videotitle, channel_name as channelname, comments as comments from videos where comments
                is not null order by comments desc"""
    cursor.execute(query10)
    mydb.commit()
    t10=cursor.fetchall()
    df10=pd.DataFrame(t10,columns=["video title","channel name","comments"])
    st.write(df10)
