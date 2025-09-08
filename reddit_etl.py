import os
import requests as req
import json
import pandas as pd
from datetime import *
import time
from s3fs import S3FileSystem
import praw #Python Reddit API Wrapper



def  run_reddit_etl():
    path = r'C:\Users\sijus\OneDrive\Desktop'
    # Authenticate with Reddit API
    reddit = praw.Reddit(
        client_id="0bw7MkRjxr_i8cy38rVOHQ",
        client_secret="8iEJW9pz5DtF6dFg2UmlHNmhhq8jQA",
        user_agent="reddit_live_feed_search_app"
    )

    # Subreddit and keyword to monitor
    subreddit_name = "Python"
    keyword = "pandas"

    # Keep track of already seen post IDs to avoid duplicates
    seen_posts = []
    found_new = False

    print("Streaming new posts from r/",subreddit_name, "containing", keyword,"...\n")


    try:
        # Fetch the 10 newest posts
        for post in reddit.subreddit(subreddit_name).new(limit=10):
            if post.id not in seen_posts and keyword.lower() in post.title.lower():
                post_dict = {
                    "Title": post.title,
                    "Author": str(post.author),
                    "Score": post.score,
                    "URL": post.url,
                    "post_time": datetime.fromtimestamp(post.created_utc).strftime('%Y-%m-%d %H:%M:%S')}
                print(post_dict)
                print("-" * 50)
                found_new = True


            elif not found_new:
                print("No new posts in the last 30 seconds. Showing recent past 10 posts:\n")
                post_dict = {
                "Title": post.title,
                "Author": str(post.author),
                "Score": post.score,
                "URL": post.url,
                "post_time": datetime.fromtimestamp(post.created_utc).strftime('%Y-%m-%d %H:%M:%S')}
                print(post_dict)
                print("-" * 50)
            seen_posts.append(post_dict)

        s3 = S3FileSystem(anon=False)
        df = pd.DataFrame(seen_posts)
        # Define S3 path
        s3_path = "s3://siju-etl-project/Python_reddit_p1.csv"

    # Upload to S3 with verification
    try:
        df.to_csv(s3_path)
        if s3.exists(s3_path):
            print(f"Successfully uploaded to {s3_path}")
        else:
            raise Exception("File upload failed - file not found in S3")
    except Exception as s3_error:
        print(f"S3 Upload Error: {s3_error}")
        raise


        # Wait before polling again (e.g., 30 seconds)
        time.sleep(30)

    except Exception as e:
        print(f"Error: {e}")
        time.sleep(60)  # wait a bit before retrying





