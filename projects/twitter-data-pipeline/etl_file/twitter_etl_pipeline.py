import tweepy
import s3fs
import pandas as pd
import json
from datetime import datetime
from dotenv import load_dotenv
import os

load_dotenv()


# def twitter_etl():

#     # defining the twitter keys
#     API_KEY = os.getenv("Twitter_API_KEY")
#     API_SECRET_KEY = os.getenv("Twitter_API_SECRET_KEY")
#     ACCESS_TOKEN = os.getenv("ACCESS_TOKEN")
#     ACCESS_TOKEN_SECRET = os.getenv("ACCESS_TOKEN_SECRET")

#     auth = tweepy.OAuthHandler(
#         consumer_key=API_KEY,
#         consumer_secret=API_SECRET_KEY,
#         access_token=ACCESS_TOKEN,
#         access_token_secret=ACCESS_TOKEN_SECRET,
#     )

#     api = tweepy.API(auth)

#     tweets = api.user_timeline(
#         screen_name="@elonmusk", count=500, include_rts=False, tweet_mode="extended"
#     )

#     listOfTweets = []
#     for tweet in tweets:
#         text = tweet._json["full_text"]
#         tweet_structure = {
#             "user": tweet.user.screen_name,
#             "text": text,
#             "favorite_count": tweet.favorite_count,
#             "retweet_count": tweet.retweet_count,
#             "created_at": tweet.created_at,
#         }

#         listOfTweets.append(tweet_structure)

#     tweet_df = pd.DataFrame(listOfTweets)
#     return tweet_df.to_csv("tweets.csv")


def twitter_etl():
    try:
        # Define the Twitter API keys from environment variables
        BEARER_TOKEN = os.getenv("Twitter_BEARER_TOKEN")

        # Check if the bearer token is None
        if not BEARER_TOKEN:
            raise ValueError("Bearer token for Twitter API is missing.")

        # Authenticate to Twitter API v2 using Bearer Token
        client = tweepy.Client(bearer_token=BEARER_TOKEN)

        # Get user ID by username
        user_response = client.get_user(username="elonmusk")

        if user_response.errors:
            for error in user_response.errors:
                print(error)
            return

        user_id = user_response.data.id

        # Get tweets from the user timeline
        response = client.get_users_tweets(
            id=user_id,
            tweet_fields=["created_at", "text", "public_metrics"],
            max_results=100,  # Max results per request, you can paginate for more
        )

        if response.errors:
            for error in response.errors:
                print(error)
            return

        tweets = response.data
        listOfTweets = []
        for tweet in tweets:
            tweet_structure = {
                "text": tweet.text,
                "created_at": tweet.created_at,
                "retweet_count": tweet.public_metrics["retweet_count"],
                "reply_count": tweet.public_metrics["reply_count"],
                "like_count": tweet.public_metrics["like_count"],
                "quote_count": tweet.public_metrics["quote_count"],
            }
            listOfTweets.append(tweet_structure)

        # Create a DataFrame from the list of tweets
        tweet_df = pd.DataFrame(listOfTweets)
        # Save the DataFrame to a CSV file
        tweet_df.to_csv("tweets.csv", index=False)
        print("Tweets successfully saved to tweets.csv")

    except tweepy.TweepyException as e:
        print(f"Error: Twitter API error - {e}")
    except Exception as e:
        print(f"Error: {e}")


twitter_etl()
