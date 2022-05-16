import os
import sys
import time
import schedule
import praw
import pymongo
import certifi
from urllib.parse import quote_plus

class WEXBot:

    def __init__(self, praw_id, praw_secret, praw_user_agent, mongo_un, mongo_pw, mongo_db, database_name, collection_name, num_posts):
        self.reddit = self.get_reddit(praw_id, praw_secret, praw_user_agent)
        self.mongo = self.get_mongo(mongo_db, mongo_un, mongo_pw, database_name, collection_name)
        self.last_timestamp = 0.0
        self.num_posts = int(num_posts)

    def get_reddit(self, client_id, client_secret, user_agent):
        # Try obtaining a PRAW instance, print error and exit if unable
        try:
            reddit = praw.Reddit(
                client_id=client_id,
                client_secret=client_secret,
                user_agent=user_agent,
            )

            for submission in reddit.subreddit("Watchexchange").new(limit=1):
                pass
            return reddit

        except Exception as e:
            sys.exit("Exception occured when connecting to reddit.\n\tException: {}".format(e))

    def get_mongo(self, db, username, password, database_name, collection_name):
        # Get ssl cert for mongodb.net
        ca = certifi.where()
        
        # Handle special characters
        encoded_username = quote_plus(username)
        encoded_password = quote_plus(password)

        # Build connection string
        conn_str = "mongodb+srv://{}:{}@{}?retryWrites=true&w=majority".format(encoded_username, encoded_password, db)
        
        # set a 5-second connection timeout
        client = pymongo.MongoClient(conn_str, serverSelectionTimeoutMS=5000, tlsCAFile=ca)
 
        # Check if connected and return collection, print error and exit if not
        try:
            client.server_info()
            collection = client[database_name][collection_name]
            return collection

        except Exception as e:
            sys.exit("Exception occured when connecting to database.\n\t Exception: {}".format(e))

    def process_new_posts(self):
        first = None
        notify_dictionary = {}

        # Iterate through our chosen number of posts
        for submission in self.reddit.subreddit("Watchexchange").new(limit=self.num_posts):

            # Save the timestamp of the newest post
            if not first:
                first = submission.created_utc

            # Stop processing once we've reached our last processed submission
            if self.last_timestamp >= submission.created_utc:
                break

            # Otherwise process the submission
            else:
                self.check_db(notify_dictionary, submission)
        
        # Set our last processed post's time, and notify users
        self.last_timestamp = first
        
        # Notify users if we have pending posts
        if notify_dictionary:
            #FIXME Need to send an email to users, just print for now
            print(notify_dictionary)

    def check_db(self, notify_dictionary, submission):
        # Get all of our emails and keywords from DB
        cursor = self.mongo.find({})

        # Iterate through our emails
        for document in cursor:
            # Iterate through our keywords
            for keyword in document.get("keywords"):

                # Check if the submission title contains one of our keywords                
                if keyword.lower() in submission.title.lower():

                    # If we already have an entry for this email, append the submission to the list
                    if notify_dictionary.get(document.get("email")):
                        notify_dictionary.get(document.get("email")).append(submission)

                    # Otherwise add a new entry in our dictionary
                    else:
                        notify_dictionary[document.get("email")] = [submission]


def main():
    instance = WEXBot(
                    os.getenv("PRAW_CLIENT_ID"), 
                    os.getenv("PRAW_CLIENT_SECRET"),
                    os.getenv("PRAW_USER_AGENT"),
                    os.getenv("MONGO_USERNAME"),
                    os.getenv("MONGO_PASSWORD"),
                    os.getenv("MONGO_URI"),
                    os.getenv("MONGO_DB_NAME"),
                    os.getenv("MONGO_COLLECTION_NAME"),
                    os.getenv("NUM_POSTS")
                )
    schedule.every(2).seconds.do(instance.process_new_posts)
    #schedule.every(5).minutes.do(instance.process_new_posts)
    
    while True:
        schedule.run_pending()
        #time.sleep(10)
        time.sleep(1)

if __name__ == "__main__":
    main()
