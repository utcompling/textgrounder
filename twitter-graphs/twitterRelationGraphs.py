#===============================================================================
# !/usr/bin/env python
# 
# twitterRelationGraphs.py
#
# This script calls the twitter API to obtain the followers and friends graph for
# a given set of users. It also obtains the tweets of these users.
#
# Dependency:
# 1. Python 2.7
# 2. Python-Twitter-0.8.2 http://code.google.com/p/python-twitter/
#     - Refer to their documentation for installation of the dependencies
#     - Currently using a modified version of library at: <NEEDS LINK> 
#     - Rebuild and reinstall with the mod ver
#
# Copyright (c) 2011 Andy Luong.
#===============================================================================

import twitter, sys
from operator import itemgetter
import codecs
import time

api = None

#===============================================================================
# Book Keeping
#===============================================================================
processedUsers = set()
processedOthers = set()
unprocessableUsers = set()
failGetTweets = 0

#===============================================================================
# Output Files
#===============================================================================
outFollowersGraph = None
outFollowersTweets = None
outFriendsGraph = None
outFriendsTweets = None

#===============================================================================
# Parameters
#===============================================================================
# Minimum number of followers/followers the target user must have before we process their graphs
# Number must be at least 1 to be processed
minNumFollowers = -1
minNumFriends = 1

# Minimum number of tweets the follower/friend must have
minNumTweets = 30

# Maximum number of followers the target user has (celebs...) 
maxNumFollowers = 1000

# Number of followers to extract
numFollowersToProcess = 20

# Number of tweets to extract for follower/friend (max = 200)
numTweetsPerTimeline = 200

# Twitter API Call delay between GetTimeline calls
apiCallDelaySeconds = 1

# max number continuous of failures before we apiCallDelaySeconds
maxNumFailures = 3

# Twitter API Call delay after maxNumFailures
failDelaySeconds = 7

#===============================================================================
# Authenticate twitter API calls
# This is required in order for most sophisticated API calls
# Obtain Keys after registering an application at: https://dev.twitter.com/apps
# Return: True if successful, False otherwise
#===============================================================================
def authenticate(ck, cs, atk, ats):
    global api

    api = twitter.Api(
        consumer_key = ck,
        consumer_secret = cs,
        access_token_key = atk,
        access_token_secret = ats)

    if not api.VerifyCredentials():
        print "There is an error with your authentication keys."
        return False

    else:
        print "You have been successfully authenticated."
        return True

#===============================================================================
# Checks how many remaining Twitter API are remaining
# If the 'remaining hits' is less than the minimum required, sleep script
#===============================================================================
def checkApiLimit():
    if api == None:
        return

    minRequiredHits = numFollowersToProcess - 2

    if api.GetRateLimitStatus()[u'remaining_hits'] - minRequiredHits < 0:
        limit_sleep = round(api.GetRateLimitStatus()[u'reset_time_in_seconds'] - time.time() + 5)
        print "API limit reached, sleep for %d seconds..." % limit_sleep
        time.sleep(limit_sleep)

#===============================================================================
# Get the last 'numTweetsPerTimeline' tweets for a given user
# If the Twitter API call fails, try up 3 times
#===============================================================================
def getTweets(user):
    global failGetTweets

    print ("\tRetrieving Timeline for User: " +
           str(user.GetId()) + "/" + user.GetName() +
           "\tNumTweets: " + str(user.GetStatusesCount()))
    attempts = 0
    maxAttempts = 3

    while attempts < maxAttempts:
        try:
            attempts += 1
            tweets = api.GetUserTimeline(id = user.GetId(), count = numTweetsPerTimeline)
            print "\t\tSuccesffuly Extracted %d tweets" % len(tweets)
            failGetTweets = 0

            return tweets

        except (KeyboardInterrupt, SystemExit):
            raise

        except:
            print ("\t\tAttempt %d: Failed retrieval for User: " + str(user.GetId())) % attempts
            failGetTweets = failGetTweets + 1

            if failGetTweets >= maxNumFailures:
                print "\nDue to high API call failure, sleeping script for %d seconds\n" % failDelaySeconds
                time.sleep(failDelaySeconds)
                failGetTweets = 0

    return []


#===============================================================================
# Remove users that have less than 'minNumTweets' of tweets
# Sort the remaining follower by number of tweets (descending)
# Return: Subset of Followers
#===============================================================================
def filterUsers(users):
    users = [(user.GetStatusesCount(), user) for user in users if (user.GetStatusesCount() >= minNumTweets and
                                                                   user.GetFollowersCount() <= maxNumFollowers)]
    users = sorted(users, key = itemgetter(0), reverse = True)
    users = [user[1] for user in users]

    return users

#===============================================================================
# For a given user, obtain their follower graph
# Notation: other = follower/friend
# Output the follower/friend graph (ID_user, SN_user, ID_other, SN_other)
# Output the follower/friend tweets, for each tweet (ID_other, SN_other, date, geo, text )
#===============================================================================
def outputGraphsAndTweets(user, fGraph, outGraph, outTweet):
    for other in fGraph:
        otherID = ("ID_" + other[0] + "\t" +
                      "SN_\'" + other[1] + "\'")

        #Write the graph
        outGraph.write("ID_" + str(user.GetId()) + "\t" +
                            "SN_\'" + user.GetName() + "\'\t" +
                            otherID +
                            "\n")

        for tweet in other[2]:
            geo = tweet.GetLocation()

            if geo == None or geo == '':
                geo = "NoCoords"
            geo = "<" + geo + ">"

            tweet_s = (otherID + "\t" +
                       tweet.GetCreatedAt() + "\t" +
                       geo + "\t" +
                       tweet.GetText().lower())

            #Write the Tweets
            outTweet.write(tweet_s + "\n")

#===============================================================================
# If the user has minNumFollowers <= # followers <= maxNumFollowers, find their followers and tweets 
# Notation: other = follower/friend
# Return: list of followers/friends where each follower/friend(id, screen_name, list(tweets) )
#===============================================================================
def relationshipGraph(user, gType):
    if gType.lower() == 'followers':
        print "Processing Followers Graph for User: " + str(user.GetId()) + "/" + user.GetName()
    elif gType.lower() == 'friends':
        print "Processing Friends Graph for User: " + str(user.GetId()) + "/" + user.GetName()

    fg = []

    if (user.GetFollowersCount() >= minNumFollowers and
        user.GetFollowersCount() <= maxNumFollowers):
        others = []
        attempts = 0
        maxAttempts = 2

        while attempts < maxAttempts:
            try:
                attempts += 1

                if gType.lower() == 'followers':
                    others = api.GetFollowers(user.GetId())
                elif gType.lower() == 'friends':
                    others = api.GetFriends(user.GetId())
                else:
                    print "Unsupported Relationship Graph Processing: " + gType
                    return []

            except twitter.TwitterError:
                print ("\t\tAttempt %d: Failed retrieval for User: " + str(user.GetId())) % attempts
                failGetTweets = failGetTweets + 1

                if failGetTweets >= maxNumFailures:
                    print "\nDue to high API call failure, sleeping script for %d seconds\n" % failDelaySeconds
                    time.sleep(failDelaySeconds)
                    failGetTweets = 0

            except (KeyboardInterrupt, SystemExit):
                raise

        if others == []:
            return []

        others = filterUsers(others)
        count = 0

        for i in range(0, len(others)):
            #Delay between calls
            time.sleep(apiCallDelaySeconds)

            other = others[i]

            if not other.GetId() in processedOthers:
                tweets = getTweets(other)
                processedOthers.add(other.GetId())

                if not tweets == []:
                    fg += [(str(other.GetId()), other.GetName(), tweets)]
                    count += 1

            if count >= numFollowersToProcess:
                break
    else:
        print "\tSkipping User because of too few or many followers (%d)." % user.GetFollowersCount()

    return fg

#===============================================================================
# For each user, process their followers/friends graph, as specified
#===============================================================================
def processUser(userID):
    print "Processing User: " + userID

    try:
        user = api.GetUser(userID)

        if minNumFollowers >= 1:
            followers = relationshipGraph(user, 'followers')
            outputGraphsAndTweets(user, followers, outFollowersGraph, outFollowersTweets)

        if minNumFriends >= 1:
            friends = relationshipGraph(user, 'friends')
            outputGraphsAndTweets(user, friends, outFriendsGraph, outFriendsTweets)

        processedUsers.add(str(user.GetId()))

    except twitter.TwitterError:
        print "\tUnprocessable User: ", userID
        unprocessableUsers.add(userID)

    except (KeyboardInterrupt, SystemExit):
        raise

#===============================================================================
# Closes output files
# Writes a log of processed users to 'processedUsers.log'
# Writes a log of unprocessed users to 'unprocessedUsers.log'
#===============================================================================
def cleanup():
    if outFollowersGraph:
        outFollowersGraph.close()

    if outFriendsGraph:
        outFriendsGraph.close()

    if outFollowersTweets:
        outFollowersTweets.close()

    flog = open('unprocessedUsers.log', 'w')
    for user in unprocessableUsers:
        flog.write(user + "\n")
    flog.close()

    flog = open('processedUsers.log', 'w')
    for user in processedUsers:
        flog.write(user + "\n")
    flog.close()

#===============================================================================
# Reads in the file with a list of users and attempts to process each user
#===============================================================================
def processUsers(users_file):
    print "Processing Users..."
    fin = open(users_file, 'r')


    for user in fin:
        user = user.strip()

        if not user in processedUsers and not len(user) == 0:
            checkApiLimit()
            processUser(user)

    fin.close()

def main():
    global outFollowersGraph, outUnprocessedUsers, outFollowersTweets, outFriendsGraph, outFriendsTweets

    if len(sys.argv) < 7:
        print "Params: <authen-keys> <users> <followers output file>"
        sys.exit()

    #Parse Key File
    fin = open(sys.argv[1], 'r')
    keys = fin.readlines()
    fin.close()
    if not authenticate(keys[0].strip(), keys[1].strip(), keys[2].strip(),
                    keys[3].strip()):
        sys.exit()

    #Create output files
    outFollowersGraph = codecs.open(sys.argv[3], encoding = 'utf-8', mode = 'w')
    outFollowersTweets = codecs.open(sys.argv[4], encoding = 'utf-8', mode = 'w')
    outFriendsGraph = codecs.open(sys.argv[5], encoding = 'utf-8', mode = 'w')
    outFriendsTweets = codecs.open(sys.argv[6], encoding = 'utf-8', mode = 'w')

    #Process Users
    processUsers(sys.argv[2])

    #Cleanup
    cleanup()

main()

