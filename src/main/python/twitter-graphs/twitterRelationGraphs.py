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
import argparse
import httplib

#===============================================================================
# Globals
#===============================================================================
api = None
args = None

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
outProcessedUsers = None
outUnProcessedUsers = None

#===============================================================================
# Parameters
#===============================================================================
# Minimum number of followers the target user must have before we process their graphs
# Number must be at least 1 to be processed
minNumFollowers = 50

# Minimum number of tweets the follower/friend must have
minNumTweets = 30

# Maximum number of followers the target user has (celebs...) 
maxNumFollowers = 1000

# Number of users to extract
numUsersToProcess = 100

# Number of tweets to extract for follower/friend (max = 200)
numTweetsPerTimeline = 200

# Twitter API Call delay between GetTimeline calls
apiCallDelaySeconds = 3

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
    attempts = 0
    while attempts < 3:
        try:
            attempts += 1
            minRequiredHits = numUsersToProcess - 2
            print "Remaining Hits: " + str(api.GetRateLimitStatus()[u'remaining_hits'])

            limit = api.GetRateLimitStatus()[u'remaining_hits']

            if not outFollowersTweets == None or not outFriendsTweets == None:
                limit -= minRequiredHits

            if  limit <= 0:
                limit_sleep = round(api.GetRateLimitStatus()[u'reset_time_in_seconds'] - time.time() + 5)
                print "API limit reached, sleep for %d seconds..." % limit_sleep
                time.sleep(limit_sleep)

            return

        except (twitter.TwitterError, httplib.BadStatusLine):
            print "Error checking for Rate Limit, Sleeping for 60 seconds..."
            time.sleep(60)

#===============================================================================
# Get the last 'numTweetsPerTimeline' tweets for a given user
# If the Twitter API call fails, try up 3 times
#===============================================================================
def getTweets(user):
    global failGetTweets

    print ("\tRetrieving Timeline for User: " +
           str(user.GetId()) + "/" + user.GetName() +
           "\tNumTweets: " + str(user.GetStatusesCount()))

    if user.GetId() in processedOthers:
        print "\t\tAlready Extracted tweets, so skipping..."
        return "SKIP"

    attempts = 0
    maxAttempts = 3

    while attempts < maxAttempts:
        try:
            attempts += 1
            tweets = api.GetUserTimeline(id = user.GetId(), count = numTweetsPerTimeline)
            print "\t\tSuccessfully Extracted %d tweets" % len(tweets)
            failGetTweets = 0

            return tweets

        except (KeyboardInterrupt, SystemExit):
            raise

        except:
            print ("\t\tAttempt %d: Failed retrieval for User: " + str(user.GetId())) % attempts
            failGetTweets = failGetTweets + 1

            time.sleep(2);

            if failGetTweets >= maxNumFailures:
                print "\nDue to high API call failure, sleeping script for %d seconds\n" % failDelaySeconds
                time.sleep(failDelaySeconds)
                failGetTweets = 0

    return []


#===============================================================================
# Remove users that have less than 'minNumTweets' of tweets and ...
# minNumFollowers <= # followers <= maxNumFollowers
# Sort the remaining follower by number of tweets (descending)
# Return: Subset of Followers
#===============================================================================
def filterUsers(users):
    users = [(user.GetStatusesCount(), user) for user in users if (user.GetStatusesCount() >= minNumTweets and
                                                                   user.GetFollowersCount() >= minNumFollowers and
                                                                   user.GetFollowersCount() <= maxNumFollowers)]
    users = sorted(users, key = itemgetter(0), reverse = True)
    users = [user[1] for user in users]

    return users

#===============================================================================
# For a given user, obtain their follower graph
# Notation: other = follower/friend
# Output the follower/friend graph (ID_user, SN_user, ID_other, SN_other)
# Output the follower/friend tweets, for each tweet (ID_other, SN_other, date, geo, text ) - Optional
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

        #Write the Tweets
        if outTweet:
            for tweet in other[2]:

                geo = tweet.GetLocation()

                if geo == None or geo == '':
                    geo = "NoCoords"
                geo = "<" + geo + ">"

                tweet_s = (otherID + "\t" +
                           tweet.GetCreatedAt() + "\t" +
                           geo + "\t" +
                           tweet.GetText().lower())

                outTweet.write(tweet_s + "\n")

#===============================================================================
# If the user has minNumFollowers <= # followers <= maxNumFollowers, find their followers and tweets 
# Notation: other = follower/friend
# Return: list of followers/friends where each follower/friend(id, screen_name, list(tweets) )
#===============================================================================
def relationshipGraph(user, gType, bTweets):
    global failGetTweets

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
                    break
                elif gType.lower() == 'friends':
                    others = api.GetFriends(user.GetId())
                    break
                else:
                    print "Unsupported Relationship Graph Processing: " + gType
                    return []

            except (KeyboardInterrupt, SystemExit):
                raise

            except (twitter.TwitterError, httplib.BadStatusLine):
                print ("\t\tAttempt %d: Failed retrieval for User: " + str(user.GetId())) % attempts
                failGetTweets = failGetTweets + 1

                if failGetTweets >= maxNumFailures:
                    print "\nDue to high API call failure, sleeping script for %d seconds\n" % failDelaySeconds
                    time.sleep(failDelaySeconds)
                    failGetTweets = 0

        if others == []:
            return []

        others = filterUsers(others)
        count = 0

        for i in range(0, len(others)):
            other = others[i]

            #Default Value such that we always output follower or friend graph
            tweets = "SKIP"

            #Grab Tweets from Others
            if bTweets:
                #Delay between calls
                time.sleep(apiCallDelaySeconds)
                tweets = getTweets(other)
                processedOthers.add(other.GetId())

            if tweets == "SKIP":
                fg += [(str(other.GetId()), other.GetName(), [])]
                count += 1
            elif not tweets == []:
                fg += [(str(other.GetId()), other.GetName(), tweets)]
                count += 1

            if count >= numUsersToProcess:
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

        if args.f:
            followers = relationshipGraph(user, 'followers', not outFollowersTweets == None)
            outputGraphsAndTweets(user, followers, outFollowersGraph, outFollowersTweets)

        if args.g:
            friends = relationshipGraph(user, 'friends', not outFriendsTweets == None)
            outputGraphsAndTweets(user, friends, outFriendsGraph, outFriendsTweets)

        processedUsers.add(str(user.GetId()))
        outProcessedUsers.write(userID + "\n")

    except (KeyboardInterrupt, SystemExit):
        raise

    except (twitter.TwitterError, httplib.BadStatusLine):
        print "\tUnprocessable User: ", userID
        unprocessableUsers.add(userID)
        outUnProcessedUsers.write(userID + "\n")


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

    if outFriendsTweets:
        outFollowersTweets.close()

    if outUnProcessedUsers:
        outUnProcessedUsers.close()

    if outProcessedUsers:
        outProcessedUsers.close()

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
    parser = argparse.ArgumentParser(description = 'Build Twitter Relation Graphs.')
    setup = parser.add_argument_group('Script Setup')
    #graphs = parser.add_mutually_exclusive_group(required = True)
    graphs = parser.add_argument_group('Relationships to Compute')
    params = parser.add_argument_group('Script Parameters (Optional)')

    setup.add_argument('-k',
                        action = "store",
                        help = 'Authentication Keys Input File',
                        metavar = 'FILE',
                        required = True)
    setup.add_argument('-u',
                        action = "store",
                        help = 'Users Input File',
                        metavar = 'FILE',
                        required = True)
    graphs.add_argument('-f',
                        action = "store",
                        metavar = 'FILE',
                        help = 'Followers Graph File, Followers Tweets File (opt)',
                        nargs = '+')
    graphs.add_argument('-g',
                        action = "store",
                        metavar = 'FILE',
                        help = 'Friends Graph File, Friends Tweets File (opt)',
                        nargs = '+')
    #Parameters
    params.add_argument('-minNumFollowers',
                        action = "store",
                        help = 'Minimum number of followers the target user must have before we process their graphs. Default %(default)s',
                        metavar = 'INT',
                        type = int,
                        default = 50)
    params.add_argument('-minNumTweets',
                        action = "store",
                        help = 'Minimum number of tweets the follower/friend must have. Default %(default)s',
                        metavar = 'INT',
                        type = int,
                        default = 30,)
    params.add_argument('-maxNumFollowers',
                        action = "store",
                        help = 'Maximum number of followers the target user has. Default %(default)s',
                        metavar = 'INT',
                        type = int,
                        default = 1000)
    params.add_argument('-numUsersToProcess',
                       action = "store",
                       help = 'Number of followers/friends to extract for a user. Default %(default)s',
                       metavar = 'INT',
                       type = int,
                       default = 100)
    params.add_argument('-numTweetsPerTimeline',
                       action = "store",
                       help = 'Number of tweets to extract for follower/friend (max = 200). Default %(default)s',
                       metavar = 'INT',
                       type = int,
                       default = 200)
    params.add_argument('-apiCallDelaySeconds',
                       action = "store",
                       help = 'Twitter API Call delay between GetTimeline calls. Default %(default)s',
                       metavar = 'INT',
                       type = int,
                       default = 3)
    params.add_argument('-maxNumFailures',
                       action = "store",
                       help = 'Max number continuous of failures before we apiCallDelaySeconds. Default %(default)s',
                       metavar = 'INT',
                       type = int,
                       default = 3)
    params.add_argument('-failDelaySeconds',
                       action = "store",
                       help = 'Twitter API Call delay after maxNumFailures. Default %(default)s',
                       metavar = 'INT',
                       type = int,
                       default = 7)
    global args
    args = parser.parse_args()
    #print args
    #sys.exit()

    #Parse Key File
    fin = open(args.k, 'r')
    keys = fin.readlines()
    fin.close()
    if not authenticate(keys[0].strip(),
                        keys[1].strip(),
                        keys[2].strip(),
                        keys[3].strip()):
        sys.exit()

    #Create output files
    if args.f:
        global outFollowersGraph, outFollowersTweets
        outFollowersGraph = codecs.open(args.f[0], encoding = 'utf-8', mode = 'w')
        if len(args.f) >= 2:
            outFollowersTweets = codecs.open(args.f[1], encoding = 'utf-8', mode = 'w')


    if args.g:
        global outFriendsGraph, outFriendsTweets
        outFriendsGraph = codecs.open(args.g[0], encoding = 'utf-8', mode = 'w')
        if  len(args.g) >= 2:
            outFriendsTweets = codecs.open(args.g[1], encoding = 'utf-8', mode = 'w')

    global outUnProcessedUsers, outProcessedUsers
    outUnProcessedUsers = codecs.open(args.u + '.unprocessedUsers.log', encoding = 'utf-8', mode = 'w')
    outProcessedUsers = codecs.open(args.u + '.processedUsers.log', encoding = 'utf-8', mode = 'w')


    #Set Parameters
    global minNumTweets, minNumFollowers, maxNumFollowers, numUsersToProcess, numTweetsPerTimeline
    global apiCallDelaySeconds, maxNumFailures, failDelaySeconds
    minNumTweets = args.minNumTweets
    minNumFollowers = max(1, args.minNumFollowers)
    maxNumFollowers = args.maxNumFollowers
    numUsersToProcess = args.numUsersToProcess
    numTweetsPerTimeline = min(200, args.numTweetsPerTimeline)
    apiCallDelaySeconds = args.apiCallDelaySeconds
    maxNumFailures = args.maxNumFailures
    failDelaySeconds = args.failDelaySeconds

    #Process Users
    processUsers(args.u)

    #Cleanup
    cleanup()

main()

