#!/usr/bin/env python

"""

Steps for converting Infochimps to our format:

1) Input is a series of files, e.g. part-00000.gz, each about 180 MB.
2) Each line looks like this:


100000018081132545      20110807002716  25430513        GTheHardWay                                     Niggas Lost in the Sauce ..smh better slow yo roll and tell them hoes to get a job nigga #MRIloveRatsIcanchange&amp;amp;saveherassNIGGA &lt;a href=&quot;http://twitter.com/download/android&quot; rel=&quot;nofollow&quot;&gt;Twitter for Android&lt;/a&gt;    en      42.330165       -83.045913                                      
The fields are:

1) Tweet ID
2) Time
3) User ID
4) User name
5) Empty?
6) User name being replied to (FIXME: which JSON field is this?)
7) User ID for replied-to user name (but sometimes different ID's for same user name)
8) Empty?
9) Tweet text -- double HTML-encoded (e.g. & becomes &amp;amp;)
10) HTML anchor text indicating a link of some sort, HTML-encoded (FIXME: which JSON field is this?)
11) Language, as a two-letter code
12) Latitude
13) Longitude
14) Empty?
15) Empty?
16) Empty?
17) Empty?


3) We want to convert each to two files: (1) containing the article-data 

"""


