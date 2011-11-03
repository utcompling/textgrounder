Introduction
============

See CHANGES for a description of the project status. 

This file contains the configuration and build instructions. 

TextGrounder website: https://bitbucket.org/utcompling/textgrounder/


---------------------------------------------------------
NOTE: See README.geolocate for more detailed instructions.
---------------------------------------------------------


Requirements
============

* Version 1.6 of the Java 2 SDK (http://java.sun.com)


Configuring your environment variables
======================================

The easiest thing to do is to set the environment variables JAVA_HOME
and TEXTGROUNDER_DIR to the relevant locations on your system. Set JAVA_HOME
to match the top level directory containing the Java installation you
want to use.

For example, on Windows:

C:\> set JAVA_HOME=C:\Program Files\jdk1.5.0_04

or on Unix:

% setenv JAVA_HOME /usr/local/java
  (csh)
> export JAVA_HOME=/usr/java
  (ksh, bash)

On Windows, to get these settings to persist, it's actually easiest to
set your environment variables through the System Properties from the
Control Panel. For example, under WinXP, go to Control Panel, click on
System Properties, choose the Advanced tab, click on Environment
Variables, and add your settings in the User variables area.

/*Next, likewise set TEXTGROUNDER_DIR to be the top level directory where you
unzipped the download. In Unix, type 'pwd' in the directory where
this file is and use the path given to you by the shell as
TEXTGROUNDER_DIR.  You can set this in the same manner as for JAVA_HOME
above.*/ //THIS WILL BE REMOVED

Next, add the directory TEXTGROUNDER_DIR/bin to your path. For example, you
can set the path in your .bashrc file as follows:

export PATH="$PATH:$TEXTGROUNDER_DIR/bin"

On Windows, you should also add the python main directory to your path.

Once you have taken care of these three things, you should be able to
build and use the TextGrounder Library.

Note: Spaces are allowed in JAVA_HOME but not in TEXTGROUNDER_DIR.  To set
an environment variable with spaces in it, you need to put quotes around
the value when on Unix, but you must *NOT* do this when under Windows.


Building the system from source
===============================

TextGrounder uses SBT (Simple Build Tool) with a standard directory
structure.  To build TextGrounder, type (in the $TEXTGROUNDER_DIR
directory):

$ textgrounder build update compile

This will compile the source files and put them in
./target/classes. If this is your first time running it, you will see
messages about Scala being dowloaded -- this is fine and
expected. Once that is over, the TextGrounder code will be compiled.

To try out other build targets, do:

$ textgrounder build

This will drop you into the SBT interface. To see the actions that are
possible, hit the TAB key. (In general, you can do auto-completion on
any command prefix in SBT, hurrah!)

Documentation for SBT is here:

https://github.com/harrah/xsbt/wiki

Note: if you have SBT 0.11.0 already installed on your system, you can
also just call it directly with "sbt" in TEXTGROUNDER_DIR.


Trying it out
=============

---------------------------------------------------------
NOTE: See README.geolocate for more detailed instructions.
---------------------------------------------------------

Also check out the web site:

https://bitbucket.org/utcompling/textgrounder/wiki/Home


Bug Reports
===========

Please report bugs by sending mail to jbaldrid at mail.utexas.edu.


Special Note
============

Parts of this README and some of the directory structure and the build
system for this project were borrowed from the JDOM project (kudos!).
See www.jdom.org for more details.


