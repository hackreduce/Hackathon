HackReduce
==========

This repository and the accompanying wiki include all the material needed for Hack/Reduce events. It's also a great place to start learning about Hadoop and Map/Reduce. 
http://www.hackreduce.org

----

***There are two parts of information for Hack/Reduce and for Learning Hadoop and Mapreduce on the git repo and github page:***

1. This README provides information on how to work with the Java project code in this repository and do local development.

2. The wiki on https://github.com/hackreduce/Hackathon/wiki has more general material and event-specific instructions:
    
    1. A presentation introducing Mapreduce
    
    2. Event specific Cluster Setup and information on how to run your jobs
    
    3. List of datasets available on the clusters
    
    4. Instructions for using Hadoop Streaming
    
    5. Instructions for Windows users
    
    6. Links to code from previous Hack/Reduce projects


Wiki
----

https://github.com/hackreduce/Hackathon/wiki


Prerequisites
-------------
* Git
* Java 1.6
* Build tool (can use either one):
    * Gradle (http://www.gradle.org/installation.html)
    * Ant


Datasets
--------

https://github.com/hackreduce/Hackathon/wiki/Datasets

Take a look at the datasets/ folder to see samples subsets of these datasets.


Run an example job locally
--------------------------

1. `git clone git://github.com/hackreduce/Hackathon.git`
   (you should occasionally run "git pull" from within the project directory to update your code)

2. `cd Hackathon`

3. Build the project depending on what tool you have installed:

    Gradle:

        $ gradle

    **OR**

    Ant:

        $ ant

4. Try running any of the **examples** from the section below


Setting up for development
--------------------------

After downloading the source files, you can start working with them in your favourite IDE using one of these methods:

### Gradle (recommended):

We recommend using Gradle for easy set up of the project in Eclipse, Idea, or other IDEs through Gradle plugins (http://www.gradle.org/standard_plugins.html). To use it, simply run one of the following:

    $ gradle eclipse

    $ gradle idea

Then import the project into your IDE of choice. This will download all of the dependencies (including sources) and create the necessary project files.


### Manual setup:

You can also bring in the project manually into your IDE and then include all the *.jar files from the **lib** folder of the project.


Examples
--------

Run any of the following commands in your CLI, and after the job's completed, check the /tmp/* folder for the output.

**Bixi:**

    $ java -classpath ".:build/libs/HackReduce-0.3.jar:lib/*" org.hackreduce.examples.bixi.RecordCounter datasets/bixi /tmp/bixi_recordcounts

**NASDAQ:**

    $ java -classpath ".:build/libs/HackReduce-0.3.jar:lib/*" org.hackreduce.examples.stockexchange.HighestDividend datasets/nasdaq/dividends /tmp/nasdaq_dividends
    $ java -classpath ".:build/libs/HackReduce-0.3.jar:lib/*" org.hackreduce.examples.stockexchange.MarketCapitalization datasets/nasdaq/daily_prices /tmp/nasdaq_marketcaps
    $ java -classpath ".:build/libs/HackReduce-0.3.jar:lib/*" org.hackreduce.examples.stockexchange.RecordCounter datasets/nasdaq/daily_prices /tmp/nasdaq_recordcounts

**NYSE:**

    $ java -classpath ".:build/libs/HackReduce-0.3.jar:lib/*" org.hackreduce.examples.stockexchange.HighestDividend datasets/nyse/dividends /tmp/nyse_dividends
    $ java -classpath ".:build/libs/HackReduce-0.3.jar:lib/*" org.hackreduce.examples.stockexchange.MarketCapitalization datasets/nyse/daily_prices /tmp/nyse_marketcaps
    $ java -classpath ".:build/libs/HackReduce-0.3.jar:lib/*" org.hackreduce.examples.stockexchange.RecordCounter datasets/nyse/daily_prices /tmp/nyse_recordcounts

**Flights:**

    $ java -classpath ".:build/libs/HackReduce-0.3.jar:lib/*" org.hackreduce.examples.flights.RecordCounter datasets/flights /tmp/flights_recordcounts

**Freebase:**

    $ java -classpath ".:build/libs/HackReduce-0.3.jar:lib/*" org.hackreduce.examples.freebase.TopicCounter datasets/freebase/topics /tmp/fb_topiccounts
    $ java -classpath ".:build/libs/HackReduce-0.3.jar:lib/*" org.hackreduce.examples.freebase.QuadCounter datasets/freebase/quadruples /tmp/fb_quadcounts
    $ java -classpath ".:build/libs/HackReduce-0.3.jar:lib/*" org.hackreduce.examples.freebase.NameLengths datasets/freebase/topics /tmp/fb_namelengths

**Wikipedia:**

    $ java -classpath ".:build/libs/HackReduce-0.3.jar:lib/*" org.hackreduce.examples.wikipedia.RecordCounter datasets/wikipedia /tmp/wikipedia_recordcounts

**Google 1gram:**

    $ java -classpath ".:build/libs/HackReduce-0.3.jar:lib/*" org.hackreduce.examples.ngram.one_gram.RecordCounter datasets/ngrams/1gram /tmp/1gram_recordcounts

**Google 2gram:**

    $ java -classpath ".:build/libs/HackReduce-0.3.jar:lib/*" org.hackreduce.examples.ngram.two_gram.RecordCounter datasets/ngrams/2gram /tmp/2gram_recordcounts

**MSD:**

    $ java -classpath ".:build/libs/HackReduce-0.3.jar:lib/*" org.hackreduce.examples.msd.RecordCounter datasets/msd /tmp/msd_recordcounts

Note: The jobs are made for the specific datasets, so pairing them up properly is important. The second argument (/tmp/*) is just a made up output path for the results of the job, and can be modified to anything you want.


Streaming example
-----------------

* Python

        $ java -classpath ".:lib/*" org.apache.hadoop.streaming.HadoopStreaming -input datasets/nasdaq/daily_prices/ -output /tmp/py_streaming_count -mapper streaming/nasdaq_counter.py -reducer aggregate

* Ruby

        $ java -classpath ".:lib/*" org.apache.hadoop.streaming.HadoopStreaming -input datasets/nasdaq/daily_prices/ -output /tmp/rb_streaming_count -mapper streaming/nasdaq_counter.rb -reducer aggregate

`-reducer aggregate` is a built-in function for streaming.

