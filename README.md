HackReduce
==========

http://www.hackreduce.org


Prerequisites
-------------
* Java 1.6+
* Gradle (http://www.gradle.org/installation.html) or Ant
* Git


Run an example job locally
--------------------------

1. git clone git://github.com/hoppertravel/HackReduce.git
   - Note: You should periodically run "git pull" from within the project directory to update your code.

2. cd HackReduce

3. Build the project depending on what tool you have installed:

    Gradle:

        $ gradle

    Ant:

        $ ant

4. Try running an example from the list below


Examples
--------

Run any of the following commands in your CLI, and after the job's completed, check the /tmp/* folder for the output.

**Bixi:**

    $ java -classpath ".:build/libs/HackReduce-0.2.jar:lib/*" org.hackreduce.examples.bixi.RecordCounter datasets/bixi /tmp/bixi_recordcounts

**NASDAQ:**

    $ java -classpath ".:build/libs/HackReduce-0.2.jar:lib/*" org.hackreduce.examples.stockexchange.HighestDividend datasets/nasdaq/dividends /tmp/nasdaq_dividends
    $ java -classpath ".:build/libs/HackReduce-0.2.jar:lib/*" org.hackreduce.examples.stockexchange.MarketCapitalization datasets/nasdaq/daily_prices /tmp/nasdaq_marketcaps
    $ java -classpath ".:build/libs/HackReduce-0.2.jar:lib/*" org.hackreduce.examples.stockexchange.RecordCounter datasets/nasdaq/daily_prices /tmp/nasdaq_recordcounts

**NYSE:**

    $ java -classpath ".:build/libs/HackReduce-0.2.jar:lib/*" org.hackreduce.examples.stockexchange.HighestDividend datasets/nyse/dividends /tmp/nyse_dividends
    $ java -classpath ".:build/libs/HackReduce-0.2.jar:lib/*" org.hackreduce.examples.stockexchange.MarketCapitalization datasets/nyse/daily_prices /tmp/nyse_marketcaps
    $ java -classpath ".:build/libs/HackReduce-0.2.jar:lib/*" org.hackreduce.examples.stockexchange.RecordCounter datasets/nyse/daily_prices /tmp/nyse_recordcounts

**Flights:**

    $ java -classpath ".:build/libs/HackReduce-0.2.jar:lib/*" org.hackreduce.examples.flights.RecordCounter datasets/flights /tmp/flights_recordcounts

**Wikipedia:**

    $ java -classpath ".:build/libs/HackReduce-0.2.jar:lib/*" org.hackreduce.examples.wikipedia.RecordCounter datasets/wikipedia /tmp/wikipedia_recordcounts

**1gram:**

    $ java -classpath ".:build/libs/HackReduce-0.2.jar:lib/*" org.hackreduce.examples.ngram.one_gram.RecordCounter datasets/ngram/1gram /tmp/1gram_recordcounts

**2gram:**

    $ java -classpath ".:build/libs/HackReduce-0.2.jar:lib/*" org.hackreduce.examples.ngram.two_gram.RecordCounter datasets/ngram/2gram /tmp/2gram_recordcounts

Note: The jobs are made for the specific datasets, so pairing them up properly is important. The second argument (/tmp/*) is just a made up output path for the results of the job, and can be modified to anything you want.


Datasets
--------

* Bixi (Courtesy of Fabrice)
* Flights (Courtesy of Hopper)
* NASDAQ daily prices and dividends (http://www.infochimps.com/datasets/daily-1970-2010-open-close-hi-low-and-volume-nasdaq-exchange)
* NYSE daily prices and dividends (http://www.infochimps.com/datasets/daily-1970-2010-open-close-hi-low-and-volume-nyse-exchange)
* Wikipedia XML (http://en.wikipedia.org/wiki/Wikipedia:Database_download#English-language_Wikipedia)
* Google Ngram (http://ngrams.googlelabs.com/datasets)

Take a look at the datasets/ folder to see samples subsets of these datasets.


Running on a Hadoop cluster
---------------------------
If you'd like to set up an actual Hadoop cluster (single or multi node), then follow the instructions on the Hadoop wiki: http://wiki.apache.org/hadoop/QuickStart

If you're running the HackReduce virtual image, or you're trying to run the example job against an actual install of Hadoop, the process should be similar. However, you'll need to upload the datasets folder into HDFS (if you're running it), or just make a note of the path.

Example:

    $ bin/hadoop jar <path_to_hackreduce_jar>/HackReduce-0.2.jar org.hackreduce.examples.stockexchange.RecordCounter <path to>/datasets/nyse/daily_prices /tmp/nyse_recordcounts


Setting up for development
--------------------------

### Gradle (recommended):

We recommend using Gradle for easy set up of the project in Eclipse, Idea, or other IDEs through Gradle plugins (http://www.gradle.org/standard_plugins.html). To use it, simply run one of the following:

    $ gradle eclipse

    $ gradle idea

Then import the project into your IDE of choice. This will download all of the dependencies (including sources) and create the necessary project files.


### Manual setup:

You can also bring in the project manually into your IDE and then include all the *.jar files from the **lib** folder of the project.

