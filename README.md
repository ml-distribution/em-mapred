#Distributed Expectation Maximization#

Jake Leichtling '14

Derek Salama '14

Computational Linguistics (COSC 73), Fall 2013

Professor Sravana Reddy

The MapReduce directory contains the principle output of our project, i.e. a distributed expectation maximization
program to estimate hidden markov model parameters. The source code can be found in the src directory, with corresponding
documenation in the doc directory. The bin directory contains the program jar, which has been compiled from the source
files linked with Hadoop version 2.2.0.

##Compiling##

In order to compile the source files, the build path must contain the hadoop-common-<version>.jar,
hadoop-hdfs-<version>.jar, and hadoop-mapreduce-client-core-<version>.jar libraries, which we have included in the
HadoopJars directory.

Please note that we have included a precompiled jar in the MapReduce/bin/ directory. You will only need to compile
if you would like to make modifications.

##Running##

The current implementation of distributed EM must be run on an Amazon ElasticMapreduce (EMR) cluster with an Amazon S3
distributed filesystem beneath it. The program jar, input corpora, and paramater seed files must be uploaded
to the S3 filesystem prior to running.

To run the jar, find a running EMR cluster or create a new one, and ensure that the cluster is using the Amazon Hadoop
distribution marked as "latest" (this was "2.4.2 (Hadoop 1.0.3) - latest" during our development). Add a custom jar
step and point it to the distributed EM jar (which can be found pre-compiled in the MapReduce/bin/ directory). 
The arguments are described in the EMDriver.java javadoc, but are repeated here for conevenience:

###Arguments###

0: Job name, e.g. "distributed-hmm-em"

1: The bucket URI, e.g. "s3n://distributed-hmm-em/" for the file system

2: Path to input directory containing emission sequences (one per line), e.g. "s3n://distributed-hmm-em/input"

3: Path to output directory that does not yet exist, e.g. "s3n://distributed-hmm-em/output-13"

4: Path to transitions file (each line of format "<from_state> <to_state>", with the first from_state
being the symbol for the start of the emission sequence, e.g. "s3n://distributed-hmm-em/transitions.txt"
     
5: Path to emissions file (each line of format "<state> <token>", e.g. "s3n://distributed-hmm-em/emissions.txt"

6: Log convergence, i.e. difference between log alpha of EM iterations before final output is produced,
e.g. "0.01"

7: Max number of EM iterations, or -1 for no maximum, e.g. "10"

8: Number of different random seeds for model parameters, e.g. "5"

9: Flag to enable Viterbi tagging following EM, e.g. a nonzero int (e.g. "1" or "-1") to enable and "0" to disable

##Documentation##

The javadoc documentation for the source code can be found in the MapRedue/doc directory. This documentation can be
opened for easy consumption by opening the index.html file in your favorite web browser.

Additionally, our project writeup FinalWriteup.pdf is located in the root directory.

##Conclusion##

We hope you find this program useful for speeding up your expectation maximization endeavours! Please email us
with any questions.

jake.leichtling@gmail.com

dereksalama@gmail.com
