RUNNING THE PROJECT CODE 

Type: 
    sbt 'runMain FindDupWordsInFiles'

at the shell prompt.


ABOUT THE PROJECT 

This project illustrates how customized implementations of 
org.apache.hadoop.mapreduce.RecordReader can be used in conjunction 
with Spark Streaming to create reports which trigger during
major and minor batch intervals and, which:

* monitor a directory for newly arriving files,

* and, for each minor batch interval emit a report of all 
    the duplicate words found in the files that have been dropped 
    into that directory. 

For each duplicate *word* found, the report for the minor batch interval will
detail the number of times that *word* was found across all files,
together with a list of "occurence details"  consisting of 
the name of the file and the line number (in that file) that describes
exactly where a  particular occurence of *word* was found.

The major batch interval report will detail the total number of occurrences
of each duplicate word found in all preceding minor batch intervals that 
comprise the major batch interval.

Note that if a word was not flagged as a duplicate in any of the minor batch 
intervals, it will *not* be considered a duplicate *even if* it happened to occur in 
in some file during two or more minor batch intervals. For the word to be 
considered a duplicate, and for it's occurrence to be counted on the 
major batch interval report, it must have occured as a 
dup in at least one minor batch interval (in other words: only duplicates within
such intervals will count toward the total in the major interval report.)


Below is an example of streaming data input, and the resultant output
we would expect from the program. Note that for documentation 
purposes the '#' character denotes a comment and should not be considered 
part of the data (although the actual program does NOT support comments.)


We assume a major interval every 2 minor intervals 

~~~~

---- minor interval 1 -----
    file: foo1
    contents:
        a brown cow
        cow now
    file: foo2
    contents:
        cat cow
        how now dish



    <report for minor interval 1 >
    cow: 3   -   foo1/1 foo1/2  foo2/1
    now: 2   -   foo1/2  foo2/2

---- minor interval 2 -----
    file: bar1
    contents:
        fish dish now
        fish wish
        cow  now  #  cow is not a dup in this cycle. 
                  #  It will therefore not appear in the report for this minor interval.
                  #  Similarly, this occurence of 'cow' will not counted in the major interval summary

    file: bar2
    contents:<empty>

    <report for minor interval 2 >
    now: 3   -   bar1/1 bar1/3
    fish: 2  -   bar1/1 bar1/2


---- minor interval 3 -----


    file: hoho1
    contents:
        ring ring


    <summary report for first major batch interval (subsuming minor intervals 1 and 2)>
    cow: 3
    now: 5
    fish: 2


    <report for minor interval 3 >
    ring: 2   -   hoho1/1 hoho1/1

~~~~



