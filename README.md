# DS--Map-Reduce-
Assignment 2 Mapreduce multi threading 

## Part One 
This part involves finding three large PDF files and converting them to text using easypdf.com
The three large PDFS that were found were :
- The Three Musketeers
- The Odyssey 
- the count of monte cristo

## Part Two
This involves parsing the input from the files and removing any special characters. The output of this step leaves the files with words and spaces

## Part Three
This step involves setting up timers for the different phases in the Map Reduce Algorithm. 
It is used to test how long in milliseconds each phase takes and outputs the statisticsat the end

## Part Four 
This involves modifying the mapping phase in order to create threads per the number of lines in the files. 
The code accepts a fourth command line argument which is the number of lines per thread. 

## Part Five 
This involves modifying the reduce phase in order to create threads per the number of words. 
Another command line argument is accepted which determies the number of words per thread. It creates the appropriate amount of threads and calls the reduce method on them. 

## Part Six
This involves testing the code generated to see which combination of lines per thread and words per thread provide the best results. 
