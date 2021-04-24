package Part_Four;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;

import static java.lang.System.exit;

/**
 * This file contains the code for :
 *
 *   1. The Code from part one to part three
 *   2. modifications made in map phase
 *
 * The file is split into three sections
 *    1. Main Method
 *    2. Map Reduce Methods
 *    3. File Handler Methods
 *
 * @Author: Sofia Badalova ( 17311566 )
 */
public class MapReduceAlgorithm {

    /**
     * Main Method Simulating Map and Reduce.
     * The file expects three file addresses to read in.
     * @param args
     */
    public static void main(String[] args) {

        if (args.length < 4) {
            System.err.println("usage: java MapReduceFiles file1.txt file2.txt file3.txt linesPerThread");
            exit(0);
        }

        // parses the input and adds to Map -> ( [file num, text ] , [ file num, text] , [file num, text] )
        Map<String, String> input = new HashMap<String, String>();
        try {
            // Calls readFile which formats the input file as it reads in the contents of the file.
            input.put(args[0], readFile(args[0]));
            input.put(args[1], readFile(args[1]));
            input.put(args[2], readFile(args[2]));
        }
        catch (IOException ex)
        {
            System.err.println("Error reading files...\n" + ex.getMessage());
            ex.printStackTrace();
            exit(0);
        }

        int linesPerThread = Integer.parseInt(args[3]);


        // APPROACH #3 : Distributed MapReduce
        {
            final Map<String, Map<String, Integer>> output = new HashMap<String, Map<String, Integer>>();

            // MAP Phase :

            long start_of_map_phase = System.currentTimeMillis(); // part three code, benchmarking

            final List<MappedItem> mappedItems = new LinkedList<MappedItem>();

            final MapCallback<String, MappedItem> mapCallback = new MapCallback<>() {
                @Override
                public synchronized void mapDone(String file, List<MappedItem> results) {
                    mappedItems.addAll(results);
                }
            };

            /**
             *  >> Mapping Phase Modified <<
             *
             * This phase creates threads per the number of lines in a file.
             * if there are no more lines in a file it creates a thread
             * and then moves onto the next file
             *
             */

            List<Thread> mapCluster = new ArrayList<Thread>(input.size());

            Iterator<Map.Entry<String, String>> inputIter = input.entrySet().iterator();
            // iterate through the files and their contents.
            while(inputIter.hasNext()) {

                Map.Entry<String, String> entry = inputIter.next();
                final String file = entry.getKey();
                final String contents = entry.getValue();

                // split the entire data by lines
                String[] lines = contents.split("\\r?\\n");
                String temp = "";
                int line_number = 0;
                // loop through each of the lines and create subsections.
                for(int i = 0; i <= linesPerThread-1; i++){
                    // fill the temp string until 1000 lines reached
                    temp = temp + lines[line_number] + "\n";

                    if( i == linesPerThread-1){ // base case :  we reached max number of lines per thread

                        final String subsectionOfLines = temp; // the number of lines we want to map per thread

                        i = -1; // reset the counter

                        Thread t = new Thread(() -> map(file,subsectionOfLines,mapCallback));
                        mapCluster.add(t);
                        t.start();
                        temp = "";
                    } else if ((line_number + 1) >= lines.length) { // base case : we reached end of file

                        final String subsectionOfLines = temp; // number of lines we want per thread

                        Thread t = new Thread(() -> map(file,subsectionOfLines,mapCallback));
                        mapCluster.add(t);
                        t.start();

                        // quit the for loop and move onto next file
                        break;
                    }

                    line_number = line_number + 1; // go to next line in array
                }
            }

            // wait for mapping phase to be over:
            for(Thread t : mapCluster) {
                try {
                    t.join();
                } catch(InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }


            long time_taken_for_map_phase = System.currentTimeMillis() - start_of_map_phase; // part three code, benchmarking

            // GROUP:
            long start_of_group_phase = System.currentTimeMillis(); // part three code, benchmarking

            Map<String, List<String>> groupedItems = new HashMap<String, List<String>>();

            Iterator<MappedItem> mappedIter = mappedItems.iterator();
            while(mappedIter.hasNext()) {
                MappedItem item = mappedIter.next();
                String word = item.getWord();
                String file = item.getFile();
                List<String> list = groupedItems.get(word);
                if (list == null) {
                    list = new LinkedList<String>();
                    groupedItems.put(word, list);
                }
                list.add(file);
            }

            long time_taken_for_group_phase = System.currentTimeMillis() - start_of_group_phase; // part three code, benchmarking

            // REDUCE:
            long start_of_reduce_phase  = System.currentTimeMillis(); // part three code, benchmarking

            final ReduceCallback<String, String, Integer> reduceCallback = new ReduceCallback<String, String, Integer>() {
                @Override
                public synchronized void reduceDone(String k, Map<String, Integer> v) {
                    output.put(k, v);
                }
            };

            List<Thread> reduceCluster = new ArrayList<Thread>(groupedItems.size());

            Iterator<Map.Entry<String, List<String>>> groupedIter = groupedItems.entrySet().iterator();
            while(groupedIter.hasNext()) {
                Map.Entry<String, List<String>> entry = groupedIter.next();
                final String word = entry.getKey();
                final List<String> list = entry.getValue();

                Thread t = new Thread(new Runnable() {
                    @Override
                    public void run() {
                        reduce(word, list, reduceCallback);
                    }
                });
                reduceCluster.add(t);
                t.start();
            }

            // wait for reducing phase to be over:
            for(Thread t : reduceCluster) {
                try {
                    t.join();
                } catch(InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
            long time_taken_for_reduce_phase = System.currentTimeMillis() - start_of_reduce_phase; // part three code, benchmarking

            System.out.println(output);

            System.out.println(" ===================================================== ");
            System.out.println("                      statistics                      ");
            System.out.println(" ===================================================== ");
            System.out.println("Time taken for map phase to execute: " + time_taken_for_map_phase + "ms");
            System.out.println("Time taken for group phase to execute: "  + time_taken_for_group_phase + "ms");
            System.out.println("Time taken for reduce phase to execute: " + time_taken_for_reduce_phase + "ms");
            System.out.println(" ===================================================== ");
            System.out.println("                     Input Analysis                    ");
            System.out.println(" ===================================================== ");
            System.out.println("CMD Argument inputs stated to use "  + linesPerThread  + " lines per thread");

        }
    }


    /**
     *
     * ====================================================================================
     *                               Map and Reduce Methods
     * ====================================================================================
     *
     */

    // Map Interface
    public static interface MapCallback<E, V> {

        public void mapDone(E key, List<V> values);
    }

    // Main map function
    public static void map(String file, String contents, MapCallback<String, MappedItem> callback) {
        String[] words = contents.trim().split("\\s+");
        List<MappedItem> results = new ArrayList<MappedItem>(words.length);
        for(String word: words) {
            results.add(new MappedItem(word, file));
        }
        callback.mapDone(file, results);
    }

    // reduce callback
    public static interface ReduceCallback<E, K, V> {

        public void reduceDone(E e, Map<K,V> results);
    }


    // main reduce function
    public static void reduce(String word, List<String> list, ReduceCallback<String, String, Integer> callback) {

        Map<String, Integer> reducedList = new HashMap<String, Integer>();
        for(String file: list) {
            Integer occurrences = reducedList.get(file);
            if (occurrences == null) {
                reducedList.put(file, 1);
            } else {
                reducedList.put(file, occurrences.intValue() + 1);
            }
        }
        callback.reduceDone(word, reducedList);
    }

    // Pair class
    private static class MappedItem {

        private final String word;
        private final String file;

        public MappedItem(String word, String file) {
            this.word = word;
            this.file = file;
        }

        public String getWord() {
            return word;
        }

        public String getFile() {
            return file;
        }

        @Override
        public String toString() {
            return "[\"" + word + "\",\"" + file + "\"]";
        }
    }

    /**
     *
     * ====================================================================================
     *                                File Handler Section
     * ====================================================================================
     *
     */

    // method to read files in..
    private static String readFile(String pathname) throws IOException {
        File file = new File(pathname);
        StringBuilder fileContents = new StringBuilder((int) file.length());
        Scanner scanner = new Scanner(new BufferedReader(new FileReader(file)));
        String lineSeparator = System.getProperty("line.separator");

        try {
            if (scanner.hasNextLine()) {
                // Format the line
                String first_input_line = formatFile(scanner.nextLine());
                fileContents.append(first_input_line);
            }
            while (scanner.hasNextLine()) {
                // format the line
                String line = formatFile(scanner.nextLine());
                fileContents.append(lineSeparator + line);
            }
            return fileContents.toString();
        } finally {
            scanner.close();
        }
    }

    /**
     *  Additional method which parses the unformatted lines and removes any special characters
     *
     * @param unformatted
     * @return
     */
    private static String formatFile(String unformatted) {
        String formatted = "";
        // This removes all special characters, but not spaces naturally present in the file
        // it replaces special characters with empty string.
        formatted = unformatted.replaceAll("[^a-zA-Z ]", "").toLowerCase();
        return formatted;
    }
}
