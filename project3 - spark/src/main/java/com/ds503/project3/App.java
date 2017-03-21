package com.ds503.project3;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.io.FileWriter;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

public class App
{
    static class ValueComparator implements
            Comparator<Tuple2<Integer, Float>>, Serializable {
        final static ValueComparator INSTANCE = new ValueComparator();
        @Override
        public int compare(Tuple2<Integer, Float> t1, Tuple2<Integer, Float> t2) {
            return -t1._2.compareTo(t2._2);
        }
    }

    public static List<Integer> getNeighbours(int x0) {
        List<Integer> result = new ArrayList<Integer>(8);
        if (x0 % 500 !=0) // 1
            result.add(x0 + 1);
        if ((x0 - 1) % 500 !=0) // 2
            result.add(x0 - 1);
        if (x0 - 500 > 0) // 3
            result.add(x0 - 500);
        if (x0 + 500 < 250000) // 4
            result.add(x0+500);
        if((x0 % 500) !=0 && (x0 - 500 > 0)) // 1 && 3
            result.add(x0-500 + 1);
        if ((x0 - 500 > 0) && ((x0 - 1) % 500 !=0)) // 3 && 2
            result.add(x0-500  - 1 );
        if ( (x0 % 500 !=0) && (x0 + 500 < 250000) ) // 1 && 4
            result.add(x0+500 + 1);
        if (((x0 - 1) % 500 !=0) && (x0 + 500 < 250000)) // 2 && 4
            result.add(x0 + 500 - 1);
        return result;
    }

    public static int getNeighboursNumber(int x0) {
        // for better performance, count without creating a list nor using getNeighbours.
        return getNeighbours(x0).size();
    }

    public static void q2(String inputFile, String outputDir) throws IOException {
        // create Spark context with Spark configuration
        JavaSparkContext sc = new JavaSparkContext(new SparkConf().setAppName("Ds503 Project 3"));

        JavaRDD<String> input = sc.textFile( inputFile );
        // the first pair is cell number, 2nd is the density (count)
        JavaPairRDD<Integer, Integer> cellDensity =
                input.mapToPair(new PairFunction<String, Integer, Integer>() {
                                    public Tuple2<Integer, Integer> call(String s) {
                                        String[] point = s.split(",");
                                        int x = Integer.valueOf(point[0].substring(1)); // subtring 1 to avoid the open bracket
                                        int y = Integer.valueOf(point[1].substring(0, point[1].length() -1));  // subtring to len -1 to avoid the close bracket
                                        int cellNumber = (499 - y / 20) * 500 + x / 20 + 1;
                                        return new Tuple2<Integer, Integer>(cellNumber, 1);
                                    }
                                }
                ).reduceByKey(
                        new Function2<Integer, Integer, Integer>(){
                            public Integer call(Integer x, Integer y){ return x + y; }
                        } );

        // send each cell to all cells that require it to calculate the density index.(to all neighbours and to itself)
        JavaPairRDD<Integer, Iterable<Tuple2<Integer, Integer>>> groupNeighboursByCell = cellDensity.flatMapToPair(new PairFlatMapFunction<Tuple2<Integer, Integer>, Integer, Tuple2<Integer, Integer>>() {

            @Override
            public Iterator<Tuple2<Integer, Tuple2<Integer, Integer>>> call(Tuple2<Integer, Integer> cell) throws Exception {
                int x0 = cell._1();
                Tuple2 cellClone = new Tuple2(cell._1(), cell._2());
                List<Tuple2<Integer, Tuple2<Integer, Integer>>> result = new ArrayList<Tuple2<Integer, Tuple2<Integer, Integer>>>(8);

                result.add(new Tuple2<Integer, Tuple2<Integer, Integer>>(x0, cellClone));
                List<Integer> neighbours = getNeighbours(x0);
                for (Integer neighbour : neighbours) {
                    result.add(new Tuple2<Integer, Tuple2<Integer, Integer>>(neighbour, cellClone));
                }
                return result.iterator();
            }
        }).groupByKey();

        JavaPairRDD<Integer, Float> cellDensityPairsRDD = groupNeighboursByCell.mapToPair(new PairFunction<Tuple2<Integer, Iterable<Tuple2<Integer, Integer>>>, Integer, Float>() {
            @Override
            public Tuple2<Integer, Float> call(Tuple2<Integer, Iterable<Tuple2<Integer, Integer>>> cell_neighbours) throws Exception {
                int cell = cell_neighbours._1();
                int cell_density = 0;

                int sum = 0;
                for (Tuple2<Integer, Integer> t : cell_neighbours._2()) {
                    if (t._1() == cell) {
                        cell_density = t._2();
                        continue;
                    }
                    sum += t._2();
                }
                int count = getNeighboursNumber(cell);
                float avg = ((float)sum) / count;
                float relativeDensity = avg != 0.0f ? (cell_density / avg): 0.0f;
                return new Tuple2<Integer, Float>(cell, relativeDensity);
            }
        });

//        // sort by value, swap key,value, user sort by key, then swap again to orginal
//        JavaPairRDD<Integer, Float> cellDensityPairsSortedRDD = cellDensityPairsRDD.mapToPair(new PairFunction<Tuple2<Integer, Float>, Float, Integer>() {
//            @Override
//            public Tuple2<Float, Integer> call(Tuple2<Integer, Float> integerFloatTuple2) throws Exception {
//                return integerFloatTuple2.swap();
//            }
//        }).sortByKey(false).mapToPair(new PairFunction<Tuple2<Float, Integer>, Integer, Float>() {
//            @Override
//            public Tuple2<Integer, Float> call(Tuple2<Float, Integer> floatIntegerTuple2) throws Exception {
//                return floatIntegerTuple2.swap();
//            }
//        });

        List<Tuple2<Integer, Float>> top50 = cellDensityPairsRDD.takeOrdered(50, ValueComparator.INSTANCE);

        // output the whole result on a text file (not required on the HW)
        //cellDensityPairsSortedRDD.saveAsTextFile(outputDir + "All_cells_density");

        // report the TOP 50
        FileWriter writer = new FileWriter(outputDir + "Top_50_cells_density.txt");
        for(Tuple2<Integer, Float> tuple2: top50) {
            writer.write(tuple2.toString() + System.lineSeparator());
        }
        writer.close();

        /* step 3:
         * Continue over the results from Step 2, and for each of the reported top 50 grid cells, report the IDs and
         * the relative-density indexes of its neighbor cells.
         */

        List<Tuple2<Integer, Integer>> top50neighbours = new ArrayList<Tuple2<Integer, Integer>>(400);
        for(Tuple2<Integer, Float> tuple2: top50) {
            for (int n : getNeighbours(tuple2._1())) {
                top50neighbours.add(new Tuple2<Integer, Integer>(n,tuple2._1()));
            }
        }

        JavaPairRDD<Integer, Integer>  top50neighboursRdd = sc.parallelizePairs(top50neighbours);

        JavaPairRDD<Integer, Float> Top50NeighboursRDD = cellDensityPairsRDD.join(top50neighboursRdd).mapToPair(new PairFunction<Tuple2<Integer, Tuple2<Float, Integer>>, Integer, Float>() {
            @Override
            public Tuple2<Integer, Float> call(Tuple2<Integer, Tuple2<Float, Integer>> integerTuple2Tuple2) throws Exception {
                return new Tuple2<Integer, Float>(integerTuple2Tuple2._1(), integerTuple2Tuple2._2()._1());
            }
        });

        Top50NeighboursRDD.saveAsTextFile(outputDir + "Top_50_Neighbours_cells_density");
    }

    public static void main( String[] args )
    {
        String outputDir = "/tmp/";
        if( args.length == 0 )
        {
            System.out.println( "Usage: <file>" );
            System.exit( 0 );
        }

        if( args.length > 1 )
            outputDir = args [1];

        try {
            q2 ( args[ 0 ], outputDir);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}