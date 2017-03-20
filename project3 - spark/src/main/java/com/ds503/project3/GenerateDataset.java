package com.ds503.project3;

import java.io.PrintWriter;
import java.util.Random;

/**
 * Created by yousef fadila on 20/03/2017.
 */
public class GenerateDataset {
    static final String FILE_NAME = "p_dataset";
    static final int MIN = 1;
    static final int MAX = 10000;
    int numOfRecords;
    static Random rnd = new Random();

    public GenerateDataset(int numOfRecords) {
        this.numOfRecords = numOfRecords;
    }
    public int randomInt( int min, int max ) {
        return rnd.nextInt(max - min + 1) + min;
    }
    public void generate_nb(int nb, String fname ) {
        try{
            int id = 0;
            PrintWriter writer = new PrintWriter(fname, "UTF-8");
            while (id <nb) {
                StringBuilder sb = new StringBuilder();
                sb.append(randomInt(MIN,MAX)).append(",")
                        .append(randomInt(MIN,MAX));
                writer.println(sb.toString());
                id++;
            }
            writer.close();
        } catch (Exception e) {
            // do something
        }
    }

    public void generate() {
        try{
            int id = 1;
            PrintWriter writer = new PrintWriter(FILE_NAME, "UTF-8");
            while (id <=numOfRecords) {
                StringBuilder sb = new StringBuilder();
                sb.append("("+randomInt(MIN,MAX)).append(",")
                        .append(randomInt(MIN,MAX)+")");
                writer.println(sb.toString());
                id++;
            }
            writer.close();
        } catch (Exception e) {
            // do something
        }
    }

    public static void main(String[] args) throws Exception {
        new GenerateDataset(10000).generate();
    }
}
