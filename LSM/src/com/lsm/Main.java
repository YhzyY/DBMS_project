package com.lsm;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

public class Main {


    public static void main(String[] args) throws IOException {
        long startTime=System.currentTimeMillis();  
        final int cacheCapacity = 2;
        final int SSTableCapacity = 2;
        final PrintStream out = new PrintStream("log.txt");
        System.setOut(out);

        //BufferedReader是可以按行读取文件
        FileInputStream inputStream = new FileInputStream("./src/com/lsm/script");
        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(inputStream));

        String action;
        String table;
        String data = "delete";
        String input;
        int numrecord = 0;
        int wnum = 0;
        int rnum = 0;
        long wtime = 0;
        long rtime = 0;
        Memory memory = new Memory(cacheCapacity, SSTableCapacity);
        while ((input = bufferedReader.readLine()) != null) {
            long instime=System.currentTimeMillis(); 
            System.out.println(input);
            String[] current = input.split(" ",3);
            action = null;
            table = null;
            data = "delete";
            action = current[0];
            table = current[1];
            if(current.length == 3)
                data = current[2];
            if(action.equals("W")){
//                System.out.println("-------write--------");
                memory.write(table, data);
                long inetime=System.currentTimeMillis();
                wtime += inetime - instime;
                wnum++; 
            }else if(action.equals("E")){
//                System.out.println("-------erase--------");
                memory.erase(table, data);
            }else if(action.equals("D")){
//                System.out.println("-------delete--------");
                memory.delete(table);
            }else if(action.equals("R")){
//                System.out.println("-------read ID--------");
                memory.readID(table, data);
                long inetime=System.currentTimeMillis();
                rtime += inetime - instime;
                rnum++; 
            }else if(action.equals("M")){
//                System.out.println("-------read area code--------");
                memory.readAreaCode(table, data);
            }
            numrecord++;
        }

//        System.out.println("---------------");
//        memory.printTables();

        //close
        inputStream.close();
        bufferedReader.close();
        long endTime=System.currentTimeMillis();
        System.out.println("-------------------------");
        System.out.println("Time: " + (endTime-startTime) + "ms");
        System.out.println("The total throughput: " + numrecord*1000.0/(endTime-startTime));
        System.out.println("The read throughput: " + rnum*1000.0/rtime);
        System.out.println("The write throughput: " + wnum*1000.0/wtime);
    }
}
