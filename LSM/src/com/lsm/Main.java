package com.lsm;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

public class Main {
//current directory: System.getProperty("user.dir")

    public static void main(String[] args) throws IOException {
        String scriptDirectory = "./src/com/lsm/scripts"; // the directory contains all the scripts
        Scanner myObj = new Scanner(System.in);
        System.out.println("Enter readMode (either 'random or 'roundRobin'):");
        String readMode = myObj.nextLine();  //either "random" or "roundRobin"
        if((!readMode.equals("roundRobin")) && (!readMode.equals("random"))){
            System.out.println("Wrong reading mode in Main.java");
            System.exit(1);
        }
        System.out.println("Enter cacheCapacity (>0):");
        int cacheCapacity = myObj.nextInt();  // buffer size for the Data Manager
        System.out.println("Enter SSTableCapacity (>0):");
        int SSTableCapacity = myObj.nextInt();  // buffer size for the Data Manager

        if((cacheCapacity <= 0) || (SSTableCapacity <= 0)){
            System.out.println("buffer size cannot be smaller than 1");
            System.exit(1);
        }
        Memory memory = new Memory(cacheCapacity, SSTableCapacity);
        TransactionManager transactionManager = new TransactionManager(scriptDirectory, memory);
        if(readMode.equals("roundRobin")){
            transactionManager.readTransactions(readMode, 0, 0);
        }else {
            System.out.println("Enter randomSeed:");
            long randomSeed = myObj.nextLong();  // seed of the random number generator
            System.out.println("Enter maxLines to be read in one random run:");
            int maxLines = myObj.nextInt();  // maxLines to be read in one random run
            transactionManager.readTransactions(readMode, randomSeed, maxLines);
        }

//  TODO:  Phase1:  You can comment all the above code and uncomment all the below code to see how LSM works without Transaction Manager and Scheduler
/**
        long startTime=System.currentTimeMillis();  
        final int cacheCapacity = 1;
        final int SSTableCapacity = 1;
        final PrintStream out = new PrintStream("log.txt");
        System.setOut(out);

        FileInputStream inputStream = new FileInputStream("./src/com/lsm/script.txt");
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
**/
    }
}
