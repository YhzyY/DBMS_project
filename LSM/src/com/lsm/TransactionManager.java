package com.lsm;

import java.io.*;
import java.util.*;

public class TransactionManager {

    static String scriptDirectory;
    static Scheduler scheduler;

    public TransactionManager(String scriptDirectory, Memory memory) {
        this.scriptDirectory = scriptDirectory;
        scheduler = new Scheduler(memory);
    }



    public static void roundRobin() throws IOException {
        System.out.println("Round Robin Reading: " + scriptDirectory);

        List transactions = new ArrayList();
        Map transactionBuffer = new HashMap<>();
        Map processBuffer = new HashMap<>();

        File filePath = new File(scriptDirectory);
        File[] allFileName = filePath.listFiles();
        List fileList = new ArrayList(Arrays.asList(allFileName));

        System.out.println(fileList);
        for (Object file : fileList){
            FileInputStream inputStream = new FileInputStream(file.toString());
            BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(inputStream));
            String input = bufferedReader.readLine();
            if(input.subSequence(input.length()-2, input.length()-1) == "1"){
//                new transaction (EMode=1)
                System.out.println(1);

            }else{
//                new process (EMode=0)
                System.out.println(0);
            }
//            fileBuffer.put(file.toString(), bufferedReader);
        }


        scheduler.scheduleTransaction(transactions);
//        scheduler.scheduleProcess(processes);


    }




    public static void random(int randomSeed) {
        System.out.println("Random Reading");
//        scheduler.scheduleTransaction(transactions);
//        scheduler.scheduleProcess(processes);

    }
}
