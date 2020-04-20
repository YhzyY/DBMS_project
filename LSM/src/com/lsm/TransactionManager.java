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

        List transactionList = new ArrayList();
        List processeList = new ArrayList();
        Map transactionBuffer = new HashMap<>();
        Map transactionMode = new HashMap<>();
        boolean finishReading = false;

        File filePath = new File(scriptDirectory);
        File[] allFileName = filePath.listFiles();
        List fileList = new ArrayList(Arrays.asList(allFileName));

        System.out.println(fileList);
        for (Object file : fileList){
            FileInputStream inputStream = new FileInputStream(file.toString());
            BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(inputStream));
            String input = bufferedReader.readLine();
            String EMode = input.substring(input.length()-1);
            if(EMode.equals("1")){  //new transaction (EMode=1)
                transactionMode.put(file, 1);
                transactionBuffer.put(file, bufferedReader);
            }else if (EMode.equals("0")){  //new process (EMode=0)
                transactionMode.put(file, 0);
                transactionBuffer.put(file, bufferedReader);
            }
        }

        while(!finishReading){
            for(Object file : fileList){
                BufferedReader buffer = (BufferedReader) transactionBuffer.get(file);
                if(buffer == null) continue;
                String input = buffer.readLine();
                if(input == null){
                    transactionBuffer.remove(file);
                    transactionMode.remove(file);
                    continue;
                }
                int nameBegin = file.toString().lastIndexOf("/") + 1;
                if(transactionMode.get(file).toString().equals("0")){
                    transactionList.add(new ArrayList<>(Arrays.asList(file.toString().substring(nameBegin), input)));
                }else{
                    processeList.add(new ArrayList<>(Arrays.asList(file.toString().substring(nameBegin), input)));
                }
            }
            if(transactionMode.isEmpty()) finishReading = true;
        }
        scheduler.scheduleTransaction(transactionList);
        scheduler.scheduleProcess(processeList);

    }




    public static void random(int randomSeed) {
        System.out.println("Random Reading");
//        scheduler.scheduleTransaction(transactions);
//        scheduler.scheduleProcess(processes);

    }
}
