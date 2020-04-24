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

    public void readTransactions(String readMode, long randomSeed, int maxLines) throws IOException {
        Map transactionBuffer = new HashMap<>();
        Map transactionMode = new HashMap<>();

        File filePath = new File(scriptDirectory);
        List list = new ArrayList();
        List fileList = getFiles(filePath, list);
//        System.out.println(fileList);

        for (Object file : fileList) {
            FileInputStream inputStream = new FileInputStream(file.toString());
            BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(inputStream));
            String input = bufferedReader.readLine();
            String EMode = input.substring(input.length() - 1);
            if (EMode.equals("1")) {  //new transaction (EMode=1)
                transactionMode.put(file, 1);
                transactionBuffer.put(file, bufferedReader);
            } else if (EMode.equals("0")) {  //new process (EMode=0)
                transactionMode.put(file, 0);
                transactionBuffer.put(file, bufferedReader);
            }
        }
        if (readMode.equals("roundRobin")) {
            roundRobin(fileList, transactionBuffer, transactionMode);
        } else {
            random(fileList, transactionBuffer, transactionMode, randomSeed, maxLines);
        }
    }

    private List getFiles(File filePath, List list) {
        File[] fs = filePath.listFiles();
        for(File f:fs){
            if(f.isDirectory())	//若是目录，则递归打印该目录下的文件
                getFiles(f, list);
            if(f.isFile())		//若是文件，直接打印
                list.add(f);
        }
        return list;
    }


    private void roundRobin(List fileList, Map transactionBuffer, Map transactionMode) throws IOException {
        System.out.println("Round Robin Reading: " + scriptDirectory);
        List transactionList = new ArrayList();
        List processList = new ArrayList();

        while (!transactionMode.isEmpty()) {
            for (int i = 0; i < fileList.size(); i++) {
                Object file = fileList.get(i);
                BufferedReader buffer = (BufferedReader) transactionBuffer.get(file);
                if (buffer == null) continue;
                String input = buffer.readLine();
                if (input == null) {
                    fileList.remove(file);
                    transactionBuffer.remove(file);
                    transactionMode.remove(file);
                    continue;
                }
                int nameBegin = file.toString().lastIndexOf("/") + 1;
                if (transactionMode.get(file).toString().equals("0")) {
                    transactionList.add(new ArrayList<>(Arrays.asList(file.toString().substring(nameBegin), input)));
                } else {
                    processList.add(new ArrayList<>(Arrays.asList(file.toString().substring(nameBegin), input)));
                }
            }
        }
        scheduler.scheduleTransaction(transactionList);
        scheduler.scheduleProcess(processList);
    }


    private void random(List fileList, Map transactionBuffer, Map transactionMode, long randomSeed, int maxLines) throws IOException {
        System.out.println("Random Reading: " + scriptDirectory);
        List transactionList = new ArrayList();
        List processList = new ArrayList();
        Random ran = new Random(randomSeed);
        while (!transactionMode.isEmpty()) {
            int fileIndex = ran.nextInt(transactionMode.size());
            Object fileName = fileList.get(fileIndex);
            BufferedReader buffer = (BufferedReader) transactionBuffer.get(fileName);
            if (buffer == null){
                fileList.remove(fileName);
                continue;
            }
            int nameBegin = fileName.toString().lastIndexOf("/") + 1;
            int readRow = ran.nextInt(maxLines);
            while(readRow > 0){
                String input = buffer.readLine();
                if (input == null) {
                    transactionBuffer.remove(fileName);
                    transactionMode.remove(fileName);
                    fileList.remove(fileName);
                    break;
                }
                if (transactionMode.get(fileName).toString().equals("0")) {
                    transactionList.add(new ArrayList<>(Arrays.asList(fileName.toString().substring(nameBegin), input)));
                } else {
                    processList.add(new ArrayList<>(Arrays.asList(fileName.toString().substring(nameBegin), input)));
                }
                readRow--;
            }
        }
        scheduler.scheduleTransaction(transactionList);
        scheduler.scheduleProcess(processList);
    }
}
