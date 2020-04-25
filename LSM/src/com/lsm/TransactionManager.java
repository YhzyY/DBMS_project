package com.lsm;

import java.io.*;
import java.util.*;
import java.text.DecimalFormat;

public class TransactionManager {

    static String scriptDirectory;
    static Scheduler scheduler;
    long startTime;

    public TransactionManager(String scriptDirectory, Memory memory) {
        this.scriptDirectory = scriptDirectory;
        scheduler = new Scheduler(memory);
    }

    public void readTransactions(String readMode, long randomSeed, int maxLines) throws IOException {
        startTime = System.currentTimeMillis();
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
                    String s = file.toString().substring(nameBegin);
                    s = s.substring(s.lastIndexOf("\\")+1);
                    transactionList.add(new ArrayList<>(Arrays.asList(s, input)));
                } else {
                    processList.add(new ArrayList<>(Arrays.asList(file.toString().substring(nameBegin), input)));
                }
            }
        }
        scheduler.scheduleTransaction(transactionList);
        scheduler.scheduleProcess(processList);
        statistic(transactionList);
    }

    private void statistic(List History){
        long endTime = System.currentTimeMillis();
        DecimalFormat fnum = new DecimalFormat("#0.000");
        Map<String,Integer> map = new HashMap<String,Integer>();
        Map<String,Integer> submap = new HashMap<String,Integer>();
        int count = 0;
        for (Object t : History) {
            String operation = t.toString().split(",")[1].substring(1,2);
            String transaction_id = t.toString().split(",")[0].substring(1);
            String tag = operation + transaction_id;
            if(!map.containsKey(tag)) {
                map.put(tag, 1);
            }
            else{
                map.put(tag, map.get(tag)+1);
            }
            count++;
            if(operation.equals("A")) {
                Iterator<Map.Entry<String, Integer>> it = map.entrySet().iterator();
                while (it.hasNext()) {
                    Map.Entry<String, Integer> entry = it.next();
                    String s = entry.getKey().substring(1);
                    if (s.equals(transaction_id)) {
                        count -= entry.getValue();
                        it.remove();
                    }
                }
            }
        }
        System.out.println();
        System.out.println("_______________Statistic______________");
        List list = new ArrayList();
        for (Map.Entry<String, Integer> entry : map.entrySet()) {
            String operation = entry.getKey().substring(0,1);
            String id = entry.getKey().substring(1);
            if(!submap.containsKey(operation)) {
                submap.put(operation, entry.getValue());
            }
            else{
                submap.put(operation, submap.get(operation)+entry.getValue());
            }
            if(!list.contains(id)){
                list.add(id);
            }
        }
        System.out.println("Number of committed transactions: " + list.size());
        System.out.println("The percentage of the operations:");
        for (Map.Entry<String, Integer> entry : submap.entrySet()) {
            double percentage = entry.getValue() * 1.0 / count;
            String d = fnum.format(percentage);
            System.out.println(entry.getKey() + ":" + d);
        }
        System.out.println("The execution time：" + (endTime - startTime) + "ms");
        double average = (endTime - startTime) * 1.0 / count;
        String d = fnum.format(average);
        System.out.println("The average response time：" + d + "ms");
        System.out.println("______________________________________");
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
