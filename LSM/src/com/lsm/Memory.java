package com.lsm;

import java.io.*;
import java.util.*;

public class Memory{
//    tableMap stores the table name and its memtable
    static Map tableMap = new HashMap();
//    tableNum stores the table name and the number of its sstable in level 0
    static Map tableNum = new HashMap();

    static int cacheCapacity;
    static Cache cache;
    static int ssTableCapacity;
    static int memTableCapacity;

    public Memory(int cacheCapacity, int ssTableCapacity) {
        this.cacheCapacity = cacheCapacity;
        this.ssTableCapacity = ssTableCapacity;
        this.memTableCapacity = ssTableCapacity * 2;
        cache = new Cache(cacheCapacity);
    }

    public static void printTables() {
        //创建TreeMap对象：
//        TreeMap<Integer, List> treeMap = new TreeMap<Integer, List>();
//        System.out.println("初始化后,TreeMap元素个数为：" + treeMap.size());
        for(Object name: tableMap.keySet()){
            System.out.println("table: " + name );
            System.out.println("memtable: " + tableMap.get(name));
        }
    }

    public static void write(String tableName, String data){
        System.out.println(tableName);
        String[] dataDetail = data.substring(1,data.length()-1).split(",");
        String key = dataDetail[0].trim();
        List value = Arrays.asList(dataDetail[1].trim(), dataDetail[2].trim());
        System.out.println(key + " : " + value);
        TreeMap memtable;
        if(tableMap.containsKey(tableName)){
            memtable = (TreeMap) tableMap.get(tableName);
        }else {
            memtable = new TreeMap<Integer, List>();
        }
        cache.set(tableName, key, value);
        memtable.put(key, value);
        checkFlush(tableName, memtable);
        System.out.println("Written: " + tableName + ", " + key + ", " + dataDetail[1].trim() + ", "+ dataDetail[2].trim());

    }

    public static void erase(String tableName, String data){
        System.out.println(tableName);
        String key = data.trim();
        List value = Arrays.asList("delete");
        System.out.println(key + " : " + value);
        if(tableMap.containsKey(tableName)){
            TreeMap memtable = (TreeMap) tableMap.get(tableName);
            memtable.put(key, value);
            checkFlush(tableName, memtable);
        }
        System.out.println("Erased: " + tableName + " " + data);
    }

    public static void delete(String tableName) {
        System.out.println(tableName);
        if(tableMap.containsKey(tableName)){
            TreeMap memtable = (TreeMap) tableMap.get(tableName);
            memtable.clear();
            memtable.put("delete", null);
            tableMap.put(tableName, memtable);
        }
        System.out.println("Deleted: " + tableName);
    }

    //reads need to bring SSTable blocks from disk to database buffer, and the blocks will be kept in a read cache.
    //只有读的时候要存buffer
    public static void readID(String tableName, String key) {
        System.out.println(tableName + ":" + key);
        List cached = cache.get(tableName, key);
        TreeMap memtable = (TreeMap) tableMap.get(tableName);
        if(cached != null) {
//        read from buffered cache
            System.out.println("read from cache");
            System.out.println(cached.toString());
            String output = cached.toString();
            System.out.println("Read: " + tableName + ", " + key + ", " + output.substring(1, output.length()-1));
        } else if((memtable != null) && (memtable.containsKey(key))){
//        read from memtable
            System.out.println("read from memtable");
            System.out.println(memtable.toString());
            String output = memtable.get(key).toString();
            System.out.println("Read: " + tableName + ", " + key + ", " + output.substring(1, output.length()-1));
        }else{
            System.out.println("read from disk");

            List list = getIDSSTable(tableName, key);
            if(list == null) {
                System.out.println("data not found in disk");
            } else {
                cache.add(tableName, (String)list.get(0), (List)list.get(1));
                String output = list.get(1).toString();
                System.out.println("Read: " + tableName + ", " + key + ", " + output.substring(1, output.length()-1));
            }
        }
    }

    public static List getIDSSTable(String tableName, String key) {
//    public static TreeMap getIDSSTable(String tableName, String key) {
        List list = new ArrayList();
//        TreeMap sstable = new TreeMap();
        if(true) {
            int levels = 0;  //  TODO: # of levels
            int level = 0;
            //find the position of the sstable needed
            String fileName = null;
            if(tableNum.size() == 0) {
                level = 1;
            }
            for(; level <= levels; level++) {
                for(int num = (int)tableNum.get(tableName); num >= 1; num--) { //TODO: # of sstable in each level
//                for(int num = 1; num <= (int)tableNum.get(tableName); num++) {
                    fileName = String.valueOf(level) + tableName + String.valueOf(num);
                    list = searchSSTable(fileName, key, "ID", null);
                    if(list != null) {
                        return list;
                    }
//                    sstable = searchSSTable(fileName, key, "ID", null);
//                    if(sstable != null) {
//                        return sstable;
//                    }
                }
            }
        }
        //data not exist
        return null;
    }

    //if search "ID": return if the treeMap contains a key = key
    //if search "AreaCode": loop through all the data in this treemap(file) and check if there is any data match the area code
    public static List searchSSTable(String fileName, String key, String search, String area) {
        TreeMap treeMap = new TreeMap();
        List lists = new ArrayList<>();
        try {
//            BufferedReader in = new BufferedReader(new FileReader(sstableName + ".txt"));
//            String sstable = in.readLine();
            FileInputStream fis = new FileInputStream(fileName + ".txt");
            ObjectInputStream ois = new ObjectInputStream(fis);
            treeMap = (TreeMap) ois.readObject();
            ois.close();
            fis.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
        if(search.equals("ID")) {
            if (treeMap.containsKey(key)) {
                lists.add(key);
                lists.add(treeMap.get(key));
                return lists;
            }
        } else if(search.equals("AreaCode")) {
            for(Object areaKey : treeMap.keySet()) {
                List value = (List)treeMap.get((String)areaKey);
                if(value.size() <= 1) {
                    continue;
                }
                if(((String)value.get(1)).substring(0, 3).equals(area)) {
                    List list = new ArrayList();
                    list.add(areaKey);
                    list.add(value);
                    lists.add(list);
                }
            }
            return lists;
        }
        return null;
    }

    public static void readAreaCode(String tableName, String area) {
        System.out.println(tableName + ":" + area);
        TreeMap memtable = (TreeMap) tableMap.get(tableName);
        Set outputKey = new HashSet();
        if(memtable != null){
//        read from memtable
            System.out.println("read from memtable");
            System.out.println(memtable.toString());
            for(Object key : memtable.keySet()) {
                if(!outputKey.contains(key)){
                    List value = (List)memtable.get((String)key);
                    if(value.size() <= 1) {
                        continue;
                    }
                    if(((String)value.get(1)).substring(0, 3).equals(area)) {
                        String output = value.toString();
                        outputKey.add(key);
                        System.out.println("Read: " + tableName + ", " + area + ", " + output.substring(1, output.length()-1));
                    }
                }

            }

        }
        System.out.println("read from disk");

        Map<String, List> dataFromDisk = getAreaCodeSSTable(tableName, area, outputKey); //从disk里取出需要的key-value pairs

        if(dataFromDisk.size() == 0) {
            System.out.println("data not found in disk");
            return;
        }else{
            for(String key: dataFromDisk.keySet()){
                cache.add(tableName, key, dataFromDisk.get(key));
                System.out.println("Read: " + tableName + ", " + area + ", " + key + ", " + dataFromDisk.get(key).toString());
            }
        }
//        for(TreeMap sstable : lists) {
////            cache.add(tableName, sstable);         //从disk里取出来的sstable，要存入cache
//            for(Object key : sstable.keySet()) {
//                if(!outputKey.contains(key)){
//                    cache.add(tableName, (String) key, (List) sstable.get(key));
//                    List value = (List)sstable.get((String)key);
//                    if(value.size() <= 1) {
//                        continue;
//                    }
//                    if(((String)value.get(1)).substring(0, 3).equals(area)) {   //找到当前sstable里areacode符合要求的 输出
//                        outputKey.add(key);
//                        String output = value.toString();
//                        System.out.println("Read: " + tableName + ", " + area + ", " + output.substring(1, output.length()-1));
//                    }
//                }
//
//            }

//        }
    }

    public static Map<String, List> getAreaCodeSSTable(String tableName, String area, Set outputKey) {
//        List<TreeMap> lists = new ArrayList<>();
//        TreeMap sstable = new TreeMap();
        Map<String, List> dataFromDisk = new HashMap<>();
        int levels = 0;         //  TODO: # of levels
        int level = 0;
        String fileName = null;
        if(tableNum.size() == 0) { //no sstable in level0
            level = 1;
        }
        for(; level <= levels; level++) {
            for(int num = (int)tableNum.get(tableName); num >= 1; num--) { //  TODO: # of sstable in each level
                fileName = String.valueOf(level) + tableName + String.valueOf(num);
                List records = searchSSTable(fileName, null, "AreaCode", area);
                if(records == null || records.size() == 0) {
                    continue;
                }
                int recordLength = records.size();
                for(int index = 0;  index < recordLength; index++){
                    List record = (List) records.get(index);
                    if(!outputKey.contains(record.get(0))){
                        outputKey.add(record.get(0));
                        dataFromDisk.put((String) record.get(0),(List) record.get(1));
                    }
                }
            }
        }
        return dataFromDisk;
    }


    //    public static void writeToFile(String fileName, String data){
    public static void writeToFile(String fileName, TreeMap data){
        try {
            FileOutputStream fos = new FileOutputStream(fileName + ".txt");
            ObjectOutputStream oos = new ObjectOutputStream(fos);
            oos.writeObject(data);
            oos.close();
            fos.close();
//            BufferedWriter out = new BufferedWriter(new FileWriter(fileName + ".txt"));
//            out.write(data);
//            out.close();
            System.out.println(fileName + ": 文件写入成功！");
        } catch (IOException e) {
        }
    }

    public static void checkFlush(String tableName, TreeMap memtable) {

        System.out.println("check ! " + tableName + ": length = " + memtable.size());
        if (memtable.size() >= ssTableCapacity) {
            try {
                System.out.println("flush!");
                int numOfTable;
    //          give this new sstable a name and update tableNum and tableMap
                if (tableNum.containsKey(tableName)) {
                    numOfTable = (int) tableNum.get(tableName);
                    numOfTable = numOfTable + 1;
                } else {
                    numOfTable = 1;
                }


                String level = "0";
//                String sstableName = tableName + "" + numOfTable;
                String sstableName = level + tableName + "" + numOfTable;
                TreeMap immutable_memtable = memtable;
                int stringlength = immutable_memtable.toString().length();
//                writeToFile(sstableName, immutable_memtable.toString().substring(1, stringlength - 1));
                writeToFile(sstableName, immutable_memtable);
                System.out.println("Create L-0 K-");
                tableMap.remove(tableName);
                tableNum.put(tableName, numOfTable);

            } catch (Exception e) {
            }
        }else{
            tableMap.put(tableName, memtable);
        }
    }
}

