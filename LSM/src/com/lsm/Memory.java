package com.lsm;

import java.io.*;
import java.util.*;

public class Memory{
//    tableMap stores the table name and its memtable
    static Map tableMap = new HashMap();
//    tableNum stores the table name and the number of its sstable in level 0
    static Map tableNum = new HashMap();
    static Set deletedTable = new HashSet();

    static int cacheCapacity;
    static Cache cache;
    static int ssTableCapacity;
    static int memTableCapacity;

    static Disk disk;

    public Memory(int cacheCapacity, int ssTableCapacity) {
        this.cacheCapacity = cacheCapacity;
        this.ssTableCapacity = ssTableCapacity;
        this.memTableCapacity = ssTableCapacity * 2;
        cache = new Cache(cacheCapacity);
        disk = new Disk(ssTableCapacity);
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
        if(deletedTable.contains(tableName)){
            deletedTable.remove(tableName);
        }
//        System.out.println(tableName);
        String[] dataDetail = data.substring(1,data.length()-1).split(",");
        String key = dataDetail[0].trim();
        List value = Arrays.asList(dataDetail[1].trim(), dataDetail[2].trim());
//        System.out.println(key + " : " + value);
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
//        System.out.println(tableName);
        String key = data.trim();
        List value = Arrays.asList("erased");
//        System.out.println(key + " : " + value);
        TreeMap memtable;
        if(tableMap.containsKey(tableName)){
            memtable = (TreeMap) tableMap.get(tableName);
        }else{
            memtable = new TreeMap();
        }
        cache.set(tableName, key, value);
        memtable.put(key, value);
        checkFlush(tableName, memtable);
        System.out.println("Erased: " + tableName + " " + data);
    }

    public static void delete(String tableName) {
//        System.out.println(tableName);
        TreeMap memtable;
        if(tableMap.containsKey(tableName)){
            memtable = (TreeMap) tableMap.get(tableName);
            memtable.clear();
        }else{
            memtable = new TreeMap();
        }
        deletedTable.add(tableName);
        memtable.put("delete", null);
        tableMap.put(tableName, memtable);
//        System.out.println("delete: " + tableName +" " + memtable.toString());
        System.out.println("Deleted: " + tableName);
    }

    //reads need to bring SSTable blocks from disk to database buffer, and the blocks will be kept in a read cache.
    //只有读的时候要存buffer
    public static void readID(String tableName, String key) {
        if (deletedTable.contains(tableName)) {
            System.out.println("table deleted");
        } else {
            boolean getFlag = false;
    //       System.out.println(tableName + ":" + key);
            List cached = cache.get(tableName, key);
            TreeMap memtable = (TreeMap) tableMap.get(tableName);
            if (cached != null) {
    //        read from buffered cache
                if(!cached.get(0).toString().equals("erased")){
                    String output = cached.toString();
                    System.out.println("Read: " + tableName + ", " + key + ", " + output.substring(1, output.length() - 1));
                    getFlag = true;
                }
            } else if ((memtable != null) && (memtable.containsKey(key))) {
    //        read from memtable
                String output = memtable.get(key).toString();
                System.out.println("Read: " + tableName + ", " + key + ", " + output.substring(1, output.length() - 1));
                getFlag = true;
            } else {
    //            System.out.println("read from disk");

                List list = getIDSSTable(tableName, key);
                if (list == null) {
    //                System.out.println("data not found in disk");
                } else {
                    cache.add(tableName, (String) list.get(0), (List) list.get(1));
                    String output = list.get(1).toString();
                    System.out.println("Read: " + tableName + ", " + key + ", " + output.substring(1, output.length() - 1));
                    getFlag = true;
                }
            }
            if(!getFlag)
                System.out.println("Read: no such data");
        }
    }

    public static List getIDSSTable(String tableName, String key) {
//    public static TreeMap getIDSSTable(String tableName, String key) {
        List list = new ArrayList();
//        TreeMap sstable = new TreeMap();
        if(true) {
            int levels = disk.getLevels();
//            System.out.println("levels : " + levels);
            int level = 0;
            //find the position of the sstable needed
            String fileName = null;
//            if(tableNum.size() == 0) {
//                level = 1;
//            }
            for(; level <= levels; level++) {
//                System.out.println("tableName----" + disk.numTableforTable(level,tableName));
                for(int num = (int)disk.numTableforTable(level,tableName); num >= 1; num--) {
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

    
    //Was updated for scheduler
        public static ArrayList<Integer> returnAreaCodes(String tableName,String area)
    {
        ArrayList<Integer>returnList = new ArrayList<Integer>();

        if (deletedTable.contains(tableName)) {
            System.out.println("table deleted");
        } else {
            //        System.out.println(tableName + ":" + area);
            TreeMap memtable = (TreeMap) tableMap.get(tableName);
            Set outputKey = new HashSet();
            if (memtable != null) {
                //        read from memtable
                //            System.out.println("read from memtable");
                //            System.out.println(memtable.toString());
                for (Object key : memtable.keySet()) {
                    if (!outputKey.contains(key)) {
                        List value = (List) memtable.get((String) key);
                        if (value.size() <= 1) {
                            continue;
                        }
                        if (((String) value.get(1)).substring(0, 3).equals(area)) {
                            String output = value.toString();
                            outputKey.add(key);
                            //System.out.println("MRead: " + tableName + ", " + key + ", " + output.substring(1, output.length() - 1));

                            //We are assuming that the key is of Integer type
                            returnList.add(Integer.valueOf((Integer)key));
                        }
                    }

                }

            }
            //        System.out.println("read from disk");

            Map<String, List> dataFromDisk = getAreaCodeSSTable(tableName, area, outputKey); //从disk里取出需要的key-value pairs

            if (dataFromDisk.size() == 0) {
                //            System.out.println("data not found in disk");
                return returnList;
            } else {
                for (String key : dataFromDisk.keySet()) {
                    cache.add(tableName, key, dataFromDisk.get(key));
                    String output = dataFromDisk.get(key).toString();
                    System.out.println("MRead: " + tableName + ", " + key + ", " + output.substring(1, output.length() - 1));

                    //Assumption is that the key holds the ID of that tuple
                    returnList.add(Integer.valueOf(key));
                }
            }
        }
        return returnList;
    }

    
    public static void readAreaCode(String tableName, String area) {
        if (deletedTable.contains(tableName)) {
            System.out.println("table deleted");
        } else {
            boolean getFlag = false;
    //        System.out.println(tableName + ":" + area);
            TreeMap memtable = (TreeMap) tableMap.get(tableName);
            Set outputKey = new HashSet();
            if (memtable != null) {
    //        read from memtable
                for (Object key : memtable.keySet()) {
                    if (!outputKey.contains(key)) {
                        List value = (List) memtable.get((String) key);
                        if (value.size() <= 1) {
                            continue;
                        }
                        if (((String) value.get(1)).substring(0, 3).equals(area)) {
                            String output = value.toString();
                            outputKey.add(key);
                            System.out.println("MRead: " + tableName + ", " + key + ", " + output.substring(1, output.length() - 1));
                            getFlag = true;
                            return;
                        }
                    }

                }

            }
            Map<String, List> dataFromDisk = getAreaCodeSSTable(tableName, area, outputKey); //从disk里取出需要的key-value pairs

            if (!(dataFromDisk.size() == 0)){
                for (String key : dataFromDisk.keySet()) {
                    cache.add(tableName, key, dataFromDisk.get(key));
                    String output = dataFromDisk.get(key).toString();
                    System.out.println("MRead: " + tableName + ", " + key + ", " + output.substring(1, output.length() - 1));
                    getFlag = true;
                }
            }
            if(!getFlag)
                System.out.println("MRead: no such data");
        }
    }

    public static Map<String, List> getAreaCodeSSTable(String tableName, String area, Set outputKey) {
//        List<TreeMap> lists = new ArrayList<>();
//        TreeMap sstable = new TreeMap();
        Map<String, List> dataFromDisk = new HashMap<>();
        int levels = disk.getLevels();
        int level = 0;
        String fileName = null;
        if(tableNum.size() == 0) { //no sstable in level0
            level = 1;
        }
        for(; level <= levels; level++) {
            for(int num = (int)disk.numTableforTable(level,tableName); num >= 1; num--) {
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
//            System.out.println(fileName + ": 文件写入成功！");
        } catch (IOException e) {
        }
    }

    public static void checkFlush(String tableName, TreeMap memtable) {

//        System.out.println("check ! " + tableName + ": length = " + memtable.size());
        if (memtable.size() >= ssTableCapacity) {
//                System.out.println("flush!");
            if(disk.numTable(0) >= 4){
                disk.Compact(0);
                tableNum.clear();
//                System.out.println(tableNum.toString());
            }
            try {
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
                String firstkey = (String) immutable_memtable.firstKey();
                String lastkey = (String) immutable_memtable.lastKey();
                System.out.println("Create L-0 K-" + tableName + firstkey + "-" + tableName + lastkey);
                tableMap.remove(tableName);
                tableNum.put(tableName, numOfTable);

            } catch (Exception e) {
            }
        }else{
            tableMap.put(tableName, memtable);
        }
    }
}

