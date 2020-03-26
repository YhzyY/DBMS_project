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
        FileInputStream inputStream = new FileInputStream("script");
        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(inputStream));

        String action;
        String table;
        String data = "delete";
        String input;
        int numrecord = 0;
        Memory memory = new Memory(cacheCapacity, SSTableCapacity);
        while ((input = bufferedReader.readLine()) != null) {
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
            }else if(action.equals("E")){
//                System.out.println("-------erase--------");
                memory.erase(table, data);
            }else if(action.equals("D")){
//                System.out.println("-------delete--------");
                memory.delete(table);
            }else if(action.equals("R")){
//                System.out.println("-------read ID--------");
                memory.readID(table, data);
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
        System.out.println("Time: " + (endTime-startTime));
        System.out.println("Throughput: " + numrecord*1000.0/(endTime-startTime));
    }
}
