import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.HashMap;

class RecordStructure
{
    Integer number_of_bytes;
    Integer is_free;
    String tuple;
    int tuple_size = 0;

    public RecordStructure(Integer ID,Integer is_free,String tuple)
    {
        this.number_of_bytes = ID;
        this.is_free = is_free;
        this.tuple = tuple;
        this.tuple_size = (String.valueOf(ID).length()) + 1 + 1 + +1 + (tuple.length()) + 1;
    }
}

public class SlottedPage {
    Integer current_slotted_page = 1;

    int page_id;

    String table_name;

    int total_size = 0;


    ArrayList<RecordStructure>contents = new ArrayList<RecordStructure>();

    HashMap<Integer,Integer>ID_hashmap = new HashMap<Integer,Integer>();

    HashMap<String,Integer>Phone_hashmap = new HashMap<String,Integer>();

    File readContents;

    public boolean add_content_to_page(RecordStructure temp)
    {
        System.out.println("Total size of the page " + total_size);
        System.out.println("Tuple size " + temp.tuple_size);
        if(total_size + temp.tuple_size > Main.length_of_file)
        {
            return false;
        }
        else
        {
            contents.add(temp);
            ID_hashmap.put(Integer.valueOf(temp.tuple.split(" ")[0]),1);
            String key = temp.tuple.trim().split(" ")[2].split("-")[0];
            if(Phone_hashmap.containsKey(key))
            {
                Integer value = Phone_hashmap.get(key);
                Phone_hashmap.put(key,value + 1);
            }
            else
            {
                Phone_hashmap.put(key,1);
            }
            total_size += temp.tuple_size;
            return true;
        }

    }

    public SlottedPage(String table_name,int current_page)
    {
        try
        {
            //Read from the slotted page and load it in contents and update ID and phone hashmap

            //System.out.println(table_name+" "+current_page);

            this.page_id = current_page;

            this.table_name = table_name;

            readContents = new File("Database//"+table_name+"//"+"slotted_"+table_name+current_page+".txt");
            BufferedReader read_page = new BufferedReader(new FileReader(readContents));
            String line = "";


            while( (line = read_page.readLine()) != null)
            {
                //System.out.println("Line " + line);

                System.out.println("PRINTING LINE " + line);

                String[] tuples = line.split("#");
                for(String tuple: tuples)
                {
                    System.out.println("PRINTING TUPLE " + tuple);
                    //System.out.println("Current tuple " + tuple);
                    String[] record_meta = tuple.split("/");
                    //record[2] has the record contents which are space separated
                    RecordStructure add_to_slotted_page = new RecordStructure(Integer.valueOf(record_meta[0]),Integer.valueOf(record_meta[1]),record_meta[2]);

                    //Check if tuple is deleted or not
                    if(add_to_slotted_page.is_free.intValue() == 1)
                    {
                        System.out.println("CHECK " + add_to_slotted_page.tuple.split(" ")[0]);
                        contents.add(add_to_slotted_page);
                        ID_hashmap.put(Integer.valueOf(add_to_slotted_page.tuple.split(" ")[0]),1);
                        String key = add_to_slotted_page.tuple.trim().split(" ")[2].split("-")[0];
                        if(Phone_hashmap.containsKey(key))
                        {
                            Integer value = Phone_hashmap.get(key);
                            Phone_hashmap.put(key,value + 1);
                        }
                        else
                        {
                            Phone_hashmap.put(key,1);
                        }

                    }
                    total_size += add_to_slotted_page.tuple_size;
                    System.out.println("SIZE OF THE SLOTTED PAGE WAS INCREASED " + total_size);
                    /*
                    //System.out.println("Record Meta contents");
                    for(String temp:record_meta)
                    {
                        //System.out.println(temp);
                    }
                    //System.out.println("Record Meta ends");

                    //Record[0] has the ID
                    //Record[1] has the clientName
                    //Record[2] has the Phone
                    String[] record = record_meta[2].trim().split(" ");

                    //System.out.println("Record contents");
                    for(String temp: record)
                    {
                        //System.out.println(temp);
                    }
                    //System.out.println("Record content Ends");


                    //Add it to the hashmap only if it a valid tuple
                    if(record_meta[1].equals("1"))
                    {
                        contents.add(record_meta[2]);
                        ID_hashmap.put(Integer.parseInt(record[0]),1);
                        Phone_hashmap.put(record[2].split("-")[0],1);
                    }

                     */
                }
            }
        }
        catch(Exception ex)
        {
            ex.printStackTrace();
        }

    }

    public void increment_current_slotted_page()
    {
        this.current_slotted_page++;
    }

    public int return_current_slotted_page()
    {
        return current_slotted_page;
    }



}
