import java.io.*;
import java.nio.file.Files;
import java.util.*;
import java.util.stream.Collectors;

public class Main {

    static  BufferedWriter lwriter;

    static String file_extension = ".txt";
    static int MAX_ID_SIZE = 4 , MAX_CLIENTNAME_SIZE = 16 , MAX_PHONE_SIZE = 12;

    public static HashMap<String,Integer> currentSlotted_table = new HashMap<String,Integer>();

    static int length_of_file = 51;

    static String record_terminator = "#";

    static DatabaseBuffer bfr = new DatabaseBuffer();

    void deleteDir(File file) {
        File[] contents = file.listFiles();
        if (contents != null) {
            for (File f : contents) {
                if (! Files.isSymbolicLink(f.toPath())) {
                    deleteDir(f);
                }
            }
        }
        file.delete();
    }


    public static void update_hashmap_to_file()
    {

        try
        {
            Iterator hp_iter = currentSlotted_table.entrySet().iterator();
            PrintWriter meta_writer = new PrintWriter(new File("Database//metadata.txt"));
            meta_writer.write("");
            meta_writer.close();

            BufferedWriter write_to_metadata = new BufferedWriter(new FileWriter("Database//metadata.txt"));
            while(hp_iter.hasNext())
            {
                Map.Entry mapElement = (Map.Entry)hp_iter.next();
                write_to_metadata.write(mapElement.getKey()+" "+mapElement.getValue());
                write_to_metadata.newLine();
            }
            write_to_metadata.close();
        }
        catch(Exception ex)
        {
            ex.printStackTrace();
        }


    }

    public static boolean isInteger(String number)
    {
        try
        {
            Integer.parseInt(number);
            return true;
        }
        catch(Exception ex)
        {
            return false;
        }

    }

    public void load_current_slotted_pages()
    {
            try
            {
                File metadata_file = new File("Database//metadata.txt");
                BufferedReader file_reader = new BufferedReader(new FileReader(metadata_file));

                String line = "";
                while( (line = file_reader.readLine()) != null)
                {
                    String[] current_slotted_pages = line.split(" ");
                    currentSlotted_table.put(current_slotted_pages[0],Integer.parseInt(current_slotted_pages[1]));
                }
            }
            catch(Exception ex)
            {
                ex.printStackTrace();
            }


    }

    public static boolean write_to_table(String table_name,String ID,String clientName,String phoneNumber,BufferedWriter lwriter)
    {
        try
        {
            lwriter.write("W "+table_name+"("+ID+","+clientName+","+phoneNumber+")");
            lwriter.write("\n");
            //Check if the bytes exceed the size
            //System.out.println(table_name+" "+ID+" "+clientName+" "+phoneNumber);

            //Checking if the tuple is valid or not
            if(ID.getBytes().length > MAX_ID_SIZE || clientName.getBytes().length > MAX_CLIENTNAME_SIZE || phoneNumber.getBytes().length > MAX_PHONE_SIZE)
            {
                //System.out.println("Exceeds size! Invalid tuple");
                return false;
            }

            TableStructure new_tuple = new TableStructure(ID,clientName,phoneNumber);

            File write_to_file = new File("Database//"+table_name);

            //Directory doesn't exists so create one, and eventually create the database(File named table_name.txt)
            if(!write_to_file.isDirectory()) {
                if (write_to_file.mkdir()) {
                    //System.out.println("Directory successfully created!");
                    File create_new_file = new File("Database//" + table_name + "//" + table_name + ".txt");

                    if (create_new_file.createNewFile()) {
                        //System.out.println("File created successfully!");
                    } else {
                        //System.out.println("File already exists!");
                    }
                } else {
                    //System.out.println("Directory already exists!");
                }
            }

            //check for best possible hole in the metadata

            //Open file in append mode and write to file.
            BufferedWriter update_database = new BufferedWriter(new FileWriter("Database//" + table_name + "//" + table_name + ".txt",true));
            String final_write_to_file = ID+" "+clientName+" "+phoneNumber;
            //System.out.println(final_write_to_file.getBytes().length);
            update_database.write(ID+" "+clientName+" "+phoneNumber);
            update_database.newLine();
            update_database.close();

            //Add it to a slotted page and put that slotted page in the buffer

            //Get the count of current tables slotted id number(Cases assuming no deletion)
            int current_page = 1;
            String write_contents_slotted_page = final_write_to_file.getBytes().length+"/1/"+final_write_to_file+record_terminator;

            /***********************************************************************
             You need to check if the current contents of the file can be written to this slotted
             page if yes then write it else create a new slotted page
             ************************************************************************/
            if(currentSlotted_table.containsKey(table_name))
            {
                current_page = currentSlotted_table.get(table_name);
            }

            //New entry to the hashmap
            if(current_page == 1)
            {
                currentSlotted_table.put(table_name,1);
            }

            String tuple = ID+" "+clientName+" "+phoneNumber;
            RecordStructure record_to_add = new RecordStructure(tuple.length(),1,tuple);

            //Check if this page is in the buffer
            //Create the slotted page file if it doesn't exist

            File f_temp = new File("Database//"+table_name+"//slotted_"+table_name+current_page+file_extension);

            if(!f_temp.exists())
            {
                if(f_temp.createNewFile())
                {
                    System.out.println("Slotted file added successfully");
                }
            }

            if(bfr.isPageInBuffer(current_page,table_name))
            {
                /*
                SlottedPage getPage = new SlottedPage();
                  getPage =  bfr.getPage(table_name,current_page);

                if(getPage == null)
                {
                    System.out.println("Something went wrong in INSERT");
                }
                */

                //if(getPage.add_content_to_page(record_to_add))
                //System.out.println("BUFFER CONTENT TABLE NAME " + bfr.getPage(table_name,current_page).table_name);
                if(bfr.getPage(table_name,current_page).add_content_to_page(record_to_add))
                {
                    //Add it to current slotted page
                    //System.out.println("Succesfully written to buffer");
                    lwriter.write("Written: "+table_name+","+ID+","+clientName+","+phoneNumber);
                    lwriter.write("\n");
                    return true;
                }
                else
                {
                    //Overflow condition
                    //There are 2 cases look for holes through the hashmap and try to add it in them
                    //else create a new slotted page bring it to the buffer and add it.

                    //Case 2:
                    int current_page_number = currentSlotted_table.get(table_name);
                    currentSlotted_table.put(table_name,currentSlotted_table.get(table_name) + 1);
                    current_page = currentSlotted_table.get(table_name);

                    //create an empty file to the disk
                    File f = new File("Database//"+table_name+"//slotted_"+table_name+current_page+file_extension);

                    if(f.createNewFile())
                    {
                        System.out.println("Slotted page successfully created in disk");
                    }
                    else
                    {
                        System.out.println("Error?Slotted page already exists");
                    }

                    SlottedPage new_page = new SlottedPage(table_name,current_page);
                    bfr.add_slotted_page(new_page);
                    lwriter.write("SWAP IN T-"+table_name+" P-"+current_page);
                    lwriter.write("\n");
                    if(new_page.add_content_to_page(record_to_add))
                {
                    lwriter.write("Written: "+table_name+","+ID+","+clientName+","+phoneNumber);
                    lwriter.write("\n");
                    return true;
                }

                }

            }
            else
            {
                //The last page of that hashmap is not in the buffer
                if((new File("Database//"+table_name+"//slotted_"+table_name+current_page+file_extension).length() +  write_contents_slotted_page.getBytes().length) > length_of_file)
                {
                    //Overflow condition
                    //There are 2 cases look for holes through the hashmap and try to add it in them
                    //else create a new slotted page bring it to the buffer and add it.

                    //Case 2 :
                    currentSlotted_table.put(table_name,currentSlotted_table.get(table_name) + 1);
                    current_page = currentSlotted_table.get(table_name);
                    File f = new File("Database//"+table_name+"//slotted_"+table_name+current_page+file_extension);

                    if(f.createNewFile())
                    {
                        System.out.println("Slotted page successfully created in disk");
                    }
                    else
                    {
                        //System.out.println("Error?Slotted page already exists");
                    }

                    SlottedPage new_page = new SlottedPage(table_name,current_page);
                    bfr.add_slotted_page(new_page);
                    lwriter.write("SWAP IN T-"+table_name+" P-"+current_page);
                    lwriter.write("\n");
                    if(!new_page.add_content_to_page(record_to_add))
                    {
                        System.out.println("Something went wrong in checking the size!");
                    }
                    else
                    {
                        lwriter.write("Written: "+table_name+","+ID+","+clientName+","+phoneNumber);
                        lwriter.write("\n");
                        return true;
                    }

                }
                else
                {
                    //Not an overflow take that page and put it to the buffer and then write the content


                    SlottedPage new_page = new SlottedPage(table_name,current_page);
                    bfr.add_slotted_page(new_page);
                    lwriter.write("SWAP IN T-"+table_name+" P-"+current_page);
                    lwriter.write("\n");
                    if(!new_page.add_content_to_page(record_to_add))
                    {
                        System.out.println("Something went wrong in checking the size!");
                    }
                    else
                    {
                        lwriter.write("Written: "+table_name+","+ID+","+clientName+","+phoneNumber);
                        lwriter.write("\n");
                        return true;
                    }
                }


            }

            /*
           // System.out.println(table_name+" "+new File("Database//"+table_name+"//slotted_"+table_name+current_page+file_extension).length()+" "+write_contents_slotted_page.getBytes().length);
            if(new File("Database//"+table_name+"//slotted_"+table_name+current_page+file_extension).length() + write_contents_slotted_page.getBytes().length > length_of_file)
            {
                //System.out.println("Current slotted page has exceeded size");
                currentSlotted_table.put(table_name,currentSlotted_table.get(table_name) + 1);
                current_page = currentSlotted_table.get(table_name);
            }

            //System.out.println(table_name+" "+current_page);

            BufferedWriter write_to_slotted_page = new BufferedWriter(new FileWriter("Database//"+table_name+"//slotted_"+table_name+current_page+file_extension,true));


            if(is_set_length == true)
            {
                write_to_slotted_page.setLength(length_of_file);
            }


            //write_to_slotted_page.write(write_contents_slotted_page);
            //write_to_slotted_page.close();


            //Put the current page to the Database buffer create a slotted page
            //SlottedPage table_page = new SlottedPage(table_name,current_page);


            ArrayList<String> contents_slotted_page = table_page.contents;
            for(String content:contents_slotted_page)
            {
                System.out.println(content);
            }

            if(bfr.getCount() < bfr.SIZE)
            {
                bfr.add_slotted_page(table_page);
                //bfr.increment_count();
            }
            else
            {
                System.out.println("A slotted page was removed");
                bfr.evict_slotted_page();
                bfr.add_slotted_page(table_page);
            }

            //System.out.println(bfr.getCount()+" "+bfr.SIZE);
            //bfr.print_contents();

            return true;

             */

        }
        catch(Exception ex)
        {
            ex.printStackTrace();
            return false;
        }
        return false;
    }


    public void readScriptFile(BufferedWriter lwriter)
    {
        try
        {
            File script_file = new File("script.txt");
            BufferedReader br_script = new BufferedReader(new FileReader(script_file));
            String line = "";

            while( (line = br_script.readLine()) != null)
            {
                line.trim();

                //Perform operation based on first character of the string
                String database_operation = line;

                if(database_operation.charAt(0) == 'R')
                {

                    //Retrieve contents of the table if it matches with the ID
                    //Check if it present in the buffer else check each slotted page
                    //Query of the form R table val

                    String[] query = database_operation.split(" ");
                    //query[0] holds the database_operation
                    //query[1] holds the table name
                    //query[2] holds the ID

                    String table_name = query[1];
                    Integer ID = Integer.valueOf(query[2]);

                    StringBuilder query_result = new StringBuilder();
                    lwriter.write("R "+table_name+" "+ID);
                    lwriter.write("\n");

                    /*
                    //Check metadata to check if table is valid
                    File f = new File("Database//metadata.txt");
                    BufferedReader read_meta = new BufferedReader(new FileReader(f));

                    line = "";
                    boolean table_available = false;

                    while( (line = read_meta.readLine()) != null)
                    {
                            if(line.split(" ")[0] == table_name)
                            {
                                table_available = true;
                            }
                    }

                     */

                    if(!currentSlotted_table.containsKey(table_name))
                    {
                        lwriter.write("Read is aborted");
                        lwriter.write("\n");
                        System.out.println("Read is aborted");
                    }
                    else if(bfr.checkInBuffer(query_result,table_name,ID,""))
                    {
                        System.out.println("Query result for ID search " + ID.intValue());
                        System.out.println(query_result);
                        lwriter.write(query_result.toString());
                        lwriter.write("\n");

                        //bfr.checkInSlottedPages(query_result,table_name,ID,"");
                    }
                    else
                    {
                       // System.out.println("Record not in buffer");

                        //Let this method return the table name and slotted page number
                        //so that we can add this to the buffer
                        String table_and_pageno = bfr.checkInSlottedPages(query_result,table_name,ID,"");
                        if(table_and_pageno.isEmpty())
                        {
                            System.out.println("Sorry the tuple is not found");
                        }
                        else
                        {
                            //Add table and page no to the buffer
                            //create a slotted page and add it to the buffer
                            String[] tab_and_pageno = table_and_pageno.split(" ");
                            SlottedPage add_to_buffer = new SlottedPage(tab_and_pageno[0],Integer.valueOf(tab_and_pageno[1]));
                            bfr.add_slotted_page(add_to_buffer);

                            lwriter.write("SWAP IN T-"+tab_and_pageno[0]+" P-"+Integer.valueOf(tab_and_pageno[1]));
                        }
                        lwriter.write("READ:"+query_result.toString());
                        lwriter.write("\n");

                    }

                }
                else if(database_operation.charAt(0) == 'M')
                {
                    //Retrieve contents of the table if it matches with the Area extension
                    //Check if it present in the buffer else check each slotted page

                    String[] query = database_operation.split(" ");

                    String table_name = query[1];
                    String area = query[2];

                    StringBuilder query_result = new StringBuilder();

                    lwriter.write("M"+table_name+" "+area);
                    lwriter.write("\n");

                    if(!currentSlotted_table.containsKey(table_name))
                    {
                        System.out.println("Read is aborted");
                    }
                    bfr.checkInBuffer(query_result,table_name,null,area);

                   // System.out.println("Qeury results after buffer" + query_result);

                    bfr.checkInSlottedPages(query_result,table_name,null,area);

                    //System.out.println("Qeury results after slotted" + query_result);

                    String[] each_records = query_result.toString().split("#");

                    ArrayList<String>query_result_array_list = new ArrayList<>();


                    for(String record:each_records)
                    {
                        //System.out.println("RECORD TO ADD TO AREA " + record);
                        query_result_array_list.add(record);
                    }

                    //System.out.println("With duplicates ");
                    /*
                    for(String temp : query_result_array_list)
                    {
                        System.out.println(temp);
                    }
                    System.out.println("With duplicates ends");

                     */

                    List<String> dup_remove = new ArrayList<>(query_result_array_list);
                    List<String> newList = dup_remove.stream().distinct().collect(Collectors.toList());

                    System.out.println("Record matching area " + area + " in the table " + table_name);
                    Iterator<String> it = newList.iterator();
                    while(it.hasNext())
                    {
                        String to_write = it.next();
                        lwriter.write("MREAD:"+to_write);
                        lwriter.write("\n");
                        System.out.println(to_write);
                    }

                        //System.out.println("Record retrieved in buffer");
                        //System.out.println(query_result);
                        //


                    //CheckInSlotted pages is currently working.
                    //bfr.checkInSlottedPages(query_result,table_name,null,area);


                }
                else if(database_operation.charAt(0) == 'W')
                {
                    //database_operation[1] holds the table name
                    //database_operation[2] holds the tuple() seperated by 2 comma
                    String[] tuple = database_operation.substring(5,database_operation.length()-2).split(",");
                    String table_name = String.valueOf(database_operation.charAt(2));

                    //tuple[0] stores ID
                    //tuple[1] stores clientName
                    //tuple[2] stores phone


                    //Check if tuple[0] is a valid integer
                    if(!isInteger(tuple[0]))
                    {
                        //System.out.println(tuple[0]);
                        //System.out.println("Sorry! Not a valid integer");
                        continue;
                    }

                    if( write_to_table(table_name,tuple[0].trim(),tuple[1].trim(),tuple[2].trim(),lwriter) )
                    {
                        System.out.println("Successfully written to database");
                    }
                    else
                    {
                        System.out.println("Error in writing to database");
                    }

                }
                else if(database_operation.charAt(0) == 'E')
                {
                    //Delete the content from the actual DB file
                    //and after that check the slotted pages to make free space

                    //Store the starting position of each record by reading it bit by bit
                    //start_position + (number of bytes needed for ID+2) will be the position of the boolean
                    String[] query = database_operation.split(" ");

                    String table_name = query[1];
                    Integer ID = Integer.valueOf(query[2]);

                    StringBuilder free_space_location = new StringBuilder();

                    lwriter.write("E "+table_name+" "+ID);
                    lwriter.write("\n");

                    if(bfr.isPageInBuffer(table_name,ID))
                    {
                        System.out.println("Record deleted in buffer successfully");
                    }
                    else
                    {
                        //Search for the file contents and bring it to the buffer and change is_free
                        if( bfr.deleteInSlottedPages(table_name,ID,free_space_location) )
                        {
                            String[] meta = free_space_location.toString().split(" ");
                            SlottedPage add_to_buffer = new SlottedPage(meta[0],Integer.valueOf(meta[1]));
                            bfr.add_slotted_page(add_to_buffer);
                            lwriter.write("SWAP IN T-"+meta[0]+" P-"+meta[1]);

                           // System.out.println("Record was bought to buffer");
                            if(bfr.isPageInBuffer(meta[0],Integer.valueOf(meta[1])))
                            {
                                System.out.println("Record deleted in buffer successfully");
                            }
                        }
                        else
                        {

                            System.out.println("Record is not there for deletion");
                        }
                        if(bfr.deleteInFile(table_name,ID))
                        {
                            System.out.println("Deleted from the database");
                        }

                    }
                    lwriter.write("Erased "+table_name+" "+ID);
                    lwriter.write("\n");

                    /*

                    if(bfr.deleteInFile(table_name,ID))
                    {
                        System.out.println("Successfully deleted in database");
                    }

                    if(bfr.deleteInSlottedPages(table_name,ID,free_space_location))
                    {
                        System.out.println("Successfully deleted from slotted pages");
                        //Check the free spave location and add it to the hashmap and write it to a meta data file.
                        File f = new File("Database//" + table_name + "//"+table_name+"_meta.txt");
                        if(f.createNewFile())
                        {
                            System.out.println("File created successfully");
                        }
                        else
                        {
                            System.out.print("File already exists");
                        }

                        //Open file in append mode for writing
                        BufferedWriter writer = new BufferedWriter(new FileWriter(f,true));
                        writer.append(free_space_location.toString());
                        writer.append("\n");
                        writer.close();
                    }

                     */

                }
                else if(database_operation.charAt(0) == 'D')
                {
                    //Delete the entire directory
                    String[] query = database_operation.split(" ");
                    File f = new File("Database//"+query[1]);
                    lwriter.write("Deleted " + query[1]);
                    lwriter.write("\n");



                    //bfr.delete_from_buffer(query[1]);
                    deleteDir(f);

                }
                else
                {
                    System.out.println("Invalid Option!");
                }

            }

            bfr.flush_all_buffer_contents();

            update_hashmap_to_file();
        }
        catch(Exception ex)
        {
            ex.printStackTrace();
        }
    }

    public static void main(String[] args)
    {
        try
        {

            File f = new File("Database//log.txt");

            if(!f.exists())
            {
                if(f.createNewFile())
                {
                    System.out.println("Log created successfully");
                }
            }

            lwriter = new BufferedWriter(new FileWriter("Database//log.txt",true));


            Main test = new Main();

            //This will store which slotted page is current for a particular table
            test.load_current_slotted_pages();
            test.readScriptFile(lwriter);


            //Database content
            bfr.print_contents();
            lwriter.close();
        }
        catch(Exception ex)
        {
            ex.printStackTrace();
        }
    }
}
