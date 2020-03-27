import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;


public class DatabaseBuffer {

    //We shall assume the DatabaseBuffer is capable of holding atmost 10 pages

    static int count = 0;
    int SIZE = 10;
    ArrayList<SlottedPage> slotted_pages = new ArrayList<SlottedPage>();

    //int length_of_file = 51;

    String file_extension = ".txt";


    public boolean isPageInBuffer(String table_name,Integer ID)
    {
        for(SlottedPage temp:slotted_pages)
        {
            if( (temp.table_name.equals(table_name)))
            {
                //Iterate through all the slotted pages and try to find a match
                for(RecordStructure record : temp.contents)
                {
                    if(ID.intValue() == Integer.valueOf(record.tuple.split(" ")[0]).intValue())
                    {
                        record.is_free = 0;
                        return true;
                    }
                }

            }

        }
        return false;
    }

    public boolean isPageInBuffer(int current_page,String table_name)
    {
        for(SlottedPage temp:slotted_pages)
        {
            if( (temp.table_name.equals(table_name)) && (temp.page_id == current_page) )
            {
                return true;
            }

        }
        return false;
    }

    public SlottedPage getPage(String table_name,Integer current_page)
    {
        for(SlottedPage temp:slotted_pages)
        {
            if( (temp.table_name.equals(table_name)) && (temp.page_id == current_page) )
            {
                return temp;
            }

        }
        return null;
    }

    public boolean make_it_recent(int current_page,String table_name)
    {
        //Assumption is that isPageInBuffer return true
        SlottedPage store_temporary = null;
        int index = 0;
        for(SlottedPage temp:slotted_pages)
        {
            if( (temp.table_name.equals(table_name)) && (temp.page_id == current_page) )
            {
               store_temporary = temp;
               break;
            }
            index++;

        }

        if(store_temporary == null)
        {
            System.out.println("Something really UNEXPECTED happened");
            return false;
        }

        slotted_pages.remove(index);
        slotted_pages.add(store_temporary);
        return true;

    }

    public boolean deleteInFile(String table_name,Integer ID)
    {
        try
        {
            boolean record_found = false;


            File inputFile = new File("Database//" + table_name + "//" + table_name + ".txt");
            File tempFile = new File("Database//" + table_name + "//" + table_name + "_temp.txt");

            if(tempFile.createNewFile())
            {
                System.out.println("Temp file created successfully");
            }

            BufferedReader reader = new BufferedReader(new FileReader(inputFile));
            BufferedWriter writer = new BufferedWriter(new FileWriter(tempFile));

            String line = "";

            while( (line = reader.readLine()) != null)
            {
                String[] tuple = line.trim().split(" ");
                if(Integer.valueOf(tuple[0]) == ID)
                {
                    record_found = true;
                }
                else
                {
                    writer.write(line);
                    writer.write("\n");
                }
            }

            writer.close();
            reader.close();

            if(record_found)
            {
                if(tempFile.renameTo(inputFile))
                {
                    System.out.println("Record deleted successfully");
                }
                return true;
            }
            else
            {
                tempFile.renameTo(inputFile);
                return false;
            }

        }
        catch(Exception ex)
        {
            ex.printStackTrace();
        }

        return false;
    }


    public boolean deleteInSlottedPages(String table_name,Integer ID,StringBuilder free_space_location)
    {
        boolean record_found = false;
        int current_page = 1 ,max_page_size = Main.currentSlotted_table.get(table_name);
        //int current_page = 1, max_page_size = 2;

        while(current_page <= max_page_size)
        {
            try
            {

                //Read it byte by byte and split based on sentinals (/ and #)
                InputStream in = new FileInputStream("Database//"+table_name+"//slotted_"+table_name+current_page+file_extension);
                byte[] read_byte = new byte[1];

                Integer size_allocated = null;
                boolean is_record_occupied = false;
                String tuple;

                ArrayList<String>collect_bytes = new ArrayList<String>();

                int seek_position = 0 , new_record_starting_position=0 , ID_byte_size = 0;

                int read_a_byte , slash_count = 0;
                while( (read_a_byte = in.read(read_byte)) != -1)
                {
                    String read_byte_string = new String(read_byte, StandardCharsets.UTF_8);
                    System.out.println("Read byte " + read_byte_string);
                    seek_position++;

                    if( (slash_count == 0) && (!read_byte_string.equals("/")))
                        ID_byte_size++;

                    if(read_byte_string.equals("#"))
                    {
                        //new record
                        slash_count = 0;
                        ID_byte_size = 0;
                        new_record_starting_position = seek_position;
                    }
                    else if(read_byte_string.equals("/"))
                    {
                        if(slash_count == 0)
                        {
                            //Read ID
                            size_allocated = Integer.valueOf(String.join("",collect_bytes));
                            collect_bytes.clear();
                            slash_count++;
                        }
                        else if(slash_count == 1)
                        {
                            //Read isFree
                            //System.out.println("Collected byted after seeing second slash " + String.join("",collect_bytes));
                            is_record_occupied = String.join("",collect_bytes).equals("1")?true:false;
                            collect_bytes.clear();

                            //Read the tuple straight away
                            //System.out.println("Number of byted to read " + size_allocated);
                            //System.out.println("Is the record occupied " + is_record_occupied);
                            byte[] read_tuple = new byte[size_allocated];
                            String actual_tuple = "";
                            //if(is_record_occupied)
                            //{
                            //The bytes still exists in file even if
                                if( (read_a_byte = in.read(read_tuple)) != -1)
                                {
                                    System.out.println("Tuple read successfully");
                                    seek_position += size_allocated;
                                    actual_tuple = new String(read_tuple,StandardCharsets.UTF_8);
                                    System.out.println(size_allocated+" "+is_record_occupied+" "+actual_tuple);
                                }
                            //}

                            if(!is_record_occupied)
                            {
                                System.out.println("This tuple is deleted " + actual_tuple);
                            }

                            System.out.println("Actual tuple is " + actual_tuple);
                            System.out.println("ID is " + ID + " tuple ID is " + Integer.valueOf(actual_tuple.split(" ")[0]));
                            System.out.println("ID byte size is " + ID_byte_size);
                            if( (is_record_occupied) && (ID != null) && (Integer.valueOf(actual_tuple.split(" ")[0]) == ID))
                            {

                                free_space_location.append(table_name+" "+current_page);
                                return true;
                                /*
                                //The new_record_starting postion is the starting point for our
                                //record that needs to be deleted
                                System.out.println("Starting position of the record to be deleted " + new_record_starting_position);

                                //Make record_occupied as 0;
                                record_found = true;
                                in.close();
                                RandomAccessFile rf = new RandomAccessFile("Database//"+table_name+"//slotted_"+table_name+current_page+file_extension,"rw");
                                try {
                                    //+1 for / separation between metadatas and the tuple
                                    rf.seek(new_record_starting_position + ID_byte_size + 1);

                                    //We are overwriting '0' to say that the record if free
                                    rf.writeByte(48);


                                    /********************************
                                     Do I need to add this slotted page to buffer contents
                                    //+3 is for /1 or 0/
                                    Integer deleted_record_starting_position = new_record_starting_position+ID_byte_size+3;
                                    free_space_location.append(size_allocated+" "+deleted_record_starting_position);
                                    SlottedPage add_modified_page = new SlottedPage(table_name,current_page);
                                    add_slotted_page(add_modified_page);

                                    //Contents have been modified in the disk and if present in the buffer
                                    //have been modified as well
                                }
                                finally
                                    {
                                    rf.close();
                                }
                            */

                                //The is is for /
                                //break;
                            }

                        }
                        else
                        {
                            System.out.println("Unhandled slash count");
                        }
                    }
                    else
                    {
                        //Collect the byte
                        collect_bytes.add(read_byte_string);
                    }
                }

            }
            catch(Exception ex)
            {
                ex.printStackTrace();
            }




            current_page++;
        }
        /*
        if(record_found)
        {
            return true;
        }
        else
        {
            return false;
        }
        */
         return false;
    }

    public String checkInSlottedPages(StringBuilder query_results,String table_name,Integer ID,String area) throws IOException {
        int current_page = 1 ,max_page_size = Main.currentSlotted_table.get(table_name);

        //int current_page = 1, max_page_size = 3;
        while(current_page <= max_page_size)
        {
            System.out.println("CHECKING IN SLOTTED PAGE " + table_name + " " + current_page);

            boolean is_cuurent_slotted_phone = false;

            try
            {
                /*
                File f = new File("Database//"+table_name+"//slotted_"+table_name+current_page+file_extension);
                BufferedReader slotted_reader = new BufferedReader(new FileReader(f));
                String line = "";
                String entire_contents_slotted_page = "";

                while( (line = slotted_reader.readLine()) != null)
                {
                    entire_contents_slotted_page = entire_contents_slotted_page + line;
                }


                String[] tuples_with_meta = entire_contents_slotted_page.split()

                */

                //Read it byte by byte and split based on sentinals (/ and #)
                InputStream in = new FileInputStream("Database//"+table_name+"//slotted_"+table_name+current_page+file_extension);
                byte[] read_byte = new byte[1];

                Integer size_allocated = null;
                boolean is_record_occupied = false;
                String tuple;

                ArrayList<String>collect_bytes = new ArrayList<String>();

                int read_a_byte , slash_count = 0;
                while( (read_a_byte = in.read(read_byte)) != -1)
                {
                    String read_byte_string = new String(read_byte, StandardCharsets.UTF_8);
                    System.out.println("Read byte " + read_byte_string);

                    if(read_byte_string.equals("#"))
                    {
                        //new record
                        slash_count = 0;
                    }
                    else if(read_byte_string.equals("/"))
                    {
                        if(slash_count == 0)
                        {
                            //Read ID
                            size_allocated = Integer.valueOf(String.join("",collect_bytes));
                            collect_bytes.clear();
                            slash_count++;
                        }
                        else if(slash_count == 1)
                        {
                            //Read isFree
                            //System.out.println("Collected byted after seeing second slash " + String.join("",collect_bytes));
                            is_record_occupied = String.join("",collect_bytes).equals("1")?true:false;
                            collect_bytes.clear();
                            slash_count++;

                            //Read the tuple straight away
                            //System.out.println("Number of byted to read " + size_allocated);
                            //System.out.println("Is the record occupied " + is_record_occupied);
                            byte[] read_tuple = new byte[size_allocated];
                            String actual_tuple = "";
                            if(is_record_occupied)
                            {
                                if( (read_a_byte = in.read(read_tuple)) != -1)
                                {
                                    System.out.println("Tuple read successfully");
                                    actual_tuple = new String(read_tuple,StandardCharsets.UTF_8);
                                    System.out.println(size_allocated+" "+is_record_occupied+" "+actual_tuple);
                                }
                            }

                            if(ID != null && Integer.valueOf(actual_tuple.split(" ")[0]) == ID)
                            {
                                query_results.append(actual_tuple);
                                in.close();
                                if(isPageInBuffer(current_page,table_name))
                                {
                                    make_it_recent(current_page,table_name);
                                }
                                else
                                {
                                    SlottedPage make_it_recent = new SlottedPage(table_name,current_page);
                                    add_slotted_page(make_it_recent);
                                    query_results.append(actual_tuple);
                                    query_results.append("#");
                                }

                                //Add this slotted page to the buffer
                                SlottedPage add_to_buffer = new SlottedPage(table_name,current_page);
                                add_slotted_page(add_to_buffer);
                                return (table_name+" "+current_page);
                            }
                            else if(!area.isEmpty() && actual_tuple.split(" ")[2].split("-")[0].equals(area))
                            {
                                //NEED TO REMOVE DUPLICATES
                                is_cuurent_slotted_phone = true;
                                //Add it to the buffer
                                //Check if this page is present in the buffer or not
                                if(isPageInBuffer(current_page,table_name))
                                {
                                    make_it_recent(current_page,table_name);
                                }
                                else
                                {
                                    SlottedPage make_it_recent = new SlottedPage(table_name,current_page);
                                    add_slotted_page(make_it_recent);
                                    System.out.println("ADD THE TUPLE " + actual_tuple);
                                    query_results.append(actual_tuple);
                                    query_results.append("#");
                                }


                            }

                        }
                        else
                        {
                            System.out.println("Unhandled slash count");
                        }
                    }
                    else
                    {
                        //Collect the byte
                        collect_bytes.add(read_byte_string);
                    }
                }

            }
            catch(Exception ex)
            {
                ex.printStackTrace();
            }

            if(is_cuurent_slotted_phone)
            {
                SlottedPage add_to_buffer = new SlottedPage(table_name,current_page);
                add_slotted_page(add_to_buffer);
            }

            current_page++;
        }

        current_page = 1;
        return "";
    }

    public boolean checkInBuffer(StringBuilder query_results,String table_name,Integer ID,String area)
    {

        boolean check_id = true , check_area = true;
        boolean check_id_found = true;

        if(ID == null)
        {
            check_id = false;
            System.out.println("Area code is " + area);
        }

        if(area.isEmpty())
        {
            check_area = false;
        }

        for(SlottedPage page:slotted_pages)
        {
            //Then that page will contain that ID which will be mostly one
            if(ID != null)
            System.out.println(check_id+" "+page.table_name+" "+table_name+" "+ID.intValue() + page.ID_hashmap.containsKey(ID.intValue()));
            if(check_id && page.table_name.equals(table_name) && page.ID_hashmap.containsKey(ID.intValue()))
            {
                //Only if it present in that page iterate through the page contents
                //ArrayList<RecordStructure>records = page.contents;
                System.out.println("Came here");
                for(RecordStructure temp : page.contents)
                {
                    String[] tuple = temp.tuple.trim().split(" ");
                    System.out.println("Chk " + Integer.valueOf(tuple[0])+" "+ID.intValue());

                        if(Integer.valueOf(tuple[0]) == ID.intValue())
                        {
                            //There will be only one record
                            //System.out.println(tuple_with_spaces.trim());
                            query_results.append(temp.tuple);
                            check_id_found = true;
                            break;
                        }

                    //Handle area code with sentinals
                }
            }

            System.out.println("AREA CHECk " + check_area + " " + page.Phone_hashmap.containsKey(area) + " " + area);
            System.out.println(page.table_name + " " + table_name);
            if(check_area && page.table_name.equals(table_name) && page.Phone_hashmap.containsKey(area))
            {
                //Do the same but we have to retrieve multiple records
                System.out.println("Matches found");
                //ArrayList<RecordStructure>records = page.contents;
                for(RecordStructure temp : page.contents)
                {
                    String[] tuple = temp.tuple.trim().split(" ");

                    System.out.println(tuple[2].split("-")[0]+" "+area);

                    if(tuple[2].split("-")[0].equals(area))
                    {
                        //There will be only one record
                        //System.out.println(tuple_with_spaces.trim());
                        query_results.append(temp.tuple);
                        query_results.append("#");
                    }

                    //Handle area code with sentinals
                }

            }

        }

        if(check_id_found)
            return true;
        else
            return false;
    }

    public void increment_count()
    {
        count++;
    }

    public void print_contents()
    {
        //System.out.println("Database buffer contents");
        System.out.println("Size of buffer contents " + slotted_pages.size());

        for(SlottedPage page:slotted_pages)
        {
            for(RecordStructure temp:page.contents)
            {
                //System.out.println("Inside the for loop");
                System.out.println(temp.tuple);
            }
            System.out.println();

            //System.out.println(page.page_id+" "+page.table_name);
        }
    }

    public void decrement_count()
    {
        count--;
    }

    public int getCount()
    {
        return count;
    }

    //Make sure that redudant slot pages are not added to the bfr manager
    //add_slotted_page if the page is not in the buffer.

    //Never call this if the page is in the buffer
    public void add_slotted_page(SlottedPage page) throws IOException {
        //Check if the slotted page id is currently present in the buffer or not.
        //If not present simply add else remove the previous copy from the database buffer
        //and add the new one

        //add_to_slotted_pages() shouldnt be used if the page is already in the buffer

        boolean is_contains = false;
        int index_to_delete = -1;
        int cur_size = slotted_pages.size();

        //Since there can be a max 10 pages we simply iterate
        for(int i=0;i<cur_size;i++)
        {
            SlottedPage to_check = slotted_pages.get(i);
            //System.out.println(page.page_id+" "+to_check.page_id+" "+page.table_name+" "+to_check.table_name);
            //System.out.println(page.table_name+" "+to_check.table_name);
            //System.out.println();
            if( (page.page_id == to_check.page_id) && (page.table_name.equals(to_check.table_name)))
            {
                //System.out.println("I came here");
                is_contains = true;
                index_to_delete = i;
                break;
            }
        }

        if(is_contains)
        {
            //Remove the old copy content and the new content , by removing and adding it to the
            //arraylist we push the slotted page to the end.
            //System.out.println("IS CONTAINS WAS TRUE BUT IR SHOULD NEVER BE");
            slotted_pages.remove(index_to_delete);
            slotted_pages.add(page);
        }
        else
        {
            if(count + 1 > SIZE)
            {
                evict_slotted_page();
            }

            slotted_pages.add(page);
            increment_count();

        }


    }

    public void delete_from_buffer(String table_name)
    {
        int index = 0;
        for(SlottedPage temp : slotted_pages)
        {
            if(temp.table_name.equals(table_name))
            {
                slotted_pages.remove(index);
            }
            else
            {
                index++;
            }
        }
    }

    public void flush_all_buffer_contents() throws IOException
    {
        for(SlottedPage temp : slotted_pages)
        {
            System.out.println("Slotted page size " + temp.total_size);
            flush(temp);
        }

    }

    public void flush(SlottedPage page) throws IOException
    {
        String table_name = page.table_name;

        Integer slotted_page_no = page.page_id;

        File f = new File("Database//"+table_name+"//slotted_"+table_name+slotted_page_no+file_extension);

        if(f.delete())
        {
            System.out.println("Slotted page deleted successfully");
        }

        if(f.createNewFile())
        {
            System.out.println("Slotted page was created again");
        }

        BufferedWriter writer = new BufferedWriter(new FileWriter(f));
        for(RecordStructure temp:page.contents)
        {
            String number_of_bytes = String.valueOf(temp.number_of_bytes);
            String is_free_marker = String.valueOf(temp.is_free);
            String tuple = String.valueOf(temp.tuple);
            writer.write(number_of_bytes+"/"+is_free_marker+"/"+tuple+"#");
        }
        //Make sure that this slotted page contents combined does not exceed block size
        writer.close();
    }


    public void evict_slotted_page() throws IOException {
        //Use LRU page replacement algorithm to evict page from the ArrayList of slotted_pages
        //Currently we will evict the first page and decrement the count

        //LRU replacement algorithm
        //The page that is the beginning will be  will be least recently used
        //If the page is used or if we try to add the slotted_page to the buffer
        //We remove that page from arraylist and add it to the end(which is handled
        // in the add_slotted_page_method)

        String table_name = slotted_pages.get(0).table_name;
        String page_number = String.valueOf(slotted_pages.get(0).page_id);
        flush(slotted_pages.get(0));
        Main.lwriter.write("SWAP OUT T-" +table_name+" P-" +page_number);
        Main.lwriter.write("\n");
        slotted_pages.remove(0);
        count--;
    }
}
