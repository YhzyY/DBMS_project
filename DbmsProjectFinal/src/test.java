public class test {

    public static void main(String[] args) {
        DatabaseBuffer bfr = new DatabaseBuffer();
/*
        StringBuilder query_results = new StringBuilder();

        String table_and_pageno = bfr.checkInSlottedPages(query_results,"X",null,"412");

        //System.out.println(table_and_pageno);

        System.out.println("Query results are " + query_results);

     try {
        //bfr.deleteInFile("X",7);
        StringBuilder free_space_location = new StringBuilder();
        bfr.deleteInSlottedPages("Y", 6, free_space_location);
        System.out.println(free_space_location);

        System.out.println("Successfully deleted from slotted pages");
        //Check the free spave location and add it to the hashmap and write it to a meta data file.
        File f = new File("Database//" + "Y" + "//" + "Y" + "_meta.txt");
        if (f.createNewFile()) {
            System.out.println("File created successfully");
        } else {
            System.out.print("File already exists");
        }

        //Open file in append mode for writing
        BufferedWriter writer = new BufferedWriter(new FileWriter(f, true));
        writer.append(free_space_location.toString());
        writer.close();
    }
    catch(Exception ex)
    {
        ex.printStackTrace();
    }
    }

 */
   SlottedPage test = bfr.getPage("X",1);

        System.out.println(bfr.getPage("X",1).table_name);
   //System.out.println(test.table_name);

    }
}
