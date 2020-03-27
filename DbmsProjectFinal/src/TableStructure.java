import java.io.File;

public class TableStructure {
    byte[] ID;
    byte[] clientName;
    byte[] phone;

    int MAX_ID_SIZE = 4 , MAX_CLIENTNAME_SIZE = 16 , MAX_PHONE_SIZE = 12;

    public TableStructure(String ID,String clientName,String phone)
    {
        this.ID = new byte[MAX_ID_SIZE];
        this.clientName = new byte[MAX_CLIENTNAME_SIZE];
        this.phone = new byte[MAX_PHONE_SIZE];

        this.ID = ID.getBytes();
        this.clientName = clientName.getBytes();
        this.phone = phone.getBytes();
    }


    public static void main(String[] args)
    {
        /*
        byte[] sample = new byte[10];

        String test = "432";
        int id = Integer.parseInt(test);

        ByteBuffer b = ByteBuffer.allocate(id);
        b.putInt(0xAABBCCDD);

        sample = b.array();

        for(byte a: b.array())
        {
            System.out.println(a);
        }

        System.out.println(sample.length);

        /*
        for(byte a:sample)
        {
            System.out.println((char)a);
        }
        */
        File f = new File("Database//Y//slotted_Y1.txt");

        System.out.println("Length of the file " + f.length());

        String a = "Hello my name is antony";
        System.out.println(a.getBytes().length);
    }
}
