package com.lsm;

import java.io.*;
import java.util.Scanner;

public class Benchmark {

    static File file = new File("./src/com/lsm/scripts");

    public static void Generator(String s, int loop1, int loop2) throws IOException{
        for (int i = 0; i < loop1; i++){
            for (int j = 0; j < loop2; j++) {
                String filename = String.valueOf(i * loop2 + 1 + j);
                FileWriter fw = new FileWriter(file + "\\" + filename, true);
                String originalLine = "B0";
                fw.write(originalLine);
                fw.write("\r\n");
                char ch = 'A';
                int a = (int) ch + i;
                int c = (int)ch + j;
                char b = (char) a;
                if (s.substring(0, 1).equals("R")){
                    originalLine = "R " + String.valueOf(b) + " 1";
                    fw.write(originalLine);
                    if (s.substring(1, 2).equals("W"))
                        originalLine = "W " + String.valueOf(b) + " (1, Thalia, 412-111-1324)";
                    else if (s.substring(1, 2).equals("D"))
                        originalLine = "D " + String.valueOf(b);
                    else
                        originalLine = "E " + String.valueOf(b) + " 1";
                    fw.write("\r\n");
                    fw.write(originalLine);
                } else if (s.substring(0, 1).equals("W")){
                    char d = (char) c;
                    char e = (char) (c + 1);
                    originalLine = "W " + String.valueOf(d) + " (" + (i+1) +", Thalia, 412-111-1324)";
                    fw.write(originalLine);
                    fw.write("\r\n");
                    if (s.substring(1, 2).equals("D")) {
                        if (j != loop2-1) originalLine = "D " + String.valueOf(e);
                        else originalLine = "D A";
                    }
                    else{
                        if (j != loop2-1) originalLine = "E " + String.valueOf(e) + " " + (i+1);
                        else originalLine = "E A " + (i+1);
                    }
                    fw.write(originalLine);
                }
                else{
                    char d = (char) c;
                    char e = (char) (c + 1);
                    originalLine = "W " + String.valueOf(d) + " (" + (i+1) +", Thalia, 412-111-1324)";
                    fw.write(originalLine);
                    fw.write("\r\n");
                    originalLine = "E " + String.valueOf(d) + " " + (i+1);
                    fw.write(originalLine);
                    fw.write("\r\n");
                    if (j != loop2-1) originalLine = "D " + String.valueOf(e);
                    else originalLine = "D A";
                    fw.write(originalLine);
                }
                originalLine = "C";
                fw.write("\r\n");
                fw.write(originalLine);
                fw.close();
            }
        }
    }

    public static void Delete() throws IOException{
        File[] files = file.listFiles();
        for (File f: files){
            f.delete();
        }
    }

    public static void main(String[] args) throws IOException {
        Scanner myObj = new Scanner(System.in);
        System.out.println("Enter the conflict operation ('RD', 'RE', 'RW', 'WD', 'WE', 'ED'):");
        String Conflict  = myObj.nextLine();
        if((!Conflict.equals("RD")) && (!Conflict.equals("RE")) && (!Conflict.equals("RW")) && (!Conflict.equals("WD")) && (!Conflict.equals("WE")) && (!Conflict.equals("ED"))){
            System.out.println("Wrong conflict mode in Benchmark.java");
            System.exit(1);
        }
        System.out.println("Enter the loop count of the generator:");
        int loop1 = myObj.nextInt();
        System.out.println("Enter the loop length of the benchmark (For the 'WD', 'WE' and 'ED' conflict mode, it should be less than 10):");
        int loop2 = myObj.nextInt();
        if((loop1 <= 0) || (loop2 <= 0)){
            System.out.println("The loop size should be larger than 1");
            System.exit(1);
        }
        Delete();
        Generator(Conflict,loop1,loop2);
    }
}
