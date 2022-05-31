package cs223.group8.utils;

import cs223.group8.entity.DataItem;

import java.io.*;
import java.util.Random;

public class LogParser {
    private String filename;
    public LogParser(String filename){
        this.filename = filename;
    }

    public void writeEntry(String entry){
        try {
            BufferedWriter out = new BufferedWriter(new FileWriter(filename, true));
            out.write(entry);
            out.write('\n');
            out.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

//    public String readEntry() {
//    }
}
