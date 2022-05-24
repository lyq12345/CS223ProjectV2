package cs223.group8.utils;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;

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

    public void readEntry() {

    }
}
