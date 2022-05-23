package cs223.group8.entity;

import lombok.Data;

@Data
public class DataItem {
    private String key;
    private Integer value;

    public DataItem(String key, Integer value) {
        this.key = key;
        this.value = value;
    }

    public void log(){
        System.out.print("|" + key + ":"+value+"|");
    }
}
