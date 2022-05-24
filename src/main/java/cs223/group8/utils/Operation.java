package cs223.group8.utils;

public class Operation {
    public static final String COMMIT = "C";
    public static final String WRITE = "W";
    public static final String READ = "R";


    private String opType;

    private String key;
    private Integer value;

    public Operation(String opType, String cmd){
        this.opType = opType;

        String[] split = cmd.split("=");
        this.key = split[0];

        if(this.opType.equals(WRITE)){
            this.value = Integer.parseInt(split[1]);
        }

    }

    public void log(){
        if(this.opType.equals(COMMIT))
            System.out.print(this.opType + ";");
        if(this.opType.equals(READ))
            System.out.print(String.format("%s(%s);", this.opType, this.key));
        if(this.opType.equals(WRITE))
            System.out.print(String.format("%s(%s=%d);", this.opType, this.key, this.value));
    }

    public String getOpType() {
        return opType;
    }

    public void setOpType(String opType) {
        this.opType = opType;
    }

    public String getKey() {
        return key;
    }

    public Integer getValue() {
        return value;
    }
}
