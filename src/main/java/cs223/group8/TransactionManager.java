package cs223.group8;

import cs223.group8.entity.DataItem;
import cs223.group8.repository.GeneralDatasourceRepository;
import cs223.group8.session.GeneralSessionConfig;
import cs223.group8.utils.LogParser;
import cs223.group8.utils.Operation;
import cs223.group8.utils.Transaction;
import javafx.util.Pair;
import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;

public class TransactionManager {

    public static final String COMMIT = "C";
    public static final String WRITE = "W";
    public static final String READ = "R";

    private ArrayList<Transaction> TSs = new ArrayList<>();
    private HashMap<String, DataItem> data = new HashMap<>();
    private GeneralDatasourceRepository generalDatasourceRepository;
    private String finalSchedule = "";

    public String getCurrentLeader() {
        return currentLeader;
    }

    private String currentLeader = "leader";


    public TransactionManager() {
        generalDatasourceRepository = new GeneralDatasourceRepository();
    }


    public Pair<String, HashMap<String, DataItem>> processTransaction(Transaction txn) {
        Operation op = txn.exec();
        String opType = op.getOpType();
        String key = op.getKey();

        if(opType.equals(READ)){
            this.finalSchedule += String.format("%s-%s(%s); ", txn.getName(), opType, key);
            return new Pair<>("READ", this.data);
        }
        else if(opType.equals(WRITE)){
            Integer value = op.getValue();
            this.finalSchedule += String.format("%s-%s(%s=%s); ", txn.getName(), opType, key, value);
            return new Pair<>("WRITE", this.data);
        }

        // If the transaction enters the validation phase
        if(opType.equals(COMMIT)){
            boolean success = true;
            if (txn.isLeader) {
                this.TSs.add(txn);
                for(int i=0; i<this.TSs.size()-1; i++){
                    success = this.compare(txn, this.TSs.get(i));
                    if(!success) break;
                }
            }

            if(success){
                //enter the write phase
                this.finalSchedule += String.format("%s-%s; ", txn.getName(), opType);
                this.data = txn.getData();
                txn.setFinishTS(new Timestamp(System.currentTimeMillis()));

                System.out.println("data after " + txn.getName() + " is committed: ");

                if (txn.isLeader) {
                    LogParser logParser = new LogParser("logs/" + currentLeader + "_log.txt");
                    String entry = "";
                    //apply to the database
                    for(DataItem item: this.data.values()){
                        generalDatasourceRepository.writeItem(item.getKey(), item.getValue());
                        entry += item.log();
                    }
                    logParser.writeEntry(entry);
                    System.out.println();

                    System.out.println("Synchronizing with follower1...");
                    GeneralSessionConfig.changeSession("follower1");
                    generalDatasourceRepository.SynchronizeWithLeader(entry, "logs/follower1_log.txt");

                    System.out.println("Synchronizing with follower2...");
                    GeneralSessionConfig.changeSession("follower2");
                    generalDatasourceRepository.SynchronizeWithLeader(entry, "logs/follower2_log.txt");

                    GeneralSessionConfig.changeSession(currentLeader);
                }

                return new Pair<>("COMMITTED", this.data);
            }

            // TODO: follower do not need to roll back
            //if failed, rollback
            else{
                if (txn.isLeader) {
                    rollback(txn);
                    return new Pair<>("ABORT", null);
                } else {
                    return new Pair<>("COMMITTED", this.data);
                }

            }
        }

        return null;
    }

    public void rollback(Transaction txn) {
        System.out.println(txn.getName() + "'s validation failed. Must rollback.");
        this.finalSchedule += String.format("%s-A; ", txn.getName());
        DateFormat dateFormat1 = new SimpleDateFormat("yyyy-MM-dd");

        Date myDate1 = null;
        try{
            myDate1 = dateFormat1.parse("3000-01-01");
        } catch (ParseException e) {
            e.printStackTrace();
        }
        Timestamp FAR_FUTURE_TIME = new Timestamp(myDate1.getTime());
        txn.setStartTS(FAR_FUTURE_TIME);
        txn.setValidationTS(FAR_FUTURE_TIME);
        txn.setFinishTS(FAR_FUTURE_TIME);
        txn.setPtr(0);
        txn.setData(this.data);
        this.TSs.remove(txn);
    }

    private boolean compare(Transaction tr1, Transaction tr2) {
        if(tr2.getFinishTS().before(tr1.getStartTS())){
            return true;
        }
        if(tr1.getStartTS().before(tr2.getFinishTS()) && tr2.getFinishTS().before(tr1.getValidationTS())){
            // get intersection
            HashSet<String> intersection = new HashSet<>();
            intersection.addAll(tr1.getReadSet());
            intersection.retainAll(tr2.getWriteSet());
            if(intersection.isEmpty())
                return true;
        }
        return false;
    }

    public void changeCurrentLeader(String leaderName){
        this.currentLeader = leaderName;
    }

    public void printSchedule(){
        System.out.println(finalSchedule);
    }

}
