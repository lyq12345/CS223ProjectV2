package cs223.group8;

import cs223.group8.entity.DataItem;
import cs223.group8.repository.GeneralDatasourceRepository;
import cs223.group8.session.GeneralSessionConfig;
import cs223.group8.utils.LogParser;
import cs223.group8.utils.Transaction;
import javafx.util.Pair;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;

public class WorkflowController {
    private HashMap<String, DataItem> data = new HashMap<>();
    private HashMap<String, Transaction> txns = new HashMap<>();
    private ArrayList<Transaction> commits = new ArrayList<>();
    private ArrayList<Transaction> rollbacks = new ArrayList<>();
    private GeneralDatasourceRepository generalDatasourceRepository = new GeneralDatasourceRepository();

    public static final String COMMIT = "C";
    public static final String WRITE = "W";
    public static final String READ = "R";

    private int failIndex = 0;

    private final TransactionManager transactionManager = new TransactionManager();;

    // File name is the node name
    public void load(String filename){
        try {
            String[] splitRes = filename.split("/");
            String nodeName = splitRes[splitRes.length-1];
            InputStreamReader reader = new InputStreamReader(
                    Files.newInputStream(Paths.get(filename)));
            BufferedReader br = new BufferedReader(reader);

            int n = Integer.parseInt(br.readLine());
            for(int i=0; i<n; i++){
                String line = br.readLine();
                String[] split = line.split("=");
                DataItem item = new DataItem(split[0], Integer.parseInt(split[1]));
                this.data.put(item.getKey(), item);
            }
            n = Integer.parseInt(br.readLine());
            for(int i=0; i<n; i++){
                String line = br.readLine();
                Transaction txn = new Transaction(line, this.data, nodeName, Objects.equals(this.transactionManager.getCurrentLeader(), nodeName));
                this.txns.put(txn.getName(), txn);
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public Transaction chooseTxn(){
        if(this.commits.size() == this.txns.size())
            return null;

        List list = new ArrayList(this.txns.keySet());
        if(this.rollbacks.size() > 0)
            return this.rollbacks.get(0);

        else{
            boolean goodChoice = false;
            Random r = new Random();
            Transaction txn = null;
            while(!goodChoice){
                int rand = r.nextInt(this.txns.size());
                txn = this.txns.get(list.get(rand));
                if(!this.commits.contains(txn))
                    goodChoice = true;

            }
            return txn;
        }

    }

    public void log(){
        for(Transaction txn: this.txns.values()){
            txn.log();
        }
    }

    public void run(){
        //count operations
        int opNum = 0;
        for(Transaction txn: txns.values()){
            opNum += txn.getOps().size();
        }
        Random r = new Random();
        failIndex = r.nextInt(opNum);

        while(true){
            this.log();
            Transaction txn = this.chooseTxn();

            // simulation of failure
//            if(failIndex == 0){
//                System.out.println("Failure happens, need to recover");
//                System.out.println("Creating new node...");
//                GeneralSessionConfig.createNewSession("backup_leader");
//                transactionManager.changeCurrentLeader("backup_leader");
//                System.out.println("Redo committed txns...");
//                try {
//                    InputStreamReader reader = new InputStreamReader(
//                            Files.newInputStream(Paths.get("logs/follower1_log.txt")));
//                    BufferedReader br = new BufferedReader(reader);
//                    String line = null;
//                    while((line = br.readLine()) != null){
//                        LogParser logParser = new LogParser("src/main/java/cs223/group8/logs/backup_leader_log.txt");
//                        logParser.writeEntry(line);
//                        String[] splits = line.split(";");
//                        for(String items: splits){
//                            String[] item = items.split("=");
//                            generalDatasourceRepository.writeItem(item[0], Integer.parseInt(item[1]));
//                        }
//                    }
//
//
//
//                } catch (IOException e) {
//                    e.printStackTrace();
//                }
//
//                System.out.println("Undo uncommited txns...");
//                for(Transaction trans: txns.values()) {
//                    if(!commits.contains(trans)){
//                        transactionManager.rollback(trans);
//                        if(!this.rollbacks.contains(trans))
//                            this.rollbacks.add(trans);
//                    }
//                }
//                continue;
//            }


            if(txn != null){

                // Check and add locks.
                if (txn.getPtr() == 0) {
                    if (!txn.addLocks())
                        continue;
                }

                // For data item involved in this operation, first check if has lock.
                if (!txn.canExecute()) {
                    System.out.print("Cannot execute: " + txn.getName() + "-");
                    txn.next().log();
                    System.out.print(" because it is blocked.");
                    System.out.println();
                    // Proceed without continuing because it is blocked.
                    continue;
                }

                System.out.print("execute: " + txn.getName() + "-");
                txn.next().log();
                System.out.println();
                Pair<String, HashMap<String, DataItem>> pair = transactionManager.processTransaction(txn);
                String message = pair.getKey();
                HashMap<String, DataItem> data = pair.getValue();
                if(message.equals("COMMITTED")){

                    // update unstarted transacitons' data
                    for(String key: this.txns.keySet()){
                        if(this.txns.get(key).getPtr() == 0)
                            this.txns.get(key).setData(data);
                    }

                    //add to commited list
                    this.commits.add(txn);

                    //if it has been rolled back before, remove it from rollback record
                    if(this.rollbacks.contains(txn)){
                        this.rollbacks.remove(txn);
                    }
                    // Release locks
                    txn.releaseLocks();
                } else if(message.equals("ABORT")){
                    // avoid repeated add
                    if(!this.rollbacks.contains(txn))
                        this.rollbacks.add(txn);
                    txn.releaseLocks();
                }
                System.out.println();
            }else{
                System.out.println();
                break;
            }

            failIndex--;

        }

        // print final schedule
        System.out.println("Schedule generated: ");
        transactionManager.printSchedule();
    }
}
