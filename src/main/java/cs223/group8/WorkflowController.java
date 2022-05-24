package cs223.group8;

import cs223.group8.entity.DataItem;
import cs223.group8.utils.Transaction;
import javafx.util.Pair;

import javax.annotation.Resource;
import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Random;

public class WorkflowController {
    private HashMap<String, DataItem> data = new HashMap<>();
    private HashMap<String, Transaction> txns = new HashMap<>();
    private ArrayList<Transaction> commits = new ArrayList<>();
    private ArrayList<Transaction> rollbacks = new ArrayList<>();

    public static final String COMMIT = "C";
    public static final String WRITE = "W";
    public static final String READ = "R";

    private TransactionManager transactionManager;

//    @Autowired
//    private static TransactionManager transactionManager;
//
//    @PostConstruct
//    public void init() {
//        transactionManager = this;
//        transactionManager.dataItemService = this.dataItemService;
//    }


//    public TransactionManager(){
////        this.load(filename);
//    }

    public void load(String filename){
        transactionManager = new TransactionManager();
        try {
            InputStreamReader reader = new InputStreamReader(
                    new FileInputStream(filename));
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
                Transaction txn = new Transaction(line, this.data);
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
        while(true){
            this.log();
            Transaction txn = this.chooseTxn();
            if(txn != null){
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
                    //log
                    this.commits.add(txn);

                    //if it has been rolled back before, remove it from rollback record
                    if(this.rollbacks.contains(txn)){
                        this.rollbacks.remove(txn);
                    }
                } else if(message.equals("ABORT")){
                    if(!this.rollbacks.contains(txn))
                        this.rollbacks.add(txn);
                }
                System.out.println();
            }else{
                System.out.println();
                break;
            }

        }

        // TODO: print final schedule
        System.out.println("Schedule generated: ");
    }
}
