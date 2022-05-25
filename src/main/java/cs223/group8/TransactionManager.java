package cs223.group8;

import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import cs223.group8.entity.DataItem;
import cs223.group8.repository.FollowerOneDatasourceRepository;
import cs223.group8.repository.FollowerTwoDatasourceRepository;
import cs223.group8.repository.LeaderDatasouceRepository;
import cs223.group8.utils.LogParser;
import cs223.group8.utils.Operation;
import cs223.group8.utils.Transaction;
import javafx.util.Pair;

import javax.annotation.Resource;
import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;

public class TransactionManager {
//    @Resource
//    private DataItemMapper dataItemMapper;
//
    public static final String COMMIT = "C";
    public static final String WRITE = "W";
    public static final String READ = "R";

    //    private ArrayList<Transaction> commits = new ArrayList<>();
//    private ArrayList<Transaction> rollbacks = new ArrayList<>();
    private ArrayList<Transaction> TSs = new ArrayList<>();
    private HashMap<String, DataItem> data = new HashMap<>();
    private LeaderDatasouceRepository leaderDatasouceRepository;
    private String finalSchedule = "";

//    @Override
//    public Integer readItemValue(String key) {
//        QueryWrapper<DataItem> queryWrapper = new QueryWrapper<>();
//        queryWrapper.eq("key", key);
//        DataItem item = dataItemMapper.selectOne(queryWrapper);
//        return item.getValue();
//    }
//
//    @Override
//    public void writeItemValue(String key, Integer value) {
//        DataItem item = new DataItem(key, value);
//        QueryWrapper<DataItem> queryWrapper = new QueryWrapper<>();
//        queryWrapper.eq("key", key);
//        if(null == dataItemMapper.selectOne(queryWrapper)){
//            dataItemMapper.insert(item);
//        }else{
//            dataItemMapper.update(item, queryWrapper);
//        }
//    }
//
//    @Override
////    @Transactional
//    public synchronized void transactionWithSychronized() {
//        QueryWrapper<DataItem> queryWrapper = new QueryWrapper<>();
//        queryWrapper.eq("key", "x");
//        DataItem item = dataItemMapper.selectOne(queryWrapper);
//        item.setValue(item.getValue() + 1);
//        dataItemMapper.update(item, queryWrapper);
//    }
    public TransactionManager() {
        leaderDatasouceRepository = new LeaderDatasouceRepository();
    }


    public Pair<String, HashMap<String, DataItem>> processTransaction(Transaction txn) {
        Operation op = txn.exec();
        String opType = op.getOpType();
        String key = op.getKey();

        if(opType.equals(READ)){
//            System.out.println("Read the value");
            this.finalSchedule += String.format("%s-%s(%s); ", txn.getName(), opType, key);
            return new Pair<>("READ", this.data);
        }
        else if(opType.equals(WRITE)){
//            System.out.println("Write the value");
            Integer value = op.getValue();
            this.finalSchedule += String.format("%s-%s(%s=%s); ", txn.getName(), opType, key, value);
            return new Pair<>("WRITE", this.data);
        }

        // If the transaction enters the validation phase
        if(opType.equals(COMMIT)){
            this.TSs.add(txn);
            boolean success = true;
            for(int i=0; i<this.TSs.size()-1; i++){
                success = this.compare(txn, this.TSs.get(i));
                if(!success) break;
            }

            if(success){
                //enter the write phase
                this.finalSchedule += String.format("%s-%s; ", txn.getName(), opType);
                this.data = txn.getData();
                txn.setFinishTS(new Timestamp(System.currentTimeMillis()));

                System.out.println("data after " + txn.getName() + " is committed: ");


                LogParser logParser = new LogParser("src/main/java/cs223/group8/logs/leader_log.txt");
                String entry = "";
                //apply to the database
                for(DataItem item: this.data.values()){
                    leaderDatasouceRepository.writeItem(item.getKey(), item.getValue());
                    entry += item.log();
                }
                logParser.writeEntry(entry);
                System.out.println();

                //TODO: Synchronize with follower1 and follower2
                System.out.println("Synchronizing with follower1...");
                FollowerOneDatasourceRepository followerOneDatasourceRepository = new FollowerOneDatasourceRepository();
                followerOneDatasourceRepository.SynchronizeWithLeader(entry);

                System.out.println("Synchronizing with follower2...");
                FollowerTwoDatasourceRepository followerTwoDatasourceRepository = new FollowerTwoDatasourceRepository();
                followerTwoDatasourceRepository.SynchronizeWithLeader(entry);

                return new Pair<>("COMMITTED", this.data);
            }

            //if failed, rollback
            else{
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
                return new Pair<>("ABORT", null);
            }
        }

        return null;
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

    public void printSchedule(){
        System.out.println(finalSchedule);
    }
}
