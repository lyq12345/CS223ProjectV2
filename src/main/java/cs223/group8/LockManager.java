package cs223.group8;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class LockManager {
    // {DataItemName -> {nodeTxnName -> Boolean (isLeaderWriteLock?)}}
    // LeaderWriteLock <> LeaderWriteLock, FollowerReadLock <> FollowerReadLock, LeaderWriteLock X FollowerReadLock
    public static final HashMap<String, HashMap<String, Boolean>> lockTable = new HashMap<>();
    // Check if able to add this lock
    public static boolean testOrAddLock(String nodeTxnName, String dataItemName, Boolean isLeaderWriteLock, Boolean addLock) {
        if (lockTable.containsKey(dataItemName)) {
            // key: nodeTxnName, value: lockType (Boolean)
            for(Map.Entry<String, Boolean> entry: lockTable.get(dataItemName).entrySet()) {
                if (!Objects.equals(entry.getKey(), nodeTxnName) && isLeaderWriteLock != entry.getValue()) {
                    // Incompatible lock types
                    return false;
                }
            }
            // No conflict, add lock
            if (addLock) {
                lockTable.get(dataItemName).put(nodeTxnName, isLeaderWriteLock);
            }
        } else {
            if (addLock) {
                HashMap<String, Boolean> locks = new HashMap<>();
                locks.put(nodeTxnName, isLeaderWriteLock);
                lockTable.put(dataItemName, locks);
            }
        }
        return true;
    }

    public static boolean releaseLock(String nodeTxnName, String dataItemName, Boolean isLeaderWriteLock) {
        if (lockTable.containsKey(dataItemName)) {
            // key: nodeTxnName, value: lockType (Boolean)
            if (lockTable.get(dataItemName).containsKey(nodeTxnName)) {
                if (lockTable.get(dataItemName).get(nodeTxnName) == isLeaderWriteLock) {
                    lockTable.get(dataItemName).remove(nodeTxnName);
                    return true;
                }
            }
        }
        return false;
    }
}
