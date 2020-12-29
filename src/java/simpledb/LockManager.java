package simpledb;

import java.security.acl.Permission;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class LockManager {
    public ConcurrentHashMap<PageId,PageLock> pageIdLockMap;
    public ConcurrentHashMap<TransactionId,ArrayList<PageId>> transactionPageIdMap ;

    private final int MIN_TIME = 100, MAX_TIME = 1000;
    private final Random rand = new Random();


    enum LockType{
        SLock, XLock;
    }

    public class PageLock{

        final PageId pageId;
        LockType lockType;
        ArrayList<TransactionId> transactionIds;
        final Permissions permissions;

        public PageLock(PageId pageId, Permissions permissions, ArrayList<TransactionId> transactionIds){
            this.pageId = pageId;
            this.permissions = permissions;
            this.transactionIds = transactionIds;
            if(permissions.equals(Permissions.READ_ONLY)){
                this.lockType = LockType.SLock;
            }else{
                this.lockType = LockType.XLock;
            }
        }

        public void setType(Permission permission) {
            if(permission.equals(Permissions.READ_ONLY)){
                this.lockType = LockType.SLock;
            }else{
                this.lockType = LockType.XLock;
            }
        }

        public LockType getType() {
            return lockType;
        }

        public PageId getPageId() {
            return pageId;
        }

        public ArrayList<TransactionId> getTransactionIds() {
            return transactionIds;
        }

        public boolean tryUpgradeLock(TransactionId tid) {
            if (lockType == LockType.SLock && transactionIds.size() == 1 && transactionIds.get(0).equals(tid)) {
                lockType = LockType.XLock;
                return true;
            }
            return false;
        }

        public TransactionId addHolder(TransactionId tid) {
            if (lockType == LockType.SLock) {
                if (!transactionIds.contains(tid)) {
                    transactionIds.add(tid);
                }
                return tid;
            }
            return null;
        }

    }

    public LockManager(int lockTabCap, int transTabCap) {
        this.pageIdLockMap = new ConcurrentHashMap<>(lockTabCap);
        this.transactionPageIdMap = new ConcurrentHashMap<>(transTabCap);
    }

    public synchronized boolean holdsLock(TransactionId tid, PageId pid) {
        ArrayList<PageId> lockList = getLockList(tid);
        return lockList != null && lockList.contains(pid);
    }

    private synchronized void block(PageId pageId, long start, long timeout)
            throws TransactionAbortedException {
        // activate blocking
        if (System.currentTimeMillis() - start > timeout) {
            throw new TransactionAbortedException();
        }
        try {
            wait(timeout);
            if (System.currentTimeMillis() - start > timeout) {
                throw new TransactionAbortedException();
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    private synchronized void updateTransactionTable(TransactionId transactionId, PageId pageId) {
        if (transactionPageIdMap.containsKey(transactionId)) {
            if (!transactionPageIdMap.get(transactionId).contains(pageId)) {
                transactionPageIdMap.get(transactionId).add(pageId);
            }
        } else {
            // no entry tid
            ArrayList<PageId> lockList = new ArrayList<PageId>();
            lockList.add(pageId);
            transactionPageIdMap.put(transactionId, lockList);
        }
    }

    public synchronized void acquireLock(PageId pageId,TransactionId transactionId,Permissions permission) throws TransactionAbortedException{
        long start = System.currentTimeMillis();
        long maxTime = MIN_TIME + rand.nextInt(MAX_TIME - MIN_TIME);
        while(true){
            if (pageIdLockMap.containsKey(pageId)) {
                // page is locked by some transaction
                if (pageIdLockMap.get(pageId).getType() == LockType.SLock) {
                    if (permission == Permissions.READ_ONLY) {
                        //rr
                        updateTransactionTable(transactionId, pageId);
                        assert pageIdLockMap.get(pageId).addHolder(transactionId) != null;
                        return;
                    } else {
                        //rw
                        if (transactionPageIdMap.containsKey(transactionId) && transactionPageIdMap.get(transactionId).contains(pageId)
                                && pageIdLockMap.get(pageId).getTransactionIds().size() == 1) {
                            // if only this transaction read this page and now need write it, upgrade it.
                            assert pageIdLockMap.get(pageId).getTransactionIds().get(0) == transactionId;
                            // this is a combined case when lock on pid hold only by one trans (which is exactly tid)
                            pageIdLockMap.get(pageId).tryUpgradeLock(transactionId);
                            // isAcquired = true;
                            return;
                        } else {
                            // all need to do is just blocking
                            block(pageId, start, maxTime);
                        }
                    }
                } else {
                    // already get a Xlock on pid
                    if (pageIdLockMap.get(pageId).getTransactionIds().get(0) == transactionId) {
                        // Xlock means only one holder
                        // request xlock or slock on the pid with that tid
                        // sanity check
                        assert pageIdLockMap.get(pageId).getTransactionIds().size() == 1;
                        // isAcquired = true;
                        return;
                    } else {
                        // otherwise block
                        block(pageId, start, maxTime);
                    }
                }
            } else {
                // this page has not been held by any transaction
                ArrayList<TransactionId> transactionIds = new ArrayList<>();
                transactionIds.add(transactionId);
                pageIdLockMap.put(pageId, new PageLock(pageId, permission,transactionIds));
                updateTransactionTable(transactionId, pageId);
                return;
            }
        }
    }

    public synchronized void releaseLock(TransactionId tid, PageId pid) {
        // remove from trans table
        if (transactionPageIdMap.containsKey(tid)) {
            transactionPageIdMap.get(tid).remove(pid);
            if (transactionPageIdMap.get(tid).size() == 0) {
                transactionPageIdMap.remove(tid);
            }
        }

        // remove from locktable
        if (pageIdLockMap.containsKey(pid)) {
            pageIdLockMap.get(pid).getTransactionIds().remove(tid);
            if (pageIdLockMap.get(pid).getTransactionIds().size() == 0) {
                pageIdLockMap.remove(pid);
            } else {
                notifyAll();
            }
        }
    }

    public synchronized void releaseLocksOnTransaction(TransactionId tid) {
        if (transactionPageIdMap.containsKey(tid)) {
            PageId[] pidArr = new PageId[transactionPageIdMap.get(tid).size()];
            PageId[] toRelease = transactionPageIdMap.get(tid).toArray(pidArr);
            for (PageId pid : toRelease) {
                releaseLock(tid, pid);
            }

        }
    }

    public synchronized ArrayList<PageId> getLockList(TransactionId tid) {
        return transactionPageIdMap.getOrDefault(tid, null);
    }
}
