package simpledb;

import java.util.HashSet;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class LockManager {
    private static final int TIMEOUT = 500;

    private Map<PageId, PageLock> pageIdToTid;
    private Map<TransactionId, Set<PageId>> tidToPageSet;

    class PageLock {
        public Set<TransactionId> readLockTidSet;
        public TransactionId writeTid;

        public PageLock() {
            this.readLockTidSet = new HashSet<>();
            this.writeTid = null;
        }

        public boolean canUpgrade(TransactionId tid) {
            return this.writeTid == null && this.readLockTidSet.size() == 1 && this.readLockTidSet.contains(tid);
        }
    }

    public LockManager() {
        this.pageIdToTid = new ConcurrentHashMap<>();
        this.tidToPageSet = new ConcurrentHashMap<>();
    }

    public void aquireLock(TransactionId tid, PageId pid, Permissions perm) throws TransactionAbortedException, DbException {
        long start = System.currentTimeMillis();
        Random random = new Random(start);
        while (true) {
            if ((System.currentTimeMillis() - start) > TIMEOUT) {
                throw new TransactionAbortedException();
            }

            boolean status;
            if (perm == Permissions.READ_ONLY) {
                status = this.aquireReadLock(tid, pid);
            } else if (perm == Permissions.READ_WRITE) {
                status = this.aquireReadWriteLock(tid, pid);
            } else {
                throw new DbException("Unknow permission: " + perm);
            }

            if (status) {
                break;
            } else {
                try {
                    Thread.sleep(random.nextInt(25) + 1);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    private boolean aquireReadLock(TransactionId tid, PageId pid) {
        synchronized (pid) {
            PageLock pl = this.pageIdToTid.getOrDefault(pid, new PageLock());
            if (pl.writeTid == tid) {
                return true;
            }
            if (pl.writeTid != null) {
                return false;
            }
            pl.readLockTidSet.add(tid);
            this.pageIdToTid.put(pid, pl);
            this.updateTransactionRecord(tid, pid);
            return true;
        }
    }

    private boolean aquireReadWriteLock(TransactionId tid, PageId pid) {
        synchronized (pid) {
            PageLock pl = this.pageIdToTid.getOrDefault(pid, new PageLock());
            if (pl.writeTid != null && pl.writeTid != tid) {
                return false;
            }
            // upgrade read lock to read/write lock
            if (pl.canUpgrade(tid)) {
                pl.writeTid = tid;
                pl.readLockTidSet.remove(tid);
                this.pageIdToTid.put(pid, pl);
                return true;
            }
            if (pl.readLockTidSet.size() == 0) {
                pl.writeTid = tid;
                this.pageIdToTid.put(pid, pl);
                this.updateTransactionRecord(tid, pid);
                return true;
            }
            return false;
        }
    }

    public void releaesLock(TransactionId tid) {
        for (PageId pid : this.getLockedPageIdSet(tid)) {
            this.releaseLock(tid, pid);
        }
    }

    public void releaseLock(TransactionId tid, PageId pid) {
        if (this.pageIdToTid.containsKey(pid)) {
            PageLock pl = this.pageIdToTid.get(pid);
            if (pl.writeTid == tid) {
                pl.writeTid = null;
            } else {
                pl.readLockTidSet.remove(tid);
            }
            if (pl.writeTid == null && pl.readLockTidSet.isEmpty()) {
                this.pageIdToTid.remove(pid);
            }
            if (this.tidToPageSet.containsKey(tid)) {
                this.tidToPageSet.get(tid).remove(pid);
                if (this.tidToPageSet.get(tid).isEmpty()) {
                    this.tidToPageSet.remove(tid);
                }
            }
        }
    }

    private void updateTransactionRecord(TransactionId tid, PageId pid) {
        Set<PageId> pidSet = this.tidToPageSet.getOrDefault(tid, new HashSet<>());
        pidSet.add(pid);
        this.tidToPageSet.put(tid, pidSet);
    }

    public boolean holdsLock(TransactionId tid, PageId pid) {
        return this.tidToPageSet.containsKey(tid) && this.tidToPageSet.get(tid).contains(pid);
    }

    public Set<PageId> getLockedPageIdSet(TransactionId tid) {
        return this.tidToPageSet.getOrDefault(tid, new HashSet<>());
    }
}
