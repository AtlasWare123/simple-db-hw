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

    public enum LockType {
        SLock,  // read only
        XLock,  // read & write
        UnLock  // unlock
    }

    class PageLock {
        Set<TransactionId> lockTidSet;
        LockType type;

        public PageLock() {
            this.lockTidSet = new HashSet<>();
            this.type = LockType.UnLock;
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
            if (pl.type == LockType.XLock) {
                return pl.lockTidSet.contains(tid);
            }
            pl.lockTidSet.add(tid);
            pl.type = LockType.SLock;
            this.pageIdToTid.put(pid, pl);
            this.updateTransactionRecord(tid, pid);
            return true;
        }
    }

    private boolean aquireReadWriteLock(TransactionId tid, PageId pid) {
        synchronized (pid) {
            PageLock pl = this.pageIdToTid.getOrDefault(pid, new PageLock());
            if (pl.type == LockType.XLock) {
                return pl.lockTidSet.contains(tid);
            }
            if ((pl.type == LockType.SLock && pl.lockTidSet.size() == 1 && pl.lockTidSet.contains(tid))
                    || pl.type == LockType.UnLock) {
                pl.lockTidSet.add(tid);
                pl.type = LockType.XLock;
                this.pageIdToTid.put(pid, pl);
                this.updateTransactionRecord(tid, pid);
                return true;
            }
            return false;
        }
    }

    public void releaseLock(TransactionId tid) {
        if (this.tidToPageSet.containsKey(tid)) {
            for (PageId pid : this.tidToPageSet.get(tid)) {
                synchronized (pid) {
                    if (this.pageIdToTid.containsKey(pid)) {
                        PageLock lock = this.pageIdToTid.get(pid);
                        lock.lockTidSet.remove(tid);
                        if (lock.lockTidSet.size() == 0) {
                            lock.type = LockType.UnLock;
                            this.pageIdToTid.remove(pid);
                        }
                    }
                }
            }
            this.tidToPageSet.remove(tid);
        }
    }

    public void releaseLock(TransactionId tid, PageId pid) {
        synchronized (pid) {
            if (this.pageIdToTid.containsKey(pid)) {
                PageLock pl = this.pageIdToTid.get(pid);
                pl.lockTidSet.remove(tid);
                if (pl.lockTidSet.size() == 0) {
                    pl.type = LockType.UnLock;
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
