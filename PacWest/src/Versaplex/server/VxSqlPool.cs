using System.Net.Sockets;

namespace versabanq.Versaplex.Server {

public class VxSqlPool
{
    private object poolsize_lock;

    private int poolmin = 1;
    public int PoolMin {
        get { lock (poolsize_lock) return poolmin; }
        set {
            if (value < 0)
                throw new ArgumentOutOfRangeException(
                        "Pool minimum must be at least 0");

            lock (poolsize_lock) {
                poolmin = value;

                LockedUpdatePoolSize();
            }
        }
    }

    private static int poolmax = 5;
    public int PoolMax {
        get { lock (poolsize_lock) return poolmax; }
        set {
            if (value < 1)
                throw new ArgumentOutOfRangeException(
                        "Pool maximum must be at least 1");

            if (value < poolmin)
                throw new ArgumentException(
                        "Pool maximum must be greater than pool minimum");

            lock (poolsize_lock) {
                poolmin = value;

                LockedUpdatePoolSize();
            }
        }
    }

    public VxSqlPool()
    {
    }

    public SqlConnection TakeConnection()
    {
        // TODO
    }

    public void ReleaseConnection(SqlConnection c)
    {
        // TODO
    }

    private void LockedUpdatePoolSize()
    {
        // TODO
    }
}

}
