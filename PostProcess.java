/**
 * Created by Pedrum on 12/11/2016.
 */

import MySqlJava.Chunk;
import MySqlJava.SqlConnect;
import MySqlJava.dbParams;

import java.sql.ResultSet;
import java.sql.SQLException;

/*after generating flows, the table needs to be adjusted to be compatible
with the requirements of the next set of functions.
 */
public class PostProcess {
    private SqlConnect sqlConnect;
    private String database;
    private String table;
    public PostProcess(dbParams __dbparams, String __table) throws SQLException {
        sqlConnect = new SqlConnect(__dbparams);
        database = __dbparams.dbName;
        table = __table;
    }

    public void run() throws SQLException
    {
        reduceExtraMin();
        //removeExtraNulls();

    }

    //as part of the algorithm the flow lengths will over estimated by 60s. This will
    //remove that effect
    private void reduceExtraMin() throws SQLException{
        String _query = " update "+database+"." + table + "_flows\n" +
                " set end_time = end_time-60";
        sqlConnect.updateQuery(_query);
    }

    //if for some reason there are some nulls in the flows, they are removed here.
    private void removeExtraNulls() throws SQLException{
        String _query = "delete from "+database+"." + table + "_flows\n" +
                "where (ip_src <=1 or ip_dst <=1 or port_src <=0 or port_dst <=0) and idflows > 1";
        sqlConnect.updateQuery(_query);
    }


    private void fillFlowCol() throws SQLException{
        String _query = "delete from "+database+"." + table + "_flows\n" +
                "where (ip_src <=1 or ip_dst <=1 or port_src <=0 or port_dst <=0) and idflows > 1";
        sqlConnect.updateQuery(_query);
    }









}
