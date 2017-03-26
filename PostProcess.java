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
    public PostProcess(dbParams __dbparams) throws SQLException {
        sqlConnect = new SqlConnect(__dbparams);
        database = __dbparams.dbName;
    }

    public void run() throws SQLException
    {
        reduceExtraMin();
        removeExtraNulls();
        //addtagColumn();
        //addIndices();
        //renameCols();
    }

    //as part of the algorithm the flow lengths will over estimated by 60s. This will
    //remove that effect
    private void reduceExtraMin() throws SQLException{
        String _query = " update "+database+".flows\n" +
                " set endTime = date_sub(endTime, INTERVAL 1 minute)";
        sqlConnect.updateQuery(_query);
    }

    //if for some reason there are some nulls in the flows, they are removed here.
    private void removeExtraNulls() throws SQLException{
        String _query = "delete from "+database+".flows\n" +
                "where (ipSrc =1 or ipDst =1 or portSrc =0 or portDst =0) and idFlows > 1";
        sqlConnect.updateQuery(_query);
    }

    //columns are named according to the net_con_count package convention
    private void renameCols() throws SQLException{
        String _query = "ALTER TABLE "+database+".flows\n" +
                        " CHANGE COLUMN ipSrc source INT(11) UNSIGNED NOT NULL ,\n" +
                        " CHANGE COLUMN ipDst dest INT(11) UNSIGNED NOT NULL ;";
        sqlConnect.updateQuery(_query);
    }

    //indices are added as required by the net_con_count pacakge
    private void addIndices() throws SQLException{

        String _query = "ALTER TABLE "+database+".flows\n" +
                "         ADD INDEX count (ipDst ASC, startTime ASC);";
        sqlConnect.updateQuery(_query);
    }

    //the net_con_count package required a tag parameter
    private void addtagColumn() throws SQLException{

        String _query = "ALTER TABLE "+database+".flows\n" +
                "        ADD COLUMN tag CHAR(20) NOT NULL DEFAULT 'Normal' AFTER endTime";
        sqlConnect.updateQuery(_query);
    }







}
