/**
 * Created by Pedrum on 12/11/2016.
 */

import java.sql.*;
import MySqlJava.*;


//handles connections to our database
public class QueriesFlows {


    private int flowRow;
    private int FLOWTIMEOUT = 60;
    private String database;
    private int getSize = 1000;
    private SqlConnect sqlConnect;
    private String insFlow;
    private java.util.Date _start;
    private Chunk chunk;
    public double laps;

    public QueriesFlows(dbParams __dbparams) throws SQLException, ClassNotFoundException {
        flowRow =2;
        sqlConnect = new SqlConnect(__dbparams);
        database = __dbparams.dbName;
        chunk =new Chunk(__dbparams, "SELECT time, least(ipSrc, ipDst) as ipSrc, greatest(ipSrc, ipDst) as ipDst, " +
                " least(portSrc, portDst) as portSrc, greatest(portSrc, portDst) as portDst from " + database + ".packets  order by time asc ", getSize);

    }

    public void update_row(){
        flowRow ++;
    }

    public void Insert_Flow(Packet __packet) throws SQLException{

        String query = "select * from "+database+".flows " +
                " where ipSrc =  " + __packet.ipSrc +
                " and ipDst =  " + __packet.ipDst +
                " and portSrc =  " + __packet.portSrc +
                " and portDst =  " + __packet.portDst +
                " and endTime >= from_unixtime(" + __packet.time + ")" +
                " and startTime <= from_unixtime(" +__packet.time + ");";

        ResultSet rs = sqlConnect.executeQuery(query);
        if (!rs.next()){
            update_row();
            query = "INSERT INTO "+database+".flows " +
                    "(idFlows ,  ipSrc, ipDst, portSrc, portDst, startTime, endTime) " +
                    " VALUES (" + Integer.toString(flowRow) +", "
                    +(__packet.ipSrc) +", " +
                    (__packet.ipDst) +", " +
                    (__packet.portSrc) +", " +
                    (__packet.portDst) + ", " +
                    " from_unixtime(" + (__packet.time) +"), " +
                    " from_unixtime( " + (__packet.time+FLOWTIMEOUT) + "));";

            sqlConnect.updateQuery(query);
        }
        else {
            query = "update "+database+".flows " +
                    " set endTime = from_unixtime( " + (__packet.time+FLOWTIMEOUT) + ")" +
                    "where ipSrc =  " + __packet.ipSrc +
                    " and ipDst =  " + __packet.ipDst +
                    " and portSrc =  " + __packet.portSrc +
                    " and portDst =  " + __packet.portDst +
                    " and endTime >= from_unixtime(" + __packet.time + ")" +
                    " and startTime <= from_unixtime(" +__packet.time + ");";

            sqlConnect.updateQuery(query);
        }
    }

    public Packet Get_packet_NA() throws SQLException{
        Packet _packet;
        ResultSet _rs;
        _rs = chunk.next();
        if (_rs == null){
            return null;
        }
        _packet = new Packet(_rs.getInt("ipSrc"), _rs.getInt("ipDst"),
                _rs.getInt("portSrc"), _rs.getInt("portDst"),
                _rs.getInt("time"), 1);
        return _packet;
    }

    public void Close() throws SQLException {
           sqlConnect.close();
    }

}
