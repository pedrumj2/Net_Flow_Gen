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
    private int getSize = 100000;
    private SqlConnect sqlConnect;
    private Chunk chunk;
    private String table;
    private int flowID;


    public QueriesFlows(dbParams __dbparams, String __packet_table) throws SQLException, ClassNotFoundException {
        flowRow =2;
        sqlConnect = new SqlConnect(__dbparams);
        database = __dbparams.dbName;
        table = __packet_table;
        chunk =new Chunk(__dbparams, "SELECT frame_number, frame_time_epoch, least(ip_src, ip_dst) as ip1, greatest(ip_src, ip_dst) as ip2, " +
                " least(port_src, port_dst) as port1, greatest(port_src, port_dst) as port2 from " + database + "." +
                __packet_table +"  order by frame_time_epoch asc ", getSize);
        insert_table();
        flowID = 1;

    }

    private void insert_table(){
        sqlConnect.updateQuery("CREATE TABLE if not exists paper3."+table+"_flows (\n" +
                                "  idflows INT NOT NULL,\n" +
                                "  port_src INT NULL,\n" +
                                "  port_dst INT NULL,\n" +
                                "  ip_src INT NULL,\n" +
                                "  ip_dst INT NULL,\n" +
                                "  start_time DOUBLE NULL,\n" +
                                "  end_time DOUBLE NULL,\n" +
                                "  PRIMARY KEY (idflows),\n" +
                                "  INDEX T (port_src ASC, port_dst ASC, ip_src ASC, ip_dst ASC, end_time ASC, start_time ASC));");

        sqlConnect.updateQuery("insert into "+table+"_flows(idflows, port_src, port_dst, ip_src, ip_dst, end_time, start_time)\n" +
                "values  (1, -1, -1, 1, 1, -1, -1)");
    }

    public void update_row(){
        flowRow ++;
    }

    public void Insert_Flow(Packet __packet) throws SQLException{
        int _cur_flow_id;
        String query = "select * from "+database+"." + table + "_flows " +
                " where ip_src =  " + __packet.ipSrc +
                " and ip_dst =  " + __packet.ipDst +
                " and port_src =  " + __packet.portSrc +
                " and port_dst =  " + __packet.portDst +
                " and end_time >= (" + __packet.time + ")" +
                " and start_time <= (" +__packet.time + ");";

        ResultSet rs = sqlConnect.executeQuery(query);
        if (!rs.next()){
            update_row();
            query = "INSERT INTO "+database+"."+table+"_flows " +
                    "(idflows, ip_src, ip_dst, port_src, port_dst, start_time, end_time) " +
                    " VALUES (" + Integer.toString(flowRow) +", "
                    +(__packet.ipSrc) + ", " +
                    (__packet.ipDst) + ", " +
                    (__packet.portSrc) + ", " +
                    (__packet.portDst) + ", " +
                    " (" + (__packet.time) +"), " +
                    " ( " + (__packet.time+FLOWTIMEOUT) + "));";

            sqlConnect.updateQuery(query);
            flowID ++;
            _cur_flow_id = flowID;
        }
        else {
            query = "select * from "+database+"."+table+"_flows " +
                    "where ip_src =  " + __packet.ipSrc +
                    " and ip_dst =  " + __packet.ipDst +
                    " and port_src =  " + __packet.portSrc +
                    " and port_dst =  " + __packet.portDst +
                    " and end_time >= (" + __packet.time + ")" +
                    " and start_time <= (" +__packet.time + ");";

            ResultSet rs2 = sqlConnect.executeQuery(query);
            rs2.next();
            _cur_flow_id = rs2.getInt("idflows");

            query = "update "+database+"."+table+"_flows " +
                    " set end_time = ( " + (__packet.time+FLOWTIMEOUT) + ")" +
                    "where ip_src =  " + __packet.ipSrc +
                    " and ip_dst =  " + __packet.ipDst +
                    " and port_src =  " + __packet.portSrc +
                    " and port_dst =  " + __packet.portDst +
                    " and end_time >= (" + __packet.time + ")" +
                    " and start_time <= (" +__packet.time + ");";

            sqlConnect.updateQuery(query);


        }
        update_flow_column(__packet, _cur_flow_id);

    }

    //updates the flow column in the packet table
    private void update_flow_column(Packet __packet, int __flow_id){
        String query = "update "+database+"."+table +
                " set flow = " + __flow_id +
               " where frame_number =  " + __packet.ID;

        sqlConnect.updateQuery(query);
    }
    public Packet Get_packet_NA() throws SQLException{
        Packet _packet;
        ResultSet _rs;
        _rs = chunk.next();
        if (_rs == null){
            return null;
        }
        _packet = new Packet(_rs.getInt("ip1"), _rs.getInt("ip2"),
                _rs.getInt("port1"), _rs.getInt("port2"),
                _rs.getDouble("frame_time_epoch"), _rs.getInt("frame_number"));
        return _packet;
    }

    public void Close() throws SQLException {
           sqlConnect.close();
    }

}
