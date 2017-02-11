/**
 * Created by Pedrum on 12/11/2016.
 */

import java.sql.*;
import java.util.ArrayList;

//handles connections to our database
public class MySqlCon {
    private int COUNTER = 10000;
    private Statement stmt;
    private Statement stmtFlow;
    private Connection connection;
    int flowRow;
    int FLOWTIMEOUT = 60;
    String database;
    ResultSet rsNA;
    int millionth;
    int getSize = 10000;

    private String insFlow;
   private java.util.Date _start;
   public double laps;

    public MySqlCon(String __database, String __password, String __IP) throws SQLException, ClassNotFoundException {
        flowRow =2;
        database = __database;
        millionth =1;
        try{
            Class.forName("com.mysql.jdbc.Driver");
            connection = DriverManager.getConnection("jdbc:mysql://"+__IP+"/" + __database, "root", __password);
            stmt = connection.createStatement();
            stmtFlow= connection.createStatement();
            String query = "SELECT time, least(ipSrc, ipDst) as ipSrc, greatest(ipSrc, ipDst) as ipDst, " +
                    " least(portSrc, portDst) as portSrc, greatest(portSrc, portDst) as portDst from "+database+".packets  order by time asc limit "+millionth+", " + getSize + ";";
            rsNA = stmt.executeQuery(query);
            millionth++;
        }
        catch(java.sql.SQLException ex) {
            System.out.println("Database not connected!");
            throw ex;
        }
        catch(ClassNotFoundException ex){
            System.out.println(ex);
            throw ex;
        }
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

        ResultSet rs = stmtFlow.executeQuery(query);
        System.out.println("Check If Flow exists");
        if (rs.next()==false){
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

            stmtFlow.executeUpdate(query);
            System.out.println("Inserted flow");
        }
        else {
            update_row();
            query = "update "+database+".flows " +
                    " set endTime = from_unixtime( " + (__packet.time+FLOWTIMEOUT) + ")" +
                    "where ipSrc =  " + __packet.ipSrc +
                    " and ipDst =  " + __packet.ipDst +
                    " and portSrc =  " + __packet.portSrc +
                    " and portDst =  " + __packet.portDst +
                    " and endTime >= from_unixtime(" + __packet.time + ")" +
                    " and startTime <= from_unixtime(" +__packet.time + ");";

            stmtFlow.executeUpdate(query);
            System.out.println("updated flow");
        }

    }



    

    public Packet Get_packet_NA() throws SQLException{
        Packet _packet;


        if (rsNA.next()==false){
            String query = "SELECT time, least(ipSrc, ipDst) as ipSrc, greatest(ipSrc, ipDst) as ipDst, " +
            " least(portSrc, portDst) as portSrc, greatest(portSrc, portDst) as portDst from "+database+".packets  order by time asc limit " + millionth + ", " + getSize + ";";
            rsNA = stmt.executeQuery(query);
            millionth++;
            System.out.println("added new mill");
            if (rsNA.next()==false){
                return null;
            }
        }




            _packet = new Packet(rsNA.getInt("ipSrc"), rsNA.getInt("ipDst"),
                    rsNA.getInt("portSrc"), rsNA.getInt("portDst"),
                    rsNA.getInt("time"), 1);
            System.out.println("returned new NA");
            return _packet;


    }



    public void Close() throws SQLException {

            stmt.close();
            connection.close();


    }

}
