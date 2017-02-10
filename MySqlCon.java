/**
 * Created by Pedrum on 12/11/2016.
 */

import java.sql.*;
import java.util.ArrayList;

//handles connections to our database
public class MySqlCon {
    private int COUNTER = 10000;
    private Statement stmt;
    private Connection connection;
    int flowRow;
    int FLOWTIMEOUT = 60;
    String database;


    private String insFlow;
   private java.util.Date _start;
   public double laps;

    public MySqlCon(String __database, String __password, String __IP) throws SQLException, ClassNotFoundException {
        flowRow =2;
        database = __database;
        try{
            Class.forName("com.mysql.jdbc.Driver");
            connection = DriverManager.getConnection("jdbc:mysql://"+__IP+"/" + __database, "root", __password);
            stmt = connection.createStatement();
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
    public Packet get_packet_max() throws  SQLException{
        Packet _packet;

        String query = "SELECT idPackets,  MAX(time) as time, ipSrc, ipDst, portSrc, " +
                "portDst from "+ database+".packets  where flow = "  + flowRow + " group by idPackets order by time desc limit 1; ";
        ResultSet rs = stmt.executeQuery(query);
        if (rs.next()){
            _packet = new Packet(rs.getInt("ipSrc"), rs.getInt("ipDst"),
                    rs.getInt("portSrc"), rs.getInt("portDst"),
                    rs.getInt("time"), rs.getInt("idPackets"));

            return _packet;
         
        }
        else{
        

            return null;
            
        }
    }
    public void Insert_Flow(Packet __packet) throws SQLException{

        Packet _packet;
        String query = "INSERT INTO "+database+".flows " +
                "(idFlows, ipSrc, ipDst, portSrc, portDst) " +
                " VALUES (" + Integer.toString(flowRow) +", "
                                +(__packet.ipSrc) +", " +
                                (__packet.ipDst) +", " +
                                (__packet.portSrc) +", " +
                                (__packet.portDst) +");";

        stmt.executeUpdate(query);




    }

    public void test_update_packets(Packet __packet) throws SQLException{
        Packet _packet;


        String query = "select * from "+database+".packets \n" +
                "     Where time < " + (__packet.time + FLOWTIMEOUT) + "\n" +
                "     and ipSrc = " + __packet.ipSrc + "\n" +
                "     and ipDst = " + __packet.ipDst + "\n" +
                "     and portSrc = " + __packet.portSrc + "\n" +
                "     and portDst = " + __packet.portDst +  ";";
        //  System.out.println(query);
        stmt.execute(query);

        query = "select * from "+database+".packets \n" +
                "     Where time < " + (__packet.time + FLOWTIMEOUT) + "\n" +
                "     and ipSrc = " + __packet.ipDst + "\n" +
                "     and ipDst = " + __packet.ipSrc + "\n" +
                "     and portSrc = " + __packet.portDst + "\n" +
                "     and portDst = " + __packet.portSrc +  ";";
        //  System.out.println(query);
        stmt.execute(query);

    }

    public void update_packets(Packet __packet) throws SQLException{
        Packet _packet;


                String query = "update "+database+".packets \n" +
                 "       set packets.Flow = " + Integer.toString(flowRow) + "\n" +
                "     Where time < " + (__packet.time + FLOWTIMEOUT) + "\n" +
                "     and ipSrc = " + __packet.ipSrc + "\n" +
                "     and ipDst = " + __packet.ipDst + "\n" +
                "     and portSrc = " + __packet.portSrc + "\n" +
                "     and portDst = " + __packet.portDst +  ";";
         //  System.out.println(query);
        stmt.executeUpdate(query);

         query = "update "+database+".packets \n" +
                "       set packets.Flow = " + Integer.toString(flowRow) + "\n" +
                "     Where time < " + (__packet.time + FLOWTIMEOUT) + "\n" +
                "     and ipSrc = " + __packet.ipDst + "\n" +
                "     and ipDst = " + __packet.ipSrc + "\n" +
                "     and portSrc = " + __packet.portDst + "\n" +
                "     and portDst = " + __packet.portSrc +  ";";
        //  System.out.println(query);
        stmt.executeUpdate(query);

    }

    public Packet Get_packetS(int i) throws SQLException{
        Packet _packet;

        String query = "SELECT * from "+database+".packets  order by time asc limit 1, " + i + ";";
        ResultSet rs = stmt.executeQuery(query);
        if (rs.next()){
            _packet = new Packet(rs.getInt("ipSrc"), rs.getInt("ipDst"),
                    rs.getInt("portSrc"), rs.getInt("portDst"),
                    rs.getInt("time"), 1);

            return _packet;
        }

        return null;

    }


    public Packet Get_packet_NA() throws SQLException{
        Packet _packet;

        String query = "SELECT time, ipSrc, ipDst, portSrc, portDst from "+database+".packets  where flow = 1  order by time asc limit 1;";
        ResultSet rs = stmt.executeQuery(query);
        if (rs.next()){
            _packet = new Packet(rs.getInt("ipSrc"), rs.getInt("ipDst"),
                    rs.getInt("portSrc"), rs.getInt("portDst"),
                    rs.getInt("time"), 1);

            return _packet;
        }

        return null;

    }



    public void Close() throws SQLException {

            stmt.close();
            connection.close();


    }

}
