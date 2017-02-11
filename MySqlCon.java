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
    int _tuple;


    private String insFlow;
   private java.util.Date _start;
   public double laps;

    public MySqlCon(String __database, String __password, String __IP) throws SQLException, ClassNotFoundException {
        flowRow =2;
        database = __database;
        _tuple = 1;
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

    public Packet getNextTuple() throws SQLException{
        Packet _packet;

        String query = "SELECT * from "+database+".tuples order by idTuples asc limit 1, " + _tuple + ";";
        _tuple++;
        ResultSet rs = stmt.executeQuery(query);
        if (rs.next()){
            _packet = new Packet(rs.getInt("ipSrc"), rs.getInt("ipDst"),
                    rs.getInt("portSrc"), rs.getInt("portDst"));

            return _packet;
        }

        return null;

    }

    public void update_row(){
        flowRow ++;
    }
    public Packet get_packet_max(Packet __packet, Packet __tuple) throws  SQLException{
        Packet _packet;

        String query = "SELECT idPackets,  time, least(ipSrc, ipDst) as IP1, greatest(ipSrc, ipDst) as IP2, " +
                "least(portSrc, portDst) as PORT1,  greatest(portSrc, portDst) as PORT2 from "+ database+".packets   " +
                " where least(ipSrc, ipDst) = " +__tuple.ipSrc +
                " and greatest(ipSrc, ipDst) = " +__tuple.ipDst + " and  least(portSrc, portDst) = " +__tuple.portSrc +
                " and greatest(portSrc, portDst) = " +__tuple.portDst + " and time < " + (__packet.time + FLOWTIMEOUT) + " order by time; ";
        ResultSet rs = stmt.executeQuery(query);

        if (rs.next()==false) {
            return null;
        }
        while (rs.next()){


        }

        rs.previous();
        _packet = new Packet(rs.getInt("IP1"), rs.getInt("IP2"),
            rs.getInt("PORT1"), rs.getInt("PORT2"),
            rs.getInt("time"), rs.getInt("idPackets"));


        return _packet;



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


    public void update_packets(Packet __packet) throws SQLException{
        Packet _packet;


                String query = "update "+database+".packets \n" +
                 "    set packets.Flow = " + Integer.toString(flowRow) + "\n" +
                "     Where time < " + (__packet.time + FLOWTIMEOUT) + "\n" +
                "     and   time > " + (__packet.time) +
                "     and least(ipSrc, ipDst) = " + __packet.ipSrc + "\n" +
                "     and greatest(ipSrc, ipDst) = " + __packet.ipDst + "\n" +
                "     and least(portSrc, portDst) = " + __packet.portSrc + "\n" +
                "     and greatest(portSrc, portDst) = " + __packet.portDst +  ";";
         //  System.out.println(query);
        stmt.executeUpdate(query);

       /*  query = "update "+database+".packets \n" +
                "       set packets.Flow = " + Integer.toString(flowRow) + "\n" +
                "     Where time < " + (__packet.time + FLOWTIMEOUT) + "\n" +
                "     and ipSrc = " + __packet.ipDst + "\n" +
                "     and ipDst = " + __packet.ipSrc + "\n" +
                "     and portSrc = " + __packet.portDst + "\n" +
                "     and portDst = " + __packet.portSrc +  ";";
        //  System.out.println(query);
        stmt.executeUpdate(query);
*/
    }




    public Packet Get_packet_NA(Packet __packet, double __time) throws SQLException{
        Packet _packet;

        String query = "SELECT time, least(ipSrc, ipDst) as IP1, greatest(ipSrc, ipDst) as IP2, " +
                "least(portSrc, portDst) as PORT1,  greatest(portSrc, portDst) as PORT2, flow from "+database+".packets " +
                "where least(ipSrc, ipDst) = " +__packet.ipSrc + " and greatest(ipSrc, ipDst) = " +__packet.ipDst +
                " and least(portSrc, portDst) = " +__packet.portSrc + " and greatest(portSrc, portDst) = " +__packet.portDst +
                " and time > " + __time + " order by time asc limit 1;";
        ResultSet rs = stmt.executeQuery(query);
        while (rs.next()){
            if (rs.getInt("flow") != 1){
                _packet = new Packet(rs.getInt("IP1"), rs.getInt("IP2"),
                        rs.getInt("PORT1"), rs.getInt("PORT2"),
                        rs.getInt("time"), 1);


                return _packet;
            }

        }

        return null;

    }



    public void Close() throws SQLException {

            stmt.close();
            connection.close();


    }

}
