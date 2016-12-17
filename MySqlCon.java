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

    private String insFlow;
   private java.util.Date _start;
   public double laps;


  private void Start(){
   _start = new java.util.Date();
  }
  
    private void End(){
    java.util.Date date2;
    date2= new java.util.Date();
   laps = date2.getTime()-_start.getTime();
   
  }
  
  

    public MySqlCon() throws SQLException, ClassNotFoundException {
        flowRow =2;
        try{
            Class.forName("com.mysql.jdbc.Driver");
            connection = DriverManager.getConnection("jdbc:mysql://127.0.0.1:3306/mydb", "root", "fafdRE$3");
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
        Start();
        String query = "SELECT idPackets,  MAX(time) as time, ipSrc, ipDst, portSrc, " +
                "portDst from mydb.packets  where flow = "  + flowRow + " group by idPackets order by time desc; ";
        ResultSet rs = stmt.executeQuery(query);
        if (rs.next()){
            _packet = new Packet(rs.getInt("ipSrc"), rs.getInt("ipDst"),
                    rs.getInt("portSrc"), rs.getInt("portDst"),
                    rs.getInt("time"), rs.getInt("idPackets"));
                        End();
            return _packet;
         
        }
        else{
        
         End();
            return null;
            
        }
    }
    public void Insert_Flow(Packet __packet) throws SQLException{
    Start();
        Packet _packet;
        String query = "INSERT INTO mydb.flows " +
                "(idFlows, ipSrc, ipDst, portSrc, portDst) " +
                " VALUES (" + Integer.toString(flowRow) +", "
                                +(__packet.ipSrc) +", " +
                                (__packet.ipDst) +", " +
                                (__packet.portSrc) +", " +
                                (__packet.portDst) +");";

        stmt.executeUpdate(query);
        End();



    }

    public void update_packets(Packet __packet) throws SQLException{
        Packet _packet;
        Start();
      /*  String query = "update mydb.packets set Flow = " + Integer.toString(flowRow)  +
                " Where ((ipSrc = " + __packet.ipSrc +
                " and ipDst = " + __packet.ipDst +
                " and portSrc = " + __packet.portSrc +
                " and portDst = " + __packet.portDst + ") or " +
                " (ipSrc = " + __packet.ipDst +
                " and ipDst = " + __packet.ipSrc +
                " and portSrc = " + __packet.portDst +
                " and portDst = " + __packet.portSrc + ")) " +
                " and time < " + (__packet.time + FLOWTIMEOUT) + ";";*/
                
                String query = "update mydb.packets \n" +
                " INNER JOIN  \n" +
                "  (  \n" +
                "     Select packets.idPackets, packets.Flow \n" +
                "     FROM mydb.packets \n" +
                "     Where time < " + (__packet.time + FLOWTIMEOUT) + "\n" +
                "     and ((ipSrc = " + __packet.ipSrc + "\n" +
                "     and ipDst = " + __packet.ipDst + "\n" +
                "     and portSrc = " + __packet.portSrc + "\n" +
                "     and portDst = " + __packet.portDst + ") or " + "\n" +
                "     (ipSrc = " + __packet.ipDst + "\n" +
                "     and ipDst = " + __packet.ipSrc + "\n" +
                "     and portSrc = " + __packet.portDst + "\n" +
                "     and portDst = " + __packet.portSrc + "))) as t " + "\n" +
                " on packets.idPackets = t.idPackets " + "\n" +
                " set packets.Flow = " + Integer.toString(flowRow)  + ";";
         //  System.out.println(query);
        stmt.executeUpdate(query);
End();



    }
    
    
     public void select_update_packets(Packet __packet) throws SQLException{
        Packet _packet;
        Start();
      /*  String query = "update mydb.packets set Flow = " + Integer.toString(flowRow)  +
                " Where ((ipSrc = " + __packet.ipSrc +
                " and ipDst = " + __packet.ipDst +
                " and portSrc = " + __packet.portSrc +
                " and portDst = " + __packet.portDst + ") or " +
                " (ipSrc = " + __packet.ipDst +
                " and ipDst = " + __packet.ipSrc +
                " and portSrc = " + __packet.portDst +
                " and portDst = " + __packet.portSrc + ")) " +
                " and time < " + (__packet.time + FLOWTIMEOUT) + ";";*/
                
                String query = " Select idPackets, Flow \n" +
                "     FROM \n " +
                "     ( \n" +
                "       select *  \n" +
                "        from mydb.packets  \n" +
                "        where time < " + (__packet.time + FLOWTIMEOUT) + "\n" +
                "     ) as t \n" +
                "     where ((ipSrc = " + __packet.ipSrc + "\n" +
                "     and ipDst = " + __packet.ipDst + "\n" +
                "     and portSrc = " + __packet.portSrc + "\n" +
                "     and portDst = " + __packet.portDst + ") or " + "\n" +
                "     (ipSrc = " + __packet.ipDst + "\n" +
                "     and ipDst = " + __packet.ipSrc + "\n" +
                "     and portSrc = " + __packet.portDst + "\n" +
                "     and portDst = " + __packet.portSrc + "));";
                
                
         //  System.out.println(query);
        stmt.executeQuery(query);
End();


    }
    public Packet Get_packet_NA() throws SQLException{
        Packet _packet;
        Start();
        String query = "SELECT time, ipSrc, ipDst, portSrc, portDst from mydb.packets  where flow = 1  order by time asc limit 1;";
        ResultSet rs = stmt.executeQuery(query);
        if (rs.next()){
            _packet = new Packet(rs.getInt("ipSrc"), rs.getInt("ipDst"),
                    rs.getInt("portSrc"), rs.getInt("portDst"),
                    rs.getInt("time"), 1);
          End();
            return _packet;
        }
  End();
        return null;

    }



    public void Close() throws SQLException {

            stmt.close();
            connection.close();


    }

}
