import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Date;

/*reads data from the fileName file and exports them to the mySQL database. It also generates flows*/
public class Main {



    private static MySqlCon mySqlCon;



    public static void main(String[] args) throws IOException, SQLException, ClassNotFoundException{


        	mySqlCon = new MySqlCon();
        	ReadData();


    }


    private static void ReadData() throws IOException{
        //make sure j is in sync with the counter for packets
        int j;
        Packet _packet;
        int prevID;
        java.util.Date date1;
        java.util.Date date2;
        java.util.Date date_flow1;
        java.util.Date date1_flow2;
        int _InsertFlow;
        int _getMax;
        int _updatePackets;
        int _getNA;
        int _selectupdate;
     Date x = new Date();
      System.out.println(x.toString());
        j =0;

               _InsertFlow = 0;
                   _getMax =0;
                   _updatePackets =0;
                _getNA = 0;
                _selectupdate =0;

            try{
                _packet = mySqlCon.Get_packet_NA();
                _getNA +=   mySqlCon.laps;
                
                 date1 = new java.util.Date();
                while (_packet != null){
            
                    mySqlCon.Insert_Flow(_packet);
                     _InsertFlow +=   mySqlCon.laps;
                     
                     
                  //     mySqlCon.select_update_packets(_packet);
                       _selectupdate+=   mySqlCon.laps;
                       
                    mySqlCon.update_packets(_packet);
                    _updatePackets+=   mySqlCon.laps;
                    
                    _packet = mySqlCon.get_packet_max();
                    _getMax+=   mySqlCon.laps;
                    
                    prevID = _packet.ID;
                    while (_packet != null){
                  //      mySqlCon.select_update_packets(_packet);
                       _selectupdate+=   mySqlCon.laps;
                    
                        mySqlCon.update_packets(_packet);
                        _updatePackets+=   mySqlCon.laps;
                        
                        _packet = mySqlCon.get_packet_max();
                        _getMax+=   mySqlCon.laps;
                        if (_packet.ID   == prevID){

                            _packet = null;

                        }
                        else{
                            prevID = _packet.ID;
                        }
                    }
                    mySqlCon.update_row();
                    
                    _packet = mySqlCon.Get_packet_NA();
                    _getNA +=   mySqlCon.laps;
                    
                    
                    
                  if ((j % 50)==0) {
                  
                         System.out.println("Insert: ");
                      System.out.println(_InsertFlow);
                                   System.out.println("get max: ");
                      System.out.println(_getMax);
                               System.out.println("update packs: ");
                      System.out.println(_updatePackets);
                           System.out.println("get na: ");
                      System.out.println(_getNA);
                      
                                                 System.out.println("select update: ");
                      System.out.println(_selectupdate);
                      _InsertFlow = 0;
                     _getMax =0;
                     _updatePackets =0;
                     _getNA = 0;
                     _selectupdate =0;
                     
                     
                      System.out.println(j);
                      date2 = new java.util.Date();
                      System.out.println("Time diff: ");
                      System.out.println(date2.getTime()-date1.getTime());
                      System.out.println("\n");
      
                      date1 = date2;
                
                  }
                j++;

                }
            }
            catch(java.sql.SQLException ex){
                System.out.println(ex);

            }


            //checks if this packet is part of a current flow. If not the flow is created

            //   Flow.Exists(flows, _packet);
            //gets the flows data have ended

            // _closedFlows = Flow.Ended(flows, _packet.time);
             //add flows to database
       //      mySqlCon.Add_Flows(_closedFlows);

    

    x = new Date();
      System.out.println(x.toString());


    }
}
