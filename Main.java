import org.omg.CORBA.BAD_TYPECODE;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Date;

/*reads data from the fileName file and exports them to the mySQL database. It also generates flows*/
public class Main {
    private static MySqlCon mySqlCon;
    public static void main(String[] args) throws IOException, SQLException, ClassNotFoundException{
        	mySqlCon = new MySqlCon("D11", "fafdRE$3", "192.168.20.12");
        	ReadData();
    }
    private static void ReadData() throws IOException{
        //make sure j is in sync with the counter for packets
        int j;
        Packet _packet;
        Packet _tuple;
        int prevID;
        Date x = new Date();
        long durationNA=0;
        long durationMax=0;
        long durationUpdate =0;
        long durationUpdatetest =0;
        long startTime;
        long endTime;
        long durationInsertFlow = 0;
        System.out.println(x.toString());
        j =0;
           try{


             /*   startTime = System.nanoTime();
                _packet = mySqlCon.Get_packet_NA();
                 endTime  = System.nanoTime();
                 durationNA += (endTime - startTime)/1000000000;
*/
               _tuple = mySqlCon.getNextTuple();
                while (_tuple != null){
                    startTime = System.nanoTime();
                    _packet = mySqlCon.Get_packet_NA(_tuple, 0);
                    endTime  = System.nanoTime();
                    durationNA += (endTime - startTime)/1000000000;

                    startTime = System.nanoTime();
                    mySqlCon.Insert_Flow(_packet);
                    endTime  = System.nanoTime();
                    durationInsertFlow += (endTime - startTime)/1000000000;


                    startTime = System.nanoTime();
                    mySqlCon.update_packets(_packet);
                    endTime  = System.nanoTime();
                    durationUpdate += (endTime - startTime)/1000000000;


                    startTime = System.nanoTime();
                    _packet = mySqlCon.get_packet_max(_packet,  _tuple);
                    endTime  = System.nanoTime();
                    durationMax += (endTime - startTime)/1000000000;


                    prevID = _packet.ID;
                    loopThroughCurrentFlow(_packet, _tuple, prevID);
                    mySqlCon.update_row();
                    _packet = mySqlCon.Get_packet_NA(_tuple,_packet.time);
                    while (_packet != null){

                        mySqlCon.Insert_Flow(_packet);
                        mySqlCon.update_packets(_packet);
                        _packet = mySqlCon.get_packet_max(_packet,  _tuple);
                        prevID = _packet.ID;
                        loopThroughCurrentFlow(_packet, _tuple, prevID);
                        mySqlCon.update_row();
                        _packet = mySqlCon.Get_packet_NA(_tuple,_packet.time);
                    }





                    startTime = System.nanoTime();
                    _tuple = mySqlCon.getNextTuple();
                    endTime  = System.nanoTime();
                    durationNA += (endTime - startTime)/1000000000;
                    System.out.println(j);
                }
               /*


                j++;
                    if (j % 1000 ==0){
                        System.out.println(j);
                    }

                }
                 mySqlCon.Get_packetS(j);*/

               System.out.println(j);
               if (j % 100 ==0){
                   System.out.println(j);
               }

               j++;


            }
            catch(java.sql.SQLException ex){
                System.out.println(ex);

            }



      System.out.println(j);


    }

    static void  loopThroughCurrentFlow(Packet __maxPacket, Packet __currentTuple,int __prevID) throws SQLException{
        while (__maxPacket != null){

            mySqlCon.update_packets(__maxPacket);
            __maxPacket = mySqlCon.get_packet_max(__maxPacket, __currentTuple);


            if (__maxPacket.ID   == __prevID){
                __maxPacket = null;
            }
            else{
                __prevID = __maxPacket.ID;
            }
        }

    }
}
