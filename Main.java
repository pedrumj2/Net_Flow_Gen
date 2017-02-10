import java.io.IOException;
import java.sql.SQLException;
import java.util.Date;

/*reads data from the fileName file and exports them to the mySQL database. It also generates flows*/
public class Main {
    private static MySqlCon mySqlCon;
    public static void main(String[] args) throws IOException, SQLException, ClassNotFoundException{
        	mySqlCon = new MySqlCon("D11", "fafdRE$3", "127.0.0.1");
        	ReadData();
    }
    private static void ReadData() throws IOException{
        //make sure j is in sync with the counter for packets
        int j;
        Packet _packet;
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
           try{ /*


                startTime = System.nanoTime();
                _packet = mySqlCon.Get_packet_NA();
                 endTime  = System.nanoTime();
                 durationNA += (endTime - startTime)/1000000000;

                while (_packet != null){
                    startTime = System.nanoTime();
                    mySqlCon.Insert_Flow(_packet);
                    endTime  = System.nanoTime();
                    durationInsertFlow += (endTime - startTime)/1000000000;


                    startTime = System.nanoTime();
                    mySqlCon.test_update_packets(_packet);
                    endTime  = System.nanoTime();
                    durationUpdatetest += (endTime - startTime)/1000000000;


                    startTime = System.nanoTime();
                    mySqlCon.update_packets(_packet);
                    endTime  = System.nanoTime();
                    durationUpdate += (endTime - startTime)/1000000000;




                    startTime = System.nanoTime();
                    _packet = mySqlCon.get_packet_max();
                    endTime  = System.nanoTime();
                    durationMax += (endTime - startTime)/1000000000;

                    prevID = _packet.ID;
                    while (_packet != null){


                        startTime = System.nanoTime();
                        mySqlCon.test_update_packets(_packet);
                        endTime  = System.nanoTime();
                        durationUpdatetest += (endTime - startTime)/1000000000;


                        startTime = System.nanoTime();
                        mySqlCon.update_packets(_packet);
                        endTime  = System.nanoTime();
                        durationUpdate += (endTime - startTime)/1000000000;



                        startTime = System.nanoTime();
                        _packet = mySqlCon.get_packet_max();
                        endTime  = System.nanoTime();
                        durationMax += (endTime - startTime)/1000000000;

                        if (_packet.ID   == prevID){
                            _packet = null;
                        }
                        else{
                            prevID = _packet.ID;
                        }
                    }


                    mySqlCon.update_row();

                    startTime = System.nanoTime();
                    _packet = mySqlCon.Get_packet_NA();
                    endTime  = System.nanoTime();
                    durationNA += (endTime - startTime)/1000000000;

                j++;
                    if (j % 1000 ==0){
                        System.out.println(j);
                    }

                }
                 mySqlCon.Get_packetS(j);*/
           while(true){
               mySqlCon.Get_packetS(j);
               if (j % 100 ==0){
                   System.out.println(j);
               }

               j++;

           }
            }
            catch(java.sql.SQLException ex){
                System.out.println(ex);

            }



      System.out.println(j);


    }
}
