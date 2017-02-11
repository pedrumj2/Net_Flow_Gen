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
        long startTime =0;
        long endTime;
        long durationInsertFlow = 0;
        System.out.println(x.toString());
        j =0;
            try{


             //    startTime = System.nanoTime();
                _packet = mySqlCon.Get_packet_NA();
              //   endTime  = System.nanoTime();
               //  durationNA += (endTime - startTime)/1000000000;

                while (_packet != null){
                   // startTime = System.nanoTime();
                    mySqlCon.Insert_Flow(_packet);
                   // endTime  = System.nanoTime();
                    //durationInsertFlow += (endTime - startTime)/1000000000;






                    //startTime = System.nanoTime();
                    _packet = mySqlCon.Get_packet_NA();
                    //endTime  = System.nanoTime();
                    //durationNA += (endTime - startTime)/1000000000;

                j++;
                   // System.out.println(j);
                    if (j % 10000 ==0){
                        System.out.println(j);
                        endTime  = System.nanoTime();
                        System.out.println((endTime - startTime)/1000000000);
                        startTime =endTime;
                    }

                }
            }
            catch(java.sql.SQLException ex){
                System.out.println(ex);

            }

      System.out.println(x.toString());


    }
}
