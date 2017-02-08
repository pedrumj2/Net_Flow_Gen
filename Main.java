import java.io.IOException;
import java.sql.SQLException;
import java.util.Date;

/*reads data from the fileName file and exports them to the mySQL database. It also generates flows*/
public class Main {
    private static MySqlCon mySqlCon;
    public static void main(String[] args) throws IOException, SQLException, ClassNotFoundException{
        	mySqlCon = new MySqlCon("paper2", "fafdRE$3", "141.117.233.232");
        	ReadData();
    }
    private static void ReadData() throws IOException{
        //make sure j is in sync with the counter for packets
        int j;
        Packet _packet;
        int prevID;
        Date x = new Date();
        System.out.println(x.toString());
        j =0;
            try{
                _packet = mySqlCon.Get_packet_NA();
                while (_packet != null){
                    mySqlCon.Insert_Flow(_packet);
                    mySqlCon.update_packets(_packet);
                    _packet = mySqlCon.get_packet_max();
                    prevID = _packet.ID;
                    while (_packet != null){
                        mySqlCon.update_packets(_packet);
                        _packet = mySqlCon.get_packet_max();
                        if (_packet.ID   == prevID){
                            _packet = null;
                        }
                        else{
                            prevID = _packet.ID;
                        }
                    }
                    mySqlCon.update_row();
                    _packet = mySqlCon.Get_packet_NA();
                j++;
                    if (j % 1000 ==0){
                        System.out.println(j);
                    }

                }
            }
            catch(java.sql.SQLException ex){
                System.out.println(ex);

            }
    x = new Date();
      System.out.println(x.toString());


    }
}
