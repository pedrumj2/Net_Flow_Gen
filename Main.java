import java.io.IOException;
import java.sql.SQLException;
import java.util.Date;
import MySqlJava.*;



public class Main {
    private static QueriesFlows queries;
    private static dbParams dbParams;
    public static void main(String[] args) throws IOException, SQLException, ClassNotFoundException{
            dbParams = new dbParams(args[0], args[1], args[2] );
            queries = new QueriesFlows(dbParams, args[3]);
            generateFlows();
            //postProcess();
    }

    private static void postProcess() throws SQLException{
        PostProcess _postProcess = new PostProcess(dbParams);
        _postProcess.run();
    }

    private static void generateFlows() throws IOException{
        //make sure j is in sync with the counter for packets
        int j;
        Packet _packet;
        Date x = new Date();

        long startTime =0;
        long endTime;
        System.out.println(x.toString());
        j =0;
            try{
                _packet = queries.Get_packet_NA();
                while (_packet != null){
                    queries.Insert_Flow(_packet);
                    _packet = queries.Get_packet_NA();
                    j++;
                    System.out.println(j);
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
