/**
 * Created by Pedrum on 12/14/2016.
 */
public class Packet {
    public int ID;
    public int ipSrc;
    public int ipDst;
    public int portSrc;
    public int portDst;
    public double time;
    public Packet(int __ipSrc, int __ipDst, int __portSrc, int __portDst, double __time, int __ID){
        ipSrc = __ipSrc;
        ipDst = __ipDst;
        portSrc = __portSrc;
        portDst = __portDst;
        time = __time;
        ID = __ID;
    }
}
