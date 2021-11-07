package cs451.Links;

import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.sql.Timestamp;
import java.net.InetAddress;
import java.io.IOException;

//rawData format: [proc_id + "." + seq_num + ":" + timestamp + "," + msg]

public final class Helper {
    
    public static String getMsgUid(String rawData){
        int iend = rawData.indexOf(",");
        return rawData.substring(0, iend);
    }

    //seperate unique message uid and msg
    public static String getMsg(String rawData){
        int index = rawData.indexOf(",");
        return rawData.substring(index+1);
    }

    public static String getProcIdFromMessageUid(String msg_uid) {
        int index = msg_uid.indexOf(".");
        return msg_uid.substring(0, index); 
    }

    public static String getSeqNumFromMessageUid(String msg_uid) {
        int index2 = msg_uid.indexOf(":");
        int index1 = msg_uid.indexOf(".");
        return msg_uid.substring(index1+1, index2); 
    }
    
    public static String createUniqueMsgUid(String proc_uid, String seq_num){
        return proc_uid + "." + seq_num + ":"+ System.currentTimeMillis();
    }

    public static String appendMsg(String data,String msg){
        return data + "," + msg;
    }
}
