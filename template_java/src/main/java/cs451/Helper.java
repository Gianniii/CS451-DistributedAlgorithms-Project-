package cs451;

import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.sql.Timestamp;
import java.net.InetAddress;
import java.io.IOException;

//rawData format: [senderId + "_" + proc_id + "." + seq_num + ":" + timestamp + "," + msg] // 
//The messageUID has format: [proc_id + "." + seq_num + ":" + timestamp]
public final class Helper {
    
    public static String getMsgUid(String rawData){
        int index2 = rawData.indexOf(",");
        int index1 = rawData.indexOf("_");
        return rawData.substring(index1+1, index2);
    }

    //seperate unique message uid and msg msg_uid = [proc_id + "." + sec_num + ":" + timestamp]
    public static String getMsg(String rawData){
        int index = rawData.indexOf(",");
        return rawData.substring(index+1);
    }

    public static String getSenderId(String rawData) {
        int index = rawData.indexOf("_");
        return rawData.substring(0, index);
    }

    public static String removeSenderId(String rawData) {
        return "_" + appendMsg(getMsgUid(rawData), getMsg(rawData));
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

    public static String extendWithSenderId(int senderId,String data){
        return String.valueOf(senderId) + "_" + data;
    }

    public static String addSenderIdAndMsgUidToMsg(int senderId, String msgUid, String data) {
        return extendWithSenderId(senderId, appendMsg(msgUid, data));
    }
}
