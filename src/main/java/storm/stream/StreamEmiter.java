package storm.stream;


import java.io.IOException;
import java.io.Serializable;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;


public class StreamEmiter implements Serializable {

    /**
     *
     */
    private static final long serialVersionUID = 4262369370788016342L;
    ;

    private int port = -1;
    String ipM = null;
    DatagramSocket udpSocket = null;
    InetAddress mcIPAddress = null;
    DatagramPacket packet = null;


    public StreamEmiter(int port, String ip) {
        this.port = port;
        this.ipM = ip;

    }


    public void send(String row) {

        try {


            udpSocket = new DatagramSocket();
            mcIPAddress = InetAddress.getByName(this.ipM);
            packet = null;

            byte[] msg = null;
            msg = row.toString().getBytes();
            try {
                packet = new DatagramPacket(msg, msg.length);
                packet.setAddress(mcIPAddress);
                packet.setPort(this.port);
                udpSocket.send(packet);
                System.out.println(row.toString() + " : sent at " + System.currentTimeMillis() + "!");
            } catch (IOException e) {
                e.printStackTrace();
            }

            try {
                Thread.sleep(1500);
            } catch (InterruptedException e1) {
                e1.printStackTrace();
            }

        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            udpSocket.close();
        }

    }


    @Override
    public String toString() {
        return "StreamEmiter[port=" + this.port + ", ipMultiC=" + this.ipM + "]";
    }


}
