import java.io.*;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;

public class Dstore {

    private static int port;
    private static int cport;
    private static int timeout;
    private static String filePath;
    private static String fileName;
    private static int fileSize;

    public static void main(String[] args){
//        port = Integer.parseInt(args[0]);
//        cport = Integer.parseInt(args[1]);
//        timeout = Integer.parseInt(args[2]);
//        filePath = args[3];

        port = 10001;
        cport = 12345;
        timeout = 5000;
        filePath = "bruh";

        try{
            ServerSocket ss = new ServerSocket(port);
            System.out.println("DStore listening on port " + port);
            try{
                InetAddress address = InetAddress.getLocalHost();
                Socket controller = new Socket(address, cport);
                PrintWriter cOut = new PrintWriter(controller.getOutputStream(), true);

                cOut.println("JOIN " + port);

                while(true){
                    try{
                        Socket client = ss.accept();
                        BufferedReader in = new BufferedReader(new InputStreamReader(client.getInputStream()));
                        PrintWriter out = new PrintWriter(client.getOutputStream(), true);
                        String line;

                        while((line = in.readLine()) != null){
                            String[] message = line.split(" ");
                            System.out.println("Received Command: " + message[0]);

                            switch (message[0]){
                                case "STORE":
                                    fileName = message[1];
                                    fileSize = Integer.parseInt(message[2]);
                                    sendACK(out);
                                    storeFile(client.getInputStream(), fileSize);
                                    sendStoreACK(cOut, fileName);
                            }
                        }

                    } catch(Exception e){System.out.println("Error " + e);}
                }

            } catch (Exception e){System.out.println("Error " + e);}
        }catch (Exception e){System.out.println("Error " + e);}

    }

    /**
     * Send acknowledgment message to client
     * @param out Writer communicating with client
     */
    private static void sendACK(PrintWriter out){
        out.println("ACK");
    }

    /**
     * Send acknowledgment message to controller
     * @param out Writer communicating with controller
     * @param fileName File name
     */
    private static void sendStoreACK(PrintWriter out, String fileName){
        out.println("STORE_ACK " + fileName);
    }

    /**
     * Receives the file from the client
     * @param in InputStream from client
     * @param fileSize Size of file
     */
    private static void storeFile(InputStream in, int fileSize){
        try{
            byte[] buf = new byte[fileSize];
            in.readNBytes(buf,0,buf.length);
            FileOutputStream fileOutputStream = new FileOutputStream(fileName);
            fileOutputStream.write(buf,0,buf.length);
            fileOutputStream.close();
        } catch (Exception e){System.out.println("Error " + e);}

    }

}
