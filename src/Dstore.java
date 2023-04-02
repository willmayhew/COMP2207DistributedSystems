import java.io.*;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.Path;

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
                    Socket client = ss.accept();

                    new Thread(() -> {
                        try{
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
                                        storeFile(client.getInputStream());
                                        client.setSoTimeout(timeout);
                                        sendStoreACK(cOut, fileName);
                                        break;
                                    case "LOAD_DATA":
                                        loadFile(client.getOutputStream(), message[1]);
                                }
                            }
                        } catch (Exception e){}
                    }).start();
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
        System.out.println("Command sent: ACK");
    }

    /**
     * Send acknowledgment message to controller
     * @param out Writer communicating with controller
     * @param fileName File name
     */
    private static void sendStoreACK(PrintWriter out, String fileName){
        out.println("STORE_ACK " + fileName);
        System.out.println("Command sent: STORE_ACK");
    }

    /**
     * Receives the file from the client
     * @param in InputStream from client
     */
    private static void storeFile(InputStream in){
        try{
            byte[] buf = new byte[fileSize];
            in.readNBytes(buf,0,buf.length);
            FileOutputStream fileOutputStream = new FileOutputStream(filePath + "/" + fileName);
            fileOutputStream.write(buf,0,buf.length);
            fileOutputStream.close();
        } catch (Exception e){System.out.println("Error " + e);}
    }

    private static void loadFile(OutputStream out, String fileName){
        try{
            byte[] buf = Files.readAllBytes(Path.of(filePath + "/" + fileName));
            out.write(buf, 0, buf.length);
        } catch (Exception e){System.out.println("Error " + e);}
    }

}
