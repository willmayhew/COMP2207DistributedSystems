import java.io.*;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.nio.file.Files;
import java.nio.file.Path;

public class Dstore {

    private static int port;
    private static int cport;
    private static int timeout;
    private static String folderPath;
    private static String fileName;
    private static int fileSize;
    private static Socket controller;
    private static PrintWriter controllerOut;

    public static void main(String[] args){
        port = Integer.parseInt(args[0]);
        cport = Integer.parseInt(args[1]);
        timeout = Integer.parseInt(args[2]);
        folderPath = args[3];

//        port = 10000;
//        cport = 12345;
//        timeout = 5000;
//        folderPath = "bruh";

        if(clearFolder(new File(folderPath))){
            System.out.println("Folder cleared");
        } else{
            System.out.println("Folder not cleared");
        }

        try{
            ServerSocket ss = new ServerSocket(port);
            System.out.println("DStore listening on port " + port);
            try{

                controller = new Socket(InetAddress.getLocalHost(), cport);
                controllerOut = new PrintWriter(controller.getOutputStream(), true);

                controllerOut.println(Protocol.JOIN_TOKEN + " " + port);
                System.out.println("Command sent: " + Protocol.JOIN_TOKEN + " " + port);

                while(true){
                    Socket client = ss.accept();

                    new Thread(() -> {
                        try{

                            BufferedReader in = new BufferedReader(new InputStreamReader(client.getInputStream()));
                            PrintWriter out = new PrintWriter(client.getOutputStream(), true);
                            String line;

                            synchronized (in){
                                while((line = in.readLine()) != null){
                                    String[] message = line.split(" ");
                                    System.out.println("Received Command: " + line);

                                    switch (message[0]){
                                        case Protocol.STORE_TOKEN:
                                            client.setSoTimeout(timeout);
                                            sendACK(out);
                                            storeFile(client.getInputStream(), message[1], Integer.parseInt(message[2]));
                                            sendStoreACK(controllerOut, message[1]);
                                            break;
                                        case Protocol.LOAD_DATA_TOKEN:
                                            loadFile(client.getOutputStream(), message[1]);
                                            break;
                                        case Protocol.REMOVE_TOKEN:
                                            removeFile(controllerOut, message[1]);
                                            break;
                                        case Protocol.LIST_TOKEN:
                                            listFiles(controllerOut);
                                            break;
                                        default:
                                            System.out.println("ERROR: Command not recognised -- " + line);
                                    }
                                }
                            }
                            client.close();
                        } catch (SocketTimeoutException e){System.out.println("Socket timed out");}
                        catch (Exception e){System.out.println("Error " + e);}
                    }).start();
                }

            } catch (Exception e){System.out.println("Error " + e);}
        }catch (Exception e){System.out.println("Error " + e);}

    }

    private static synchronized void sendCommand(PrintWriter out,String command) {
        out.println(command);
    }

    private static boolean clearFolder(File folder){
        if (folder.isDirectory()) {
            File[] files = folder.listFiles();
            if(files != null) {
                for (File file : files) {
                    clearFolder(file);
                    file.delete();
                }
            }
            return true;
        }
        return false;
    }

    /**
     * Send acknowledgment message to client
     * @param out Writer communicating with client
     */
    private static void sendACK(PrintWriter out){
//        out.println(Protocol.ACK_TOKEN);
        sendCommand(out,Protocol.ACK_TOKEN);
        System.out.println("Command sent: " + Protocol.ACK_TOKEN);
    }

    /**
     * Send acknowledgment message to controller
     * @param out Writer communicating with controller
     * @param fileName File name
     */
    private static void sendStoreACK(PrintWriter out, String fileName){
        String message = Protocol.STORE_ACK_TOKEN + " " + fileName;
//        out.println(message);
        sendCommand(out,message);
        System.out.println("Command sent: " + message);
    }

    /**
     * Receives the file from the client
     * @param in InputStream from client
     */
    private static void storeFile(InputStream in, String fileName, Integer fileSize){
        try{
            byte[] buf = new byte[fileSize];
            in.readNBytes(buf,0,buf.length);
            FileOutputStream fileOutputStream = new FileOutputStream(folderPath + "/" + fileName);
            fileOutputStream.write(buf,0,buf.length);
            fileOutputStream.close();
        } catch (Exception e){System.out.println("Error " + e);}
    }

    /**
     * Sends the bytes of a given file from the file path
     * @param out Output stream
     * @param fileName File name
     */
    private static void loadFile(OutputStream out, String fileName){
        try{
            byte[] buf = Files.readAllBytes(Path.of(folderPath + "/" + fileName));
            out.write(buf, 0, buf.length);
        } catch (Exception e){System.out.println("Error " + e);}
    }

    /**
     * Removes a file from the store
     * @param out Print writer
     * @param fileName File name
     */
    private static void removeFile(PrintWriter out, String fileName){
        File toDelete = new File(folderPath + "/" + fileName);

        if(toDelete.exists() && toDelete.isFile()){
            toDelete.delete();
            String message = Protocol.REMOVE_ACK_TOKEN + " " + fileName;
//            out.println(message);
            sendCommand(out,message);
            System.out.println("Command sent: " + message);
        } else{
            Errors.errorFileDoesNotExist(out);
        }
    }

    /**
     * Sends a list of files stored in the Dstore to the Controller
     * @param out Print writer
     */
    private static void listFiles(PrintWriter out){
        String message = Protocol.LIST_TOKEN + " ";

        File[] files = new File(folderPath).listFiles();

        for(File file : files){
            message = message + file.getName() + " ";
        }

//        out.println(message);
        sendCommand(out,message);
        System.out.println("Command sent: " + message);
    }

}
