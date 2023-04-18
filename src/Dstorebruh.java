import java.io.*;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.Path;

public class Dstorebruh {

    private static int port;
    private static int cport;
    private static int timeout;
    private static String folderPath;
    private static String fileName;
    private static int fileSize;
    private static PrintWriter controllerWriter;
    private static BufferedReader controllerReader;
    private static Socket controllerSocket;

    public static void main(String[] args){

//        port = Integer.parseInt(args[0]);
//        cport = Integer.parseInt(args[1]);
//        timeout = Integer.parseInt(args[2]);
//        folderPath = args[3];

        port = 10000;
        cport = 12345;
        timeout = 5000;
        folderPath = "bruh";

        if(clearFolder(new File(folderPath))){
            System.out.println("Folder cleared");
        } else{
            System.out.println("Folder not cleared");
        }

        while(true){
            try{
                controllerSocket = new Socket(InetAddress.getLocalHost(), cport);
                controllerReader = new BufferedReader(new InputStreamReader(controllerSocket.getInputStream()));
                controllerWriter = new PrintWriter(controllerSocket.getOutputStream(), true);

                String message = Protocol.JOIN_TOKEN + " " + port;
//                controllerWriter.println(message);
                writeMessage(controllerWriter,message);
                System.out.println("Command sent: " + message);
                break;
            } catch (Exception e){System.out.println("Error " + e);}
        }

        new Thread(() -> clientHandle()).start();

    }

    private static synchronized void writeMessage(PrintWriter out, String message){
        out.println(message);
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

    private static void clientHandle(){

        for(;;){
            try{
                ServerSocket ss = new ServerSocket(port);
                System.out.println("Dstore listening on port " + port);
                for(;;){
                    Socket clientSocket = ss.accept();
                    new Thread(() -> {
                        try{
                            BufferedReader clientReader = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
                            PrintWriter clientWriter = new PrintWriter(clientSocket.getOutputStream(), true);
                            String line;

                            while((line = clientReader.readLine()) != null){
                                String[] message = line.split(" ");
                                System.out.println("Received Command: "+ line);

                                switch (message[0]){
                                    case Protocol.STORE_TOKEN:
                                        storeReceiveHandle(clientWriter);
                                        fileName = message[1];
                                        fileSize = Integer.parseInt(message[2]);
                                        clientSocket.setSoTimeout(timeout);
                                        storeFileHandle(clientSocket.getInputStream(), controllerWriter, fileName);
                                        break;
                                    case Protocol.LOAD_DATA_TOKEN:
                                        loadFileHandle(clientSocket, message[1]);
                                        break;
                                    case Protocol.REMOVE_TOKEN:
                                        removeFileHandle(controllerWriter, message[1]);
                                        break;
                                    case Protocol.LIST_TOKEN:
                                        listFilesHandle(controllerWriter);
                                        break;
                                    default:
                                        System.out.println("ERROR client: Command not recognised -- " + line);
                                }
                            }

                        } catch(Exception e){}
                    }).start();
                }
            } catch (Exception e){System.out.println("Error " + e);}
        }


    }

    private static synchronized void storeReceiveHandle(PrintWriter out){
        out.println(Protocol.ACK_TOKEN);
        System.out.println("Command sent: " + Protocol.ACK_TOKEN);
    }

    private static synchronized void storeFileHandle(InputStream in, PrintWriter out, String fileName){
        try{
            byte[] buf = new byte[fileSize];
            in.readNBytes(buf,0,buf.length);
            FileOutputStream fileOutputStream = new FileOutputStream(folderPath + "/" + fileName);
            fileOutputStream.write(buf,0,buf.length);
            fileOutputStream.close();

            String message = Protocol.STORE_ACK_TOKEN + " " + fileName;
//            out.println(message);
            writeMessage(out,message);
            System.out.println("Command sent: " + message);
        } catch (Exception e){System.out.println("Error " + e);}
    }

    private static synchronized void removeFileHandle(PrintWriter out, String fileName){
        File toDelete = new File(folderPath + "/" + fileName);

        if(toDelete.exists() && toDelete.isFile()){
            toDelete.delete();
            String message = Protocol.REMOVE_ACK_TOKEN + " " + fileName;
//            out.println(message);
            writeMessage(out,message);
            System.out.println("Command sent: " + message);
        } else{
            Errors.errorFileDoesNotExist(out);
        }
    }

    private static synchronized void loadFileHandle(Socket client, String fileName) throws IOException {
        try{
            byte[] buf = Files.readAllBytes(Path.of(folderPath + "/" + fileName));
            OutputStream out = client.getOutputStream();
            out.write(buf,0,buf.length);
        } catch (Exception e) {
            System.out.println("Error " + e);
            client.close();
        }
    }

    private static synchronized void listFilesHandle(PrintWriter out){
        String message = Protocol.LIST_TOKEN + " ";

        File[] files = new File(folderPath).listFiles();

        for(File file : files){
            message = message + file.getName() + " ";
        }

//        out.println(message);
        writeMessage(out,message);
        System.out.println("Command sent: " + message);

    }

}
