import java.io.File;

public class Dstore {

    private static int port;
    private static int cport;
    private static int timeout;
    private static String filePath;

    public static void main(String[] args){
        port = Integer.parseInt(args[0]);
        cport = Integer.parseInt(args[1]);
        timeout = Integer.parseInt(args[2]);
        filePath = args[3];
    }

}
