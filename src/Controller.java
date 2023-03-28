import java.io.*;
import java.net.*;
import java.util.logging.Logger;

public class Controller {

    static Logger logger = Logger.getLogger(Controller.class.getName());

    private static int cport;
    private static int r;
    private static int timeout;
    private static int rebalance;

    public static void main(String[] args){

        cport = Integer.parseInt(args[0]);
        r = Integer.parseInt(args[1]);
        timeout = Integer.parseInt(args[2]);
        rebalance = Integer.parseInt(args[3]);

        try{
            ServerSocket ss = new ServerSocket(cport);
            logger.info("Listening on port " + cport);
            for(;;){
                try{
                    Socket client = ss.accept();
                    logger.info("Connected");
                    BufferedReader in = new BufferedReader(new InputStreamReader(client.getInputStream()));
                    String line;
                    while((line = in.readLine()) != null) {

                    }
                } catch (Exception e){System.out.println("Error " + e);}
            }
        }catch (Exception e){System.out.println("Error " + e);}

    }

}
