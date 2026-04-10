import java.io.*;
import java.net.*;
import java.util.*;

public class WebServer {
    public static void main(String[] args) throws Exception {
        //setup port that will be used
        int port = 6789;

        //creating server socket to listen
        ServerSocket server = new ServerSocket(port);
        //infinite loop to create threads and handle requests
        while (true) {
            //listening on server socket
            Socket clientSocket = server.accept();
            //creating object and thread to handle the http request
            HttpRequest request = new HttpRequest(clientSocket);
            Thread thread = new Thread(request);
            thread.start();
        }
    }
}

class HttpRequest implements Runnable {
    final String CRLF = "\r\n";
    Socket socket;
    public HttpRequest(Socket socket) throws Exception {
        this.socket = socket;
    }

    @Override
    public void run() {
        try {
            processRequest();
        } catch (Exception e) {
            System.out.println(e);
        }
    }

    private void processRequest() throws Exception {

    }
}