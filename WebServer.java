import java.io.*;
import java.net.*;
import java.util.*;

public class WebServer {
    public static void main(String[] args) throws Exception {
        int port = 6789;

        ServerSocket server = new ServerSocket(port);
        while (true) {
            Socket clientSocket = server.accept();

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