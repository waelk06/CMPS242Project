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
        // Get a reference to the socket’s input and output
        // streams
        InputStream is = socket.getInputStream();
        DataOutputStream os = new DataOutputStream(socket.getOutputStream());
        // Set up input stream filters
        InputStreamReader isr = new InputStreamReader(is);
        BufferedReader br = new BufferedReader(isr);
        // Get the request line of the HTTP request message.
        String requestLine = br.readLine();
        // Display the request line.
        System.out.println();
        System.out.println(requestLine);
        // Get and display the header lines.
        String headerLine = null;
        while ((headerLine = br.readLine()).length() != 0)
        {
        System.out.println(headerLine);
        }

        // Extract the filename from the request line.
        StringTokenizer tokens = new StringTokenizer(requestLine);
        tokens.nextToken(); // skip over the method, which should be "GET"
        String fileName = tokens.nextToken();
        // Prepend a "." so that file request is within the current directory.
        fileName = "." + fileName;

        // Open the requested file.
        FileInputStream fis = null;
        boolean fileExists = true;
        try
        {
        fis = new FileInputStream(fileName);
        }
        catch (FileNotFoundException e)
        {
        fileExists = false;
        }

        // Close streams and socket.
        os.close();
        br.close();
        socket.close();
    }
}