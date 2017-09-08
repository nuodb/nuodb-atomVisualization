// (C) Copyright NuoDB, Inc. 2007-2017  All Rights Reserved.

import java.io.PrintWriter;

import java.net.ServerSocket;
import java.net.Socket;

import java.sql.*;

import java.util.Properties;
import java.util.Map;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Random;

class InsertLoad extends Thread
{
    private static final String DRIVER = "com.nuodb.jdbc.Driver";
    
    private static class Configuration 
    {
        private static final String ARG_USER            = "--user";
        private static final String ARG_PASSWORD        = "--password";
        private static final String ARG_CLIENTS         = "--clients";
        private static final String ARG_OPERATIONS      = "--operations";
        private static final String ARG_BATCH_SIZE      = "--batch-size";
        private static final String ARG_RECONNECT_AFTER = "--reconnect-after";
        private static final String ARG_DISABLE_INDEX   = "--disable-index";
        private static final String ARG_DISABLE_RANDOM  = "--disable-random";
        private static final String ARG_SEPARATE_TABLES = "--separate-tables";
        private static final String ARG_AUTO_COMMIT     = "--auto-commit";        
        

        private Map<String, String> configuration;
        private String database = "test@localhost";
        
        Configuration()
        {
            try {
                configuration = new HashMap<String, String>();
                configuration.put(ARG_USER,     "dba");
                configuration.put(ARG_PASSWORD, "dba");
                configuration.put(ARG_CLIENTS,  "20");
                configuration.put(ARG_OPERATIONS, "1000000");
                configuration.put(ARG_BATCH_SIZE, "200");
                configuration.put(ARG_RECONNECT_AFTER, Integer.toString(Integer.MAX_VALUE));
                configuration.put(ARG_DISABLE_INDEX, "false");
                configuration.put(ARG_SEPARATE_TABLES, "false");
                configuration.put(ARG_DISABLE_RANDOM, "false");
                configuration.put(ARG_AUTO_COMMIT, "false");
            } catch (Exception x) {
                x.printStackTrace();
                System.exit(-1);
            }
        }

        private void parse(String[] args) 
        {
            String key = null;
            
            for (String arg : args) {
                if (key != null) {
                    configuration.put(key, arg);
                    key = null;
                    
                    continue;
                }

                if (arg.startsWith("--")) {
                    if (arg.equals(ARG_DISABLE_INDEX) ||
                        arg.equals(ARG_DISABLE_RANDOM) ||
                        arg.equals(ARG_SEPARATE_TABLES) ||
                        arg.equals(ARG_AUTO_COMMIT)) {
                        configuration.put(arg, "true");
                    } else {
                        key = arg;
                    }
                }

                if (arg.indexOf("@") != -1) {
                    database = arg;
                }
            }
        }

        public String toString() 
        {
            StringBuilder sb = new StringBuilder("\n\tParameters:");

            for (Iterator<Map.Entry<String, String>> it= configuration.entrySet().iterator(); it.hasNext(); ) {
                Map.Entry<String, String> entry = it.next();
                String key = entry.getKey();
                sb.append("\n\t  ").append(key).append(" ");

                if (!key.equals(ARG_DISABLE_INDEX) &&
                    !key.equals(ARG_DISABLE_RANDOM) &&
                    !key.equals(ARG_SEPARATE_TABLES) &&
                    !key.equals(ARG_AUTO_COMMIT)) {
                    sb.append("<default ").append(entry.getValue()).append(">, ");
                }
            }

            int len = sb.length();
            sb.delete(len - 2, len);
            sb.append("\n\tDatabase:\n\t  <default ").append(database).append(">");

            return sb.toString();
        }

        public String getUser()
        {
            return configuration.get(ARG_USER);
        }

        public String getPassword()
        {
            return configuration.get(ARG_PASSWORD);
        }

        public int getClients() throws Exception
        {
            return Integer.parseInt(configuration.get(ARG_CLIENTS));
        }

        public int getOperations() throws Exception
        {
            return Integer.parseInt(configuration.get(ARG_OPERATIONS));
        }

        public int getBatchSize() throws Exception
        {
            return Integer.parseInt(configuration.get(ARG_BATCH_SIZE));
        }

        public int getReconnectAfter() throws Exception
        {
            return Integer.parseInt(configuration.get(ARG_RECONNECT_AFTER));
        }

        public boolean useIndex() throws Exception
        {
            return !Boolean.parseBoolean(configuration.get(ARG_DISABLE_INDEX));
        }

        public boolean separateTables() throws Exception
        {
            return Boolean.parseBoolean(configuration.get(ARG_SEPARATE_TABLES));
        }

        public boolean useRandom() throws Exception 
        {
            return !Boolean.parseBoolean(configuration.get(ARG_DISABLE_RANDOM));
        }
        
        public boolean autoCommit() throws Exception
        {
            return Boolean.parseBoolean(configuration.get(ARG_AUTO_COMMIT));
        }

        public String getDBURL()
        {
            String[] parts = database.split("@");

            return "jdbc:com.nuodb://" + parts[1] + "/" + parts[0];
        }

        public Properties getConnectionProperties() 
        {
            Properties p = new Properties();
            p.setProperty("user", getUser());
            p.setProperty("password", getPassword());
            p.setProperty("schema", "test");

            return p;
        }
    };

    private static class Monitor extends Thread
    {
        private Monitor(long started, InsertLoad[] drivers)
        {
            this.started = started;
            this.drivers = drivers;
            this.active  = true;
            this.rate = 0.0;
        }

        public void run() 
        {
            try {
                while (active) {
                    synchronized(this) {
                        wait(5000);
                    }

                    if (active) {
                        int inserted = 0;
                        
                        for (InsertLoad driver : drivers) {
                            inserted += driver.insertedRows();
                        }


                        long finished = System.nanoTime();
                        long secs = (finished - started) / 1000000000;
                        rate = inserted / secs;
                        System.out.println("Total time " + secs + " secs, " + 
                                           "inserted " + inserted + " rows, " +
                                           "rate " + rate + " IPS.");

                    }
                }
            } catch (Exception x) {
                x.printStackTrace();
            }
        }

        private void shutdown() throws Exception
        {
            active = false;
            
            synchronized(this) {
                notify();
            }

            join();
        }

        private long started;
        private InsertLoad[] drivers;
        private volatile boolean active;
        private double rate;
    };

    private static class HTTPServer extends Thread
    {
        private HTTPServer(Monitor monitor, int port)
        {
            this.monitor = monitor;
            this.port = port;
            this.serverSocket = null;
        }

        public void run()
        {
            try {
                System.out.println("HTTP started on port " + port);
                serverSocket = new ServerSocket(port);

                do {
                    Socket socket = serverSocket.accept();
                    PrintWriter writer = new PrintWriter(socket.getOutputStream());
                    String rate = Double.toString(monitor.rate);
                    writer.print("HTTP/1.0 200 OK\r\n");
                    writer.print("Content-Type: application/json\r\n");
                    writer.print("Content-Length: " + rate.length() + "\r\n");
                    writer.print("\r\n");
                    writer.print(rate);
                    writer.flush();
                    // socket.close();
                } while(monitor.active);

            } catch (Exception x) {
                x.printStackTrace();
            } finally {
                System.out.println("HTTP finished");
            }
        }

        private void shutdown() throws Exception
        {
            synchronized(this) {
                if (serverSocket != null) {
                    serverSocket.close();
                }
            }

            join();
        }
        

        private Monitor monitor;
        private ServerSocket serverSocket;
        private int port;
    };

    public static void main(String[] argv)
    {
        try {
            Configuration configuration = new Configuration();
            
            if (argv.length == 1 && argv[0].indexOf("help") != -1) {
                System.out.println("java InsertLoad [parameters] [database]" + configuration);
                System.exit(0);
            }

            configuration.parse(argv);

            int clients    = configuration.getClients();
            int operations = configuration.getOperations();

            System.out.println("Insert load test: clients " + clients +
                               ", operations per client " + operations);

            Class.forName(DRIVER);

            if (!configuration.separateTables()) {
                Connection conn = DriverManager.getConnection(configuration.getDBURL(),
                                                              configuration.getConnectionProperties());
                System.out.println ("dropping table...");
                Statement st = conn.createStatement();
                st.execute("drop table foo if exists");
                System.out.println ("create table...");
                st.execute("create table foo (v string, i int)");

                if (configuration.useIndex()) {
                     st.execute("create index fooV on foo(v)");
                }

                st.close();
                conn.close();
            }

            InsertLoad[] drivers = new InsertLoad[clients];
            long started = System.nanoTime();

            for (int i = 0; i < clients; i++) {
                drivers[i] = new InsertLoad(i * operations, configuration);
                drivers[i].start();
            }

            Monitor monitor = new Monitor(started, drivers);
            monitor.start();

            HTTPServer server = new HTTPServer(monitor, 17170);
            server.start();

            int inserted = 0;

            for (InsertLoad driver : drivers) {
                driver.join();
                inserted += driver.insertedRows();
            }

            long finished = System.nanoTime();
            long secs = (finished - started) / 1000000000;
            monitor.shutdown();
            server.shutdown();
            System.out.println("Clients " + clients + ", " +
                               "total time " + secs + " secs, " + 
                               "inserted " + inserted + " rows, " +
                               "rate " + (inserted / secs) + " IPS.");
        } catch (Exception x) {
            x.printStackTrace();
            System.exit(-1);
        }
    }

    public InsertLoad(int start, Configuration configuration) throws Exception
    {
        value = "hellooooooooooooooooooo";
        this.start = start;
        this.configuration = configuration;
        this.table = configuration.separateTables() ? "foo" + getDriverId() : "foo";

        randgen = configuration.useRandom() ? new Random() : null;
        inserted = 0;
        active   = true;

        System.out.println("Inserter #" + getDriverId() + " range [" + start +
                           ", " + (start + configuration.getOperations()) +
                           "], table " + table + ", random " + (randgen != null));

    }

    public int getDriverId() throws Exception
    {
        return start / configuration.getOperations() + 1;
    }

    public int insertedRows() 
    {
        return inserted;
    }

    public void run()
    {
        try {
            String dburl = configuration.getDBURL();
            Properties props = configuration.getConnectionProperties();
            boolean autocommit = configuration.autoCommit();

            Connection conn = DriverManager.getConnection(dburl, props);
            conn.setAutoCommit(autocommit);

            if (configuration.separateTables()) {
                Statement st = conn.createStatement();
                st.execute("drop table " + table + " if exists");
                st.execute("create table " + table + " (v string, i int)");

                if (configuration.useIndex()) {
                     st.execute("create index "+ table + "V on " + table + "(v)");
                }

                st.close();
            }

            PreparedStatement pst = conn.prepareStatement("insert into " + table + "(v,i) values(?,?)");
            int end = start + configuration.getOperations();
            int operations = configuration.getOperations();
            int batchSize = configuration.getBatchSize();
            int reconnectAfter = configuration.getReconnectAfter();

            int currentlyInserted = 0;

            for (inserted = 0; active && inserted < operations; ) {
                try {
                    int batch = Math.min(batchSize, operations - inserted);

                    if (batch == 1) {
                        int prefix = start;

                        if (randgen != null) {
                            prefix += randgen.nextInt(operations);
                        }
                
                        pst.setString(1, prefix + value + inserted);
                        pst.setInt(2, randgen.nextInt(operations));
                        pst.executeUpdate();                        
                    } else {
                        for (int j = 0; j < batch; j++) {
                            int prefix = start;
                            int val = 0;

                            if (randgen != null) {
                                prefix += randgen.nextInt(operations);
                                val = randgen.nextInt(operations);
                            }

                
                            pst.setString(1, prefix + value + inserted + j);
                            pst.setInt(2, val);
                            pst.addBatch();
                        }

                        pst.executeBatch();
                    }

                    if (!autocommit) {
                        conn.commit();
                    }

                    inserted += batch;
                    currentlyInserted += batch;

                    if (currentlyInserted >= reconnectAfter) {
                        pst.close();
                        conn.close();
                        conn = DriverManager.getConnection(dburl, props);
                        conn.setAutoCommit(autocommit);
                        pst = conn.prepareStatement("insert into " + table + "(v,i) values(?,?)");
                        currentlyInserted = 0;
                    }
                } catch (SQLException sqlx) {
                    System.err.println(sqlx);
                }
            }
            
            pst.close();
            conn.close();
        } catch(Exception x) {
            x.printStackTrace();
            System.exit(-1);
        }
    }

    private int                 start;
    private Configuration       configuration;
    private Random              randgen;
    private String              value;
    private String              table;
    private volatile int        inserted;
    private volatile boolean    active;
}
