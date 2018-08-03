package sd.kvstore;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.concurrent.CompletableFuture;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.atomix.catalyst.transport.Address;
import io.atomix.catalyst.transport.netty.NettyTransport;
import io.atomix.copycat.client.CopycatClient;
import io.atomix.copycat.server.CopycatServer;
import io.atomix.copycat.server.cluster.Member.Type;
import io.atomix.copycat.server.storage.Storage;
import io.atomix.copycat.server.storage.StorageLevel;

import sd.operations.Delete;
import sd.operations.EntryEvent;
import sd.operations.Get;
import sd.operations.Listen;
import sd.operations.Put;
import sd.operations.PutWithTtl;

public class KeyValueServer {
  private static final Logger LOG = LoggerFactory.getLogger(KeyValueServer.class);

  public static void main(String... args) throws Throwable {
    deleteLogs();
    Address address = new Address("localhost", 5000);

    // Start the server
    CopycatServer server = CopycatServer.builder(address)
        .withStateMachine(KeyValueStore::new)
        .withTransport(NettyTransport.builder().withThreads(4).build())
        .withStorage(Storage.builder().withDirectory(new File("/home/mafra/workspace/distributed-datastore/kvstore/src/main/java/sd/kvstore/logs")).withStorageLevel(StorageLevel.DISK).build())
        .build();

    server.bootstrap().thenAccept(srvr -> LOG.info(srvr + " has bootstrapped a cluster")).join();
    
    Address clusterAddress = new Address("localhost", 5000);
    server.join(clusterAddress).thenAccept(srvr -> System.out.println(srvr + " has joined the cluster"));
    // Add two new servers to cluster
    Address server1Address = new Address("localhost", 5001);
    CopycatServer server1 = CopycatServer.builder(server1Address)
            .withStateMachine(KeyValueStore::new)
            .withTransport(NettyTransport.builder().withThreads(4).build())
            .withStorage(Storage.builder().withDirectory(new File("/home/mafra/workspace/distributed-datastore/kvstore/src/main/java/sd/kvstore/logs")).withStorageLevel(StorageLevel.DISK).build())
            .build();
    
    server1.join(clusterAddress).thenAccept(srvr -> System.out.println(srvr + " has joined the cluster"));
    
    Address server2Address = new Address("localhost", 5002);
    CopycatServer server2 = CopycatServer.builder(server2Address)
            .withStateMachine(KeyValueStore::new)
            .withTransport(NettyTransport.builder().withThreads(4).build())
            .withStorage(Storage.builder().withDirectory(new File("/home/mafra/workspace/distributed-datastore/kvstore/src/main/java/sd/kvstore/logs")).withStorageLevel(StorageLevel.DISK).build())
            .build();
    
    server2.join(clusterAddress).thenAccept(srvr -> System.out.println(srvr + " has joined the cluster"));
    
    Address server3Address = new Address("localhost", 5003);
    CopycatServer server3 = CopycatServer.builder(server3Address)
            .withStateMachine(KeyValueStore::new)
            .withTransport(NettyTransport.builder().withThreads(4).build())
            .withStorage(Storage.builder().withDirectory(new File("/home/mafra/workspace/distributed-datastore/kvstore/src/main/java/sd/kvstore/logs")).withStorageLevel(StorageLevel.DISK).build())
            .build();
    
    server3.join(clusterAddress).thenAccept(srvr -> System.out.println(srvr + " has joined the cluster"));
    
    Address server4Address = new Address("localhost", 5004);
    CopycatServer server4 = CopycatServer.builder(server4Address)
            .withStateMachine(KeyValueStore::new)
            .withTransport(NettyTransport.builder().withThreads(4).build())
            .withStorage(Storage.builder().withDirectory(new File("/home/mafra/workspace/distributed-datastore/kvstore/src/main/java/sd/kvstore/logs")).withStorageLevel(StorageLevel.DISK).build())
            .build();
    
    server4.join(clusterAddress).thenAccept(srvr -> System.out.println(srvr + " has joined the cluster"));
    
    
//    Address server5Address = new Address("localhost", 5005);
//    CopycatServer server5 = CopycatServer.builder(server5Address)
//            .withStateMachine(KeyValueStore::new)
//            .withTransport(NettyTransport.builder().withThreads(4).build())
//            .withStorage(Storage.builder().withDirectory(new File("/local/mafra/workspace/distributed-datastore/kvstore/src/main/java/sd/kvstore/logs")).withStorageLevel(StorageLevel.DISK).build())
//            .build();
//    
//    server5.join(clusterAddress).thenAccept(srvr -> System.out.println(srvr + " has joined the cluster"));
    
    // Waiting for synchronization of servers in cluster.
    Thread.sleep(5000);
    
    
    // Create a client
    CopycatClient client = CopycatClient.builder()
        .withTransport(NettyTransport.builder().withThreads(2).build())
        .build();

    // Connect to the server
    client.connect(clusterAddress).join();

    // Submit operations
    
    client.submit(new Put(1, "Hello world!"));
   
    Thread thread = new Thread(){
    	
        int cont = 0;
        
        public void run(){
          while (true) {
        	  cont += 1;
        	  
        	  if (cont == 10) {
        		   server.leave();
        		   
        	  } else {
        		  try {
      				Thread.sleep(500);
      			} catch (InterruptedException e1) {
      				e1.printStackTrace();
      			}
        		
        		
        		try {
        			   CompletableFuture<Void> f = client.submit(new Get(1)).thenAccept(result -> System.out.println("Result is: " + result));
        			   
        			   if (f.get() != null) {
            			   System.out.println(f.get());
        			   }
        		} catch (Exception e) {
    				System.out.println(e.getMessage());
    				
    			}
        		  
        	  }
        	  
          }
        }
     };   
    thread.start();
   
//    //Install listeners
//    client.submit(new Listen()).thenRun(() -> LOG.info("listener registered")).join();
//    client.onEvent("put", (EntryEvent event) -> LOG.info("put event received for: " + event.key));
//    client.onEvent("delete", (EntryEvent event) -> LOG.info("delete event received for: " + event.key));
//    client.onEvent("expire", (EntryEvent event) -> LOG.info("TTL expired event received for: " + event.key));
//
//    // Submit operations to trigger event listeners
//    client.submit(new Put("foo", "bar")).join();
//    client.submit(new Get("foo")).join();
//    client.submit(new Delete("foo")).join();
//    client.submit(new PutWithTtl("foo", "bar", 1000)).join();

  }

  private static void deleteLogs() throws Throwable {
    Files.walkFileTree(new File("/home/mafra/workspace/distributed-datastore/kvstore/src/main/java/sd/kvstore/logs").toPath(), new SimpleFileVisitor<Path>() {
      @Override
      public FileVisitResult visitFile(final Path file, final BasicFileAttributes attrs) throws IOException {
        Files.delete(file);
        return FileVisitResult.CONTINUE;
      }

      @Override
      public FileVisitResult postVisitDirectory(final Path dir, final IOException e) throws IOException {
        Files.delete(dir);
        return FileVisitResult.CONTINUE;
      }
    });
  }
}
