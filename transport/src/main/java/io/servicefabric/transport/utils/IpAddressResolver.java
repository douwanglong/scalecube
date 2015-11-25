package io.servicefabric.transport.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.*;
import java.util.Enumeration;

/**
 * Utility class that defines node's IP address which is different from localhost.
 */
public class IpAddressResolver {

  /** The Constant LOGGER. */
  private static final Logger LOGGER = LoggerFactory.getLogger(IpAddressResolver.class);

  /**
   * Instantiates a new ip address resolver.
   */
  private IpAddressResolver() {
    /* Can't be instantiated */
  }

  /**
   * Resolve ip address.
   *
   * @return the inet address
   * @throws java.net.UnknownHostException the unknown host exception
   */
  public static InetAddress resolveIpAddress() throws UnknownHostException {
    Enumeration<NetworkInterface> netInterfaces = null;
    try {
      netInterfaces = NetworkInterface.getNetworkInterfaces();
    } catch (SocketException e) {
      LOGGER.error("Socket error during resolving IP address", e);
    }

    while (netInterfaces != null && netInterfaces.hasMoreElements()) {
      NetworkInterface ni = netInterfaces.nextElement();
      Enumeration<InetAddress> address = ni.getInetAddresses();
      while (address.hasMoreElements()) {
        InetAddress addr = address.nextElement();
        LOGGER.debug("Found network interface: {}", addr.getHostAddress());
        if (!addr.isLoopbackAddress() && addr.getAddress().length == 4 // for IP4 addresses
        ) {
          return addr;
        }
      }
    }
    return InetAddress.getLocalHost();
  }


  /**
   * Checks to see if a specific port is available.
   *
   * @param port the port to check for availability
   */
    public static boolean available(int port) {

    ServerSocket ss = null;
    DatagramSocket ds = null;
    try {
      ss = new ServerSocket(port);
      ss.setReuseAddress(true);
      ds = new DatagramSocket(port);
      ds.setReuseAddress(true);
      return true;
    } catch (IOException ignore) {
      //ignore
    } finally {
      if (ds != null) {
        ds.close();
      }

      if (ss != null) {
        try {
          ss.close();
        } catch (IOException e) {
                /* should not be thrown */
        }
      }
    }

    return false;
  }

}
