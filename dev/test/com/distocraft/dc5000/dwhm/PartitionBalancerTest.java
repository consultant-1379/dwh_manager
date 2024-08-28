package com.distocraft.dc5000.dwhm;

import static org.junit.Assert.*;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;

import org.junit.BeforeClass;
import org.junit.Test;

/**
 * 
 * @author ejarsok
 *
 */

public class PartitionBalancerTest {

  private static Field size;
  
  private static Field currentSize;
  
  private static Field centerValue;
  
  private static Method init;
  
  @BeforeClass
  public static void init() {
    try {
      size = PartitionBalancer.class.getDeclaredField("size");
      currentSize = PartitionBalancer.class.getDeclaredField("currentSize");
      centerValue = PartitionBalancer.class.getDeclaredField("centerValue");
      init = PartitionBalancer.class.getDeclaredMethod("init", new Class[] {int.class});
      
      size.setAccessible(true);
      currentSize.setAccessible(true);
      centerValue.setAccessible(true);
      init.setAccessible(true);
      
    } catch (Exception e) {
      e.printStackTrace();
      fail("init() failed");
    }
  }
  
  @Test
  public void testGetSize() {
    PartitionBalancer pb = new PartitionBalancer();
    
    try {
      size.set(pb, 10);
      
      assertEquals(10, pb.getSize());
      
    } catch (Exception e) {
      e.printStackTrace();
      fail("testGetSize() failed");
    }
  }

  @Test
  public void testSetSize() {
    PartitionBalancer pb = new PartitionBalancer();
    
    pb.setSize(20);
    
    try {
      assertEquals(20, size.get(pb));
      
    } catch (Exception e) {
      e.printStackTrace();
      fail("testSetSize() failed");
    }
  }

  @Test
  public void testGetCurrentSize() {
    PartitionBalancer pb = new PartitionBalancer();
    
    try {
      currentSize.set(pb, 30);
      
      assertEquals(30, pb.getCurrentSize());
      
    } catch (Exception e) {
      e.printStackTrace();
      fail("testGetCurrentSize() failed");
    }
  }

  @Test
  public void testInit() {
    PartitionBalancer pb = new PartitionBalancer();
    
    try {
      init.invoke(pb, new Object[] {3});
      
      String expected = "3,2";
      String actual = "" + currentSize.get(pb) + "," + centerValue.get(pb);
      
      assertEquals(expected, actual);
      
    } catch (Exception e) {
      e.printStackTrace();
      fail("testInit() failed");
    }
  }
  
  @Test
  public void testSetContent1() {
    PartitionBalancer pb = new PartitionBalancer();
    
    ArrayList data = new ArrayList();
    data.add(1);
    data.add(2);
    data.add(3);
    data.add(4);
    
    pb.setContent(data);
    
    try {
      String expected = "4,null";
      String actual = "" + currentSize.get(pb) + "," + centerValue.get(pb);
      
      assertEquals(expected, actual);
      
    } catch (Exception e) {
      e.printStackTrace();
      fail("testSetContent1() failed");
    }
  }
  
  @Test
  public void testSetContent2() {
    PartitionBalancer pb = new PartitionBalancer();
    
    ArrayList data = new ArrayList();
    data.add(1);
    data.add(2);
    data.add(3);
    
    pb.setContent(data);
    
    try {
      String expected = "3,2";
      String actual = "" + currentSize.get(pb) + "," + centerValue.get(pb);
      
      assertEquals(expected, actual);
      
    } catch (Exception e) {
      e.printStackTrace();
      fail("testSetContent2() failed");
    }
  }

  @Test
  public void testGetNextValue1() {
    PartitionBalancer pb = new PartitionBalancer();
    
    pb.setSize(5);
    
    try {
      assertEquals(3, pb.getNextValue());
           
    } catch (Exception e) {
      e.printStackTrace();
      fail("testGetNextValue1() failed");
    }
  }

  @Test
  public void testGetNextValue2() {
    PartitionBalancer pb = new PartitionBalancer();
    
    pb.setSize(4);
    
    try {
      assertEquals(1, pb.getNextValue());
      
    } catch (Exception e) {
      e.printStackTrace();
      fail("testGetNextValue2() failed");
    }
  }
}
