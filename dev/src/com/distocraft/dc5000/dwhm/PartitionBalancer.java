package com.distocraft.dc5000.dwhm;

import java.util.List;
import java.util.ArrayList;

/**
 * Returns balanzed sequence within given size
 * 
 * @author Jukka Karvanen
 * 
 */
public class PartitionBalancer {

  private int size = 0;

  private int currentSize = 0;

  private Integer centerValue = null;

  private PartitionBalancer leftArm = null;

  private PartitionBalancer rightArm = null;

  public int getSize() {
    return size;
  }

  public void setSize(int size) {
    this.size = size;
  }

  public int getCurrentSize() {
    return currentSize;
  }

  /**
   * Create list of possible values
   * 
   * @param size
   *          max size of sequence
   */
  private void init(int size) {
    List data = new ArrayList(size);
    for (int i = 1; i <= size; i++) {
      data.add(new Integer(i));
    }
    setContent(data);

  }

  /**
   * Init data structure with recursive childs
   * 
   * @param data
   *          list of possible values
   */
  protected void setContent(List data) {
    currentSize = data.size();
    int armSize = currentSize / 2;

    // Set center value to node if size is odd
    if (data.size() % 2 != 0) {
      centerValue = (Integer) data.get(armSize);
    } else {
      centerValue = null;
    }

    // Create recursive childs objects if size is greater than 1
    if (armSize > 0) {
      leftArm = new PartitionBalancer();
      leftArm.setContent(data.subList(0, armSize));
      rightArm = new PartitionBalancer();
      rightArm.setContent(data.subList(data.size() - armSize, data.size()));
    }
  }

  /**
   * Return next balanzed value in sequence
   * 
   * @return sequence value
   */
  public int getNextValue() throws Exception {

    try {
      if (currentSize == 0) {
        init(size);  
      }

      int value;
      if (centerValue != null) {
        value = centerValue.intValue();
        centerValue = null;
      } else if (leftArm.getCurrentSize() >= rightArm.getCurrentSize()) {
        value = leftArm.getNextValue();
      } else {
        value = rightArm.getNextValue();
      }
      currentSize--;
      return value;
    } catch (Exception e) {
      
      throw new Exception("PartitionBalancer failed in getNextValue.",e);
    }
  }
}
