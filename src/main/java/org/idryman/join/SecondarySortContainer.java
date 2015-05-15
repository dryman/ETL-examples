package org.idryman.join;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class SecondarySortContainer<T extends WritableComparable> implements WritableComparable<SecondarySortContainer<T>>{
  private T   value;
  private int order;
  
  public SecondarySortContainer(T value, int order) {
    this.value = value;
    this.order = order;
  }
  
  @Override
  public void write(DataOutput out) throws IOException {
    value.write(out);
    out.writeInt(order);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    value.readFields(in);
    order = in.readInt();
  }

  @Override
  public int compareTo(SecondarySortContainer o) {
    int cmp = value.compareTo((T)o.value);
    if (cmp!=0) return cmp;
    return Integer.compare(order, o.order);
  }

  public T getValue() {
    return value;
  }
  
  public int getOrder() {
    return order;
  }
}
