package com.datatorrent.demos.eventtree;

/**
 * Created by Pramod Immaneni <pramod@datatorrent.com> on 3/13/15.
 */
public class EventCount
{
  long parentId;
  int stage;
  int count;

  @Override
  public boolean equals(Object o)
  {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    EventCount that = (EventCount) o;

    if (parentId != that.parentId) return false;

    return true;
  }

  @Override
  public int hashCode()
  {
    return (int) (parentId ^ (parentId >>> 32));
  }

  @Override
  public String toString()
  {
    return "EventCount{" +
            "parentId=" + parentId +
            ", stage=" + stage +
            ", count=" + count +
            '}';
  }
}
