package com.datatorrent.demos.eventtree;

/**
 * Created by Pramod Immaneni <pramod@datatorrent.com> on 3/13/15.
 */
public class EventProcessed
{
  Long parentId;
  int stage;
  int count;

  @Override
  public boolean equals(Object o)
  {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    EventProcessed that = (EventProcessed) o;

    if (parentId != null ? !parentId.equals(that.parentId) : that.parentId != null) return false;

    return true;
  }

  @Override
  public int hashCode()
  {
    return parentId != null ? parentId.hashCode() : 0;
  }

  @Override
  public String toString()
  {
    return "EventProcessed{" +
            "parentId=" + parentId +
            ", stage=" + stage +
            ", count=" + count +
            '}';
  }
}
