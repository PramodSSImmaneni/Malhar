package com.datatorrent.demos.eventtree;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Created by Pramod Immaneni <pramod@datatorrent.com> on 3/13/15.
 */
public class EventComplete
{
  long parentId;
  long stage1TS;
  long completeTS;
  
  transient DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");

  @Override
  public String toString()
  {
    return "EventComplete{" +
            "id=" + parentId +
            ", start timestamp=" + dateFormat.format(new Date(stage1TS)) +
            ", complete timestamp=" + dateFormat.format(new Date(completeTS)) +
            '}';
  }
}
