package com.datatorrent.demos.eventtree;

import com.datatorrent.api.Context;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.InputOperator;

import com.datatorrent.common.util.DTThrowable;

/**
 * Created by Pramod Immaneni <pramod@datatorrent.com> on 3/13/15.
 */
public class EventInput implements InputOperator
{
  
  long id;
  
  int explodeFactor = 10;
  //int explodeFactor = 2;
  
  int tuplesBlast = 10;
  //int tuplesBlast = 1;
  long waitTimeout = 100;
  //long waitTimeout = 500;
  
  public transient final DefaultOutputPort<Event> output = new DefaultOutputPort<Event>();

  public transient final DefaultOutputPort<EventCount> count = new DefaultOutputPort<EventCount>();
  
  @Override
  public void emitTuples()
  {
    for (int i = 0; i < tuplesBlast; ++i) {
      // Doing explode in stage 1 itself
      /*
      Event event = new Event();
      event.parentId = id;
      event.stage = 1;
      output.emit(event);
      */
      long nextId = id++;
      for (int j = 0; j < explodeFactor; ++j) {
        Event event = new Event();
        event.parentId = nextId;
        event.stage = 1;
        output.emit(event);
      }
      EventCount eventCount = new EventCount();
      eventCount.parentId = nextId;
      eventCount.stage = 1;
      eventCount.count = explodeFactor;
      count.emit(eventCount);
    }
    try {
      Thread.sleep(waitTimeout);
    } catch (InterruptedException e) {
      DTThrowable.rethrow(e);
    }
  }

  @Override
  public void beginWindow(long l)
  {

  }

  @Override
  public void endWindow()
  {

  }

  @Override
  public void setup(Context.OperatorContext context)
  {

  }

  @Override
  public void teardown()
  {

  }
}
