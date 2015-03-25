package com.datatorrent.demos.eventtree;

import java.util.Map;

import com.google.common.collect.Maps;

import com.datatorrent.api.BaseOperator;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;

/**
 * Created by Pramod Immaneni <pramod@datatorrent.com> on 3/13/15.
 */
public class EventExploder extends BaseOperator
{
  private int explodeFactor = 10;
  //private int explodeFactor = 2;
  
  private long processedInterval = 100;
  
  public transient final DefaultOutputPort<Event> output = new DefaultOutputPort<Event>();

  public transient final DefaultOutputPort<EventCount> count = new DefaultOutputPort<EventCount>();

  public transient final DefaultOutputPort<EventProcessed> processed = new DefaultOutputPort<EventProcessed>();/* {
    @Override
    public Unifier<EventProcessed> getUnifier()
    {
      return new EventProcessedUnifier();
    }
  };
  */
  
  private Map<Long, EventProcessed> processedCounts = Maps.newHashMap();
  private long lastProcessedSent = System.currentTimeMillis();
  
  public transient final DefaultInputPort<Event> input = new DefaultInputPort<Event>()
  {
    @Override
    public void process(Event event)
    {
      if (output.isConnected()) {
        int stage = event.stage + 1; 
        for (int i = 0; i < explodeFactor; ++i) {
          Event childEvent = new Event();
          childEvent.parentId = event.parentId;
          childEvent.stage = stage;
          output.emit(childEvent);
        }
        EventCount eventCount = new EventCount();
        eventCount.parentId = event.parentId;
        eventCount.stage = stage;
        eventCount.count = explodeFactor;
        count.emit(eventCount);
      } else if (processed.isConnected()) {
        EventProcessed eventProcessed = new EventProcessed();
        eventProcessed.parentId = event.parentId;
        eventProcessed.stage = event.stage;
        eventProcessed.count = 1;
        addProcessedCount(event.parentId, event.stage, 1);
        handleProcessedCounts(false);
      }
    }
  };
  
  private void addProcessedCount(Long parentId, int stage, int count)
  {
    EventProcessed eventProcessed = processedCounts.get(parentId);
    if (eventProcessed == null) {
      eventProcessed = new EventProcessed();
      eventProcessed.parentId = parentId;
      eventProcessed.stage = stage;
      processedCounts.put(parentId, eventProcessed);
    }
    eventProcessed.count += count;
  }
  
  private void handleProcessedCounts(boolean process) {
    long currTimestamp = System.currentTimeMillis();
    if (((currTimestamp - lastProcessedSent) >= processedInterval) || process) {
      for (EventProcessed eventProcessed : processedCounts.values()) {
        processed.emit(eventProcessed);
      }
      lastProcessedSent = currTimestamp;
      processedCounts.clear();
    }
  }

  @Override
  public void endWindow()
  {
    handleProcessedCounts(true);
  }

  public int getExplodeFactor()
  {
    return explodeFactor;
  }

  public void setExplodeFactor(int explodeFactor)
  {
    this.explodeFactor = explodeFactor;
  }
}
