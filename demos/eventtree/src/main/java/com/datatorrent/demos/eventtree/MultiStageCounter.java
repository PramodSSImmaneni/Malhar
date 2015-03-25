package com.datatorrent.demos.eventtree;

import java.util.Map;

import com.google.common.collect.Maps;

import com.datatorrent.api.BaseOperator;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;

/**
 * Created by Pramod Immaneni <pramod@datatorrent.com> on 3/13/15.
 */
public class MultiStageCounter extends BaseOperator
{
  
  public transient final DefaultOutputPort<EventComplete> complete = new DefaultOutputPort<EventComplete>();
  
  Map<Long, EventStatus> eventStatuses = Maps.newHashMap();
  
  protected void processEventCount(EventCount eventCount) {
    EventStatus eventStatus = getEventStatus(eventCount.parentId);
    StageStatus stageStatus = getStageStatus(eventCount.stage, eventStatus);
    stageStatus.numCounts++;
    stageStatus.count += eventCount.count;
    checkEventDone(eventStatus);
    //System.out.println(eventCount);
  }
  
  private void processEventProcessed(EventProcessed eventProcessed) {
    EventStatus eventStatus = getEventStatus(eventProcessed.parentId);
    StageStatus stageStatus = getStageStatus(eventProcessed.stage, eventStatus);
    stageStatus.count -= eventProcessed.count;
    // Minor optimization to not have to check at each event
    if (stageStatus.count == 0) {
      checkEventDone(eventStatus);
    }
    //System.out.println(eventProcessed);
  }

  private void checkEventDone(EventStatus eventStatus)
  {
    int count = checkStageDone(eventStatus.stageStatuses.size(), eventStatus);
    if (count == 0) {
      EventComplete eventComplete = new EventComplete();
      eventComplete.parentId = eventStatus.id;
      eventComplete.stage1TS = eventStatus.startTs;
      eventComplete.completeTS = System.currentTimeMillis();
      complete.emit(eventComplete);
      eventStatuses.remove(eventStatus.id);
    }
  }
  
  // Returns count if a stage is done else returns -1
  private int checkStageDone(int stage, EventStatus eventStatus) {
    StageStatus stageStatus = eventStatus.stageStatuses.get(stage);
    if (stageStatus != null) {
      if (!stageStatus.done) {
        if (stage == 1) {
          stageStatus.done = true;
        } else {
          int prevCount = checkStageDone(stage - 1, eventStatus);
          //System.out.println("Stage " + stage + " prev count " + prevCount + " num counts " + stageStatus.numCounts );
          if ((prevCount != -1) && (stageStatus.numCounts == prevCount)) {
            stageStatus.done = true;
          }
        }
      }
      if (stageStatus.done) return stageStatus.count;
    }
    return -1;
  }

  private EventStatus getEventStatus(Long id)
  {
    EventStatus eventStatus = eventStatuses.get(id);
    if (eventStatus == null) {
      eventStatus = new EventStatus();
      eventStatus.id = id;
      eventStatus.startTs = System.currentTimeMillis();
      eventStatuses.put(id, eventStatus);
    }
    return eventStatus;
  }

  private StageStatus getStageStatus(int stage, EventStatus eventStatus)
  {
    StageStatus stageStatus = eventStatus.stageStatuses.get(stage);
    if (stageStatus == null) {
      stageStatus = new StageStatus();
      eventStatus.stageStatuses.put(stage, stageStatus);
    }
    return stageStatus;
  }

  private static class EventStatus {
    Long id;
    long startTs;
    Map<Integer, StageStatus> stageStatuses = Maps.newHashMap();
  }

  private static class StageStatus {
    int numCounts;
    int count;
    boolean done;
  }

  public transient final DefaultInputPort<EventCount> stage1 = new DefaultInputPort<EventCount>()
  {
    @Override
    public void process(EventCount eventCount)
    {
      processEventCount(eventCount);
    }
  };

  public transient final DefaultInputPort<EventCount> stage2 = new DefaultInputPort<EventCount>()
  {
    @Override
    public void process(EventCount eventCount)
    {
      processEventCount(eventCount);
    }
  };

  public transient final DefaultInputPort<EventCount> stage3 = new DefaultInputPort<EventCount>()
  {
    @Override
    public void process(EventCount eventCount)
    {
      processEventCount(eventCount);
    }
  };

  public transient final DefaultInputPort<EventCount> stage4 = new DefaultInputPort<EventCount>()
  {
    @Override
    public void process(EventCount eventCount)
    {
      processEventCount(eventCount);
    }
  };

  public transient final DefaultInputPort<EventProcessed> processed = new DefaultInputPort<EventProcessed>()
  {
    @Override
    public void process(EventProcessed eventProcessed)
    {
      processEventProcessed(eventProcessed);
    }
  };

}
