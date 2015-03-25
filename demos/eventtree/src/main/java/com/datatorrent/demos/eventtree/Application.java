/**
 * Put your copyright and license info here.
 */
package com.datatorrent.demos.eventtree;

import org.apache.hadoop.conf.Configuration;

import com.datatorrent.lib.io.ConsoleOutputOperator;

import com.datatorrent.api.DAG;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.ApplicationAnnotation;

@ApplicationAnnotation(name="EventTree")
public class Application implements StreamingApplication
{

  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    EventInput stage1 = dag.addOperator("InputStage1", EventInput.class);
    EventExploder stage2 = dag.addOperator("Stage2", EventExploder.class);
    EventExploder stage3 = dag.addOperator("Stage3", EventExploder.class);
    EventExploder stage4 = dag.addOperator("Stage4", EventExploder.class);
    EventExploder stage5 = dag.addOperator("Stage5", EventExploder.class);
    
    MultiStageCounter counter = dag.addOperator("MultiCounter", MultiStageCounter.class);
    ConsoleOutputOperator notifier = dag.addOperator("Notifier", ConsoleOutputOperator.class);
    
    dag.addStream("stream12", stage1.output, stage2.input);
    dag.addStream("stream23", stage2.output, stage3.input);
    dag.addStream("stream34", stage3.output, stage4.input);
    dag.addStream("stream45", stage4.output, stage5.input);
    //dag.addStream("stream34", stage3.output, processor.input);
    
    dag.addStream("count1", stage1.count, counter.stage1);
    dag.addStream("count2", stage2.count, counter.stage2);
    dag.addStream("count3", stage3.count, counter.stage3);
    dag.addStream("count4", stage4.count, counter.stage4);
    dag.addStream("processed", stage5.processed, counter.processed);
    
    dag.addStream("notification", counter.complete, notifier.input);
  }
}
