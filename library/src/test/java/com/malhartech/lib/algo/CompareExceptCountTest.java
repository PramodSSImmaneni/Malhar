/**
 * Copyright (c) 2012-2012 Malhar, Inc. All rights reserved.
 */
package com.malhartech.lib.algo;

import com.malhartech.engine.TestCountAndLastTupleSink;
import java.util.HashMap;
import junit.framework.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * Functional tests for {@link com.malhartech.lib.algo.CompareExceptCount} <p>
 *
 */
public class CompareExceptCountTest
{
  private static Logger log = LoggerFactory.getLogger(CompareExceptCountTest.class);

  /**
   * Test node logic emits correct results
   */
  @Test
  @SuppressWarnings("SleepWhileInLoop")
  public void testNodeProcessing() throws Exception
  {
    testNodeProcessingSchema(new CompareExceptCount<String, Integer>());
    testNodeProcessingSchema(new CompareExceptCount<String, Double>());
    testNodeProcessingSchema(new CompareExceptCount<String, Float>());
    testNodeProcessingSchema(new CompareExceptCount<String, Short>());
    testNodeProcessingSchema(new CompareExceptCount<String, Long>());
  }

  public void testNodeProcessingSchema(CompareExceptCount oper)
  {
    TestCountAndLastTupleSink countSink = new TestCountAndLastTupleSink();
    TestCountAndLastTupleSink exceptSink = new TestCountAndLastTupleSink();

    oper.count.setSink(countSink);
    oper.except.setSink(exceptSink);

    oper.setKey("a");
    oper.setValue(3.0);
    oper.setTypeEQ();

    oper.beginWindow(0);
    HashMap<String, Number> input = new HashMap<String, Number>();
    input.put("a", 2);
    input.put("b", 20);
    input.put("c", 1000);
    oper.data.process(input);
    input.clear();
    input.put("a", 3);
    oper.data.process(input);
    input.clear();
    input.put("a", 5);
    oper.data.process(input);
    oper.endWindow();

    // One for each key
    Assert.assertEquals("number emitted tuples", 1, exceptSink.count);
    Assert.assertEquals("number emitted tuples", 1, countSink.count);
    Assert.assertEquals("number emitted tuples", "2", exceptSink.tuple.toString());
    Assert.assertEquals("number emitted tuples", "1", countSink.tuple.toString());
  }
}