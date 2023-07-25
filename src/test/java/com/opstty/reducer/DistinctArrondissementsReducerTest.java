package com.opstty.reducer;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.io.IOException;
import java.util.Collections;

import static org.mockito.Mockito.verify;

@RunWith(MockitoJUnitRunner.class)
public class DistinctArrondissementsReducerTest {
    @Mock
    private Reducer.Context context;
    private DistinctArrondissementsReducer distinctArrondissementsReducer;

    @Before
    public void setup() {
        this.distinctArrondissementsReducer = new DistinctArrondissementsReducer();
    }

    @Test
    public void testReduce() throws IOException, InterruptedException {
        String key = "11";
        Iterable<NullWritable> values = Collections.singletonList(NullWritable.get());
        this.distinctArrondissementsReducer.reduce(new Text(key), values, this.context);
        verify(this.context).write(new Text(key), NullWritable.get());
    }
}

