package com.kong;

import org.testng.annotations.Test;

public class DemoAppTest {

    @Test (enabled=false)
    public void canRunApplicationForProducer() throws Exception {
        Main.main(new String[] {"producer"});
    }


    @Test(enabled=false)
    public void canRunApplicationForConsumer() throws Exception {
        Main.main(new String[] {"consumer"});
    }
}
