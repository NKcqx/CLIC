package channel;

import org.javatuples.Pair;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.*;

public class ChannelTest {
    public Pair<String, String> testPair;
    public List<Pair<String, String>> testList = new ArrayList<>();
    public List<Pair<String, String>> testListByDefault = new ArrayList<>();
    public Channel channelByDefault;
    public Channel channelByPair;
    public Channel channelByList;
    public Channel channelByString;

    @Before
    public void before(){
        testPair = new Pair<>("testKey", "testValue");
        testList.add(testPair);
        testListByDefault.add(new Pair<>("result", "data"));

    }

    @Test
    public void testConstruct(){
        channelByDefault = new Channel();
        channelByPair = new Channel(testPair);
        channelByList = new Channel(testList);
        channelByString = new Channel("testKey", "testValue");
    }

    @Test
    public void testGetter(){
        testConstruct();
        assertEquals(new Pair<>("result", "data"), channelByDefault.getKeyPair());
        assertEquals(testPair, channelByPair.getKeyPair());
        assertEquals(testPair, channelByList.getKeyPair());
        assertEquals(testPair, channelByString.getKeyPair());
        assertEquals(testListByDefault, channelByDefault.getKeyPairs());
        assertEquals(testList, channelByPair.getKeyPairs());
        assertEquals(testList, channelByList.getKeyPairs());
        assertEquals(testList, channelByString.getKeyPairs());
    }
}