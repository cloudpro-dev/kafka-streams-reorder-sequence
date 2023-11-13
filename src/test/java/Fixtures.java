import org.example.avro.ElectronicOrder;

import java.text.ParseException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class Fixtures {

    private static long ts(final String timeString)  {
        return Instant.parse(timeString).toEpochMilli();
    }

    // first set of user records all arrive within grace period
    private final static ElectronicOrder orderOne = ElectronicOrder.newBuilder().setElectronicId("HDTV-3333").setOrderId("1").setUserId("10261998").setSequenceNumber(1).setTime(ts("2021-11-04T01:01:00Z")).setPrice(5.0).build();
    private final static ElectronicOrder orderTwo = ElectronicOrder.newBuilder().setElectronicId("HDTV-3333").setOrderId("2").setUserId("10261998").setSequenceNumber(2).setTime(ts("2021-11-04T01:02:00Z")).setPrice(10.0).build();
    private final static ElectronicOrder orderThree = ElectronicOrder.newBuilder().setElectronicId("HDTV-3333").setOrderId("3").setUserId("10261998").setSequenceNumber(3).setTime(ts("2021-11-04T01:03:00Z")).setPrice(15.0).build();
    private final static ElectronicOrder orderFour = ElectronicOrder.newBuilder().setElectronicId("HDTV-3333").setOrderId("4").setUserId("10261998").setSequenceNumber(4).setTime(ts("2021-11-04T01:04:00Z")).setPrice(20.0).build();

    // second set of user records all arrive within grace period
    private final static ElectronicOrder orderFive = ElectronicOrder.newBuilder().setElectronicId("HDTV-3333").setOrderId("5").setUserId("13245635").setSequenceNumber(1).setTime(ts("2021-11-04T02:01:00Z")).setPrice(25.0).build();
    private final static ElectronicOrder orderSix = ElectronicOrder.newBuilder().setElectronicId("HDTV-3333").setOrderId("6").setUserId("13245635").setSequenceNumber(2).setTime(ts("2021-11-04T02:02:00Z")).setPrice(30.0).build();
    private final static ElectronicOrder orderSeven = ElectronicOrder.newBuilder().setElectronicId("HDTV-3333").setOrderId("7").setUserId("13245635").setSequenceNumber(3).setTime(ts("2021-11-04T02:03:00Z")).setPrice(35.0).build();
    private final static ElectronicOrder orderEight = ElectronicOrder.newBuilder().setElectronicId("HDTV-3333").setOrderId("8").setUserId("13245635").setSequenceNumber(4).setTime(ts("2021-11-04T02:04:00Z")).setPrice(40.0).build();

    public static List<ElectronicOrder> inputValues() {
        return new ArrayList<>(Arrays.asList(orderOne, orderThree, orderTwo, orderFour, orderFive, orderEight, orderSeven, orderSix));
    }

    public static List<ElectronicOrder> expectedValues() {
        return new ArrayList<>(Arrays.asList(orderOne, orderTwo, orderThree, orderFour, orderFive, orderSix, orderSeven, orderEight));
    }

}
