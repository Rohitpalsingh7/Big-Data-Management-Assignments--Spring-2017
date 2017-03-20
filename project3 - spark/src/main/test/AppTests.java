/**
 * Created by yousef fadila on 19/03/2017.
 */
import static org.junit.Assert.*;

import com.ds503.project3.App;
import org.junit.Before;
import org.junit.Test;

public class AppTests {

    @Test
    public void testGetNeighbours()
    {
        System.out.println("Neighbours of 1: " + App.getNeighbours(1));
        System.out.println("Neighbours of 2: " +App.getNeighbours(2));
        System.out.println("Neighbours of 1000: " +App.getNeighbours(1000));
        System.out.println("Neighbours of 1001: " +App.getNeighbours(1001));
        System.out.println("Neighbours of 1002: " +App.getNeighbours(1002));
        System.out.println("Neighbours of 1500: " +App.getNeighbours(1500));
        System.out.println("Neighbours of 250000: " +App.getNeighbours(250000));
    }

}
