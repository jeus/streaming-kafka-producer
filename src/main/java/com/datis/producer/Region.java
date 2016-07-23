/*
*This class for send every Second a Word to specified topic
*i will with this want test windowing. 
*/
package com.datis.producer;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 *
 * @author jeus
 */
public class Region {
    
    String[] world =  "USA,Afghanistan,Albania,Algeria".split(",");
public void Region()
{
    
}
    



    private String getWord()
    {
        Random random = new Random();
        return world[random.nextInt(world.length)];
    }
    
}
