package sgbd.util;

import java.util.Random;

public class Faker {

    private static final String primaryName[] = {"Luiz","Henrique","Sabrina","James","Mary","Robert","Patricia","John","Jennifer","Michael","Linda","William","Elizabeth","David","Barbara","Richard","Susan","Joseph","Jessica","Thomas","Sarah","Charles","Karen","Christopher","Nancy","Daniel","Lisa","Matthew","Betty","Anthony","Margaret","Mark","Sandra","Donald","Ashley","Steven","Kimberly","Paul","Emily","Andrew","Donna","Joshua","Michelle","Kenneth","Dorothy","Kevin","Carol","Brian","Amanda","George","Melissa","Edward","Deborah","Ronald","Stephanie","Timothy","Rebecca","Jason","Sharon","Jeffrey","Laura","Ryan","Cynthia","Jacob","Kathleen","Gary","Amy","Nicholas","Shirley","Eric","Angela","Jonathan","Helen","Stephen","Anna","Larry","Brenda","Justin","Pamela","Scott","Nicole","Brandon","Emma","Benjamin","Samantha","Samuel","Katherine","Gregory","Christine","Frank","Debra","Alexander","Rachel","Raymond","Catherine","Patrick","Carolyn","Jack","Janet","Dennis","Ruth","Jerry","Maria","Tyler","Heather","Aaron","Diane","Jose","Virginia","Adam","Julie","Henry","Joyce","Nathan","Victoria","Douglas","Olivia","Zachary","Kelly","Peter","Christina","Kyle","Lauren","Walter","Joan","Ethan","Evelyn","Jeremy","Judith","Harold","Megan","Keith","Cheryl","Christian","Andrea","Roger","Hannah","Noah","Martha","Gerald","Jacqueline","Carl","Frances","Terry","Gloria","Sean","Ann","Austin","Teresa","Arthur","Kathryn","Lawrence","Sara","Jesse","Janice","Dylan","Jean","Bryan","Alice","Joe","Madison","Jordan","Doris","Billy","Abigail","Bruce","Julia","Albert","Judy","Willie","Grace","Gabriel","Denise","Logan","Amber","Alan","Marilyn","Juan","Beverly","Wayne","Danielle","Roy","Theresa","Ralph","Sophia","Randy","Marie","Eugene","Diana","Vincent","Brittany","Russell","Natalie","Elijah","Isabella","Louis","Charlotte","Bobby","Rose","Philip","Alexis","Johnny","Kayla"};

    private static Random random = new Random();


    public static void replaceRandom(Random r){
        random = r;
    }

    public static String firstName(){
        int names = random.nextInt(3)+1;
        String name = "";
        for(int x=0;x<names;x++){
            name+=" "+primaryName[random.nextInt(primaryName.length)];
        }
        return name.trim();
    }

    public static int integer(int min, int max){
        return random.nextInt(max-min)+min;
    }

    public static float floatPoint(int min, int max){
        float value = random.nextFloat();
        return value*max+(max-min);
    }

}
