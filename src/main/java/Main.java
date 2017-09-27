
import utils.IPinYouParser;

import java.io.File;

import java.io.FileNotFoundException;
import java.util.Scanner;

public class Main {
    public static void main(String[] args) throws FileNotFoundException {
        String path ="C:\\Users\\Yura_Vusach\\Desktop\\leaderboard.test.data.20130613_15.txt";
        Scanner input = new Scanner (new File(path));
        //input.useDelimiter("\t");
        IPinYouParser parser;
        while (input.hasNext()) {
            parser = new IPinYouParser(input.nextLine());
            if (parser.isValid()) {
                int res =parser.getCityID();
                if(res<=395){
                    System.out.println(res);
                    System.out.println("price:"+parser.getBiddingPrice());
                }
                else {
                    System.out.println("fuck "+res);
                    break;
                }
            }
        }
    }
}
